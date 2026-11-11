#!/usr/bin/env python3
"""
Prepare the CUSP observation CSV as a GeoPackage for GeoServer.

Invoked by the sync flow as::

    python -m cusp.prep_for_geoserver_gpkg \\
        --source-csv /work/cusp_v1.1.csv \\
        --release-version 1.1 \\
        --output-gpkg /work/cusp_observations.gpkg \\
        --sources-bib /work/cusp_sources_v1.1.bib

Outputs
-------
The GeoPackage named by ``--output-gpkg`` (internal layer: cusp_observations)
and a JSON manifest beside it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

import geopandas as gpd
import pandas as pd
import pyogrio

from cusp.citations import citations_by_source, resolve_citations

# ---------------------------------------------------------------------
# Output configuration
# ---------------------------------------------------------------------

# The internal GeoPackage layer name. This must stay stable across releases:
# the GeoServer store and layer configuration point at it by name.
LAYER_NAME = "cusp_observations"
CRS = "EPSG:4326"

# CUSP release CSVs are named cusp_v<version>.csv. The declared version is
# cross-checked against the --release-version argument as a safety net.
CSV_VERSION_PATTERN = re.compile(r"^cusp_v(?P<version>.+)\.csv$")


@dataclass(frozen=True)
class PrepConfig:
    """Everything one preprocessing run needs to know."""

    source_csv: Path
    release_version: str
    output_gpkg: Path
    manifest_path: Path
    sources_bib: Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the CLI arguments the sync flow passes when invoking this module."""
    parser = argparse.ArgumentParser(
        description="Prepare the CUSP observation CSV as a GeoPackage for GeoServer."
    )
    parser.add_argument("--source-csv", required=True, type=Path)
    parser.add_argument("--release-version", required=True)
    parser.add_argument("--output-gpkg", required=True, type=Path)
    parser.add_argument("--sources-bib", required=True, type=Path)
    parser.add_argument("--manifest", type=Path, default=None)

    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> PrepConfig:
    """Turn parsed arguments into a :class:`PrepConfig`.

    Unless ``--manifest`` is given, the manifest defaults to sitting beside
    the GeoPackage as ``<stem>_gpkg_manifest.json`` so provenance stays with
    the output it describes.
    """
    manifest_path = args.manifest or args.output_gpkg.with_name(
        f"{args.output_gpkg.stem}_gpkg_manifest.json"
    )

    return PrepConfig(
        source_csv=args.source_csv,
        release_version=args.release_version,
        output_gpkg=args.output_gpkg,
        manifest_path=manifest_path,
        sources_bib=args.sources_bib,
    )


def verify_version_matches_filename(source_csv: Path, release_version: str) -> None:
    """Fail when the requested version disagrees with the source CSV filename.

    The GitHub release tag is authoritative for the version, but the CSV
    filename declares one too. A disagreement means the wrong file is about
    to be stamped with the wrong version, so it fails before anything is
    processed.
    """
    match = CSV_VERSION_PATTERN.match(source_csv.name)

    if match is None:
        raise ValueError(
            f"Source CSV is not named cusp_v<version>.csv: {source_csv.name}"
        )

    filename_version = match.group("version")

    if filename_version != release_version:
        raise ValueError(
            f"Requested release version {release_version!r} does not match source CSV "
            f"{source_csv.name!r}, which declares {filename_version!r}."
        )


# ---------------------------------------------------------------------
# Expected source fields
# ---------------------------------------------------------------------

REQUIRED_SOURCE_COLUMNS = [
    "cusp_obs_id",
    "source",
    "lat",
    "lon",
    "date",
    "pf_observed",
    "thaw_depth",
    "pf_depth",
    "obs_limit",
    "method",
]

OPTIONAL_SOURCE_COLUMNS = [
    "site_id",
    "quality_flags",
]


# ---------------------------------------------------------------------
# User-facing labels
# ---------------------------------------------------------------------

METHOD_LABELS = {
    "gp": "Ground probe",
    "tp": "Thaw probe",
    "pit": "Soil pit",
    "aug": "Auger",
    "pit_aug": "Pit and auger",
    "tp_pit": "Thaw probe and pit",
    "temp": "Temperature profile or temperature-based interpretation",
    "tt": "Thaw tube",
    "insar": "InSAR or remote-sensing-derived observation",
    "unknown": "Unknown method",
}

PF_OBSERVED_LABELS = {
    1: "Permafrost observed",
    0: "Permafrost not observed within observation context",
}


# ---------------------------------------------------------------------
# Output attribute fields
# ---------------------------------------------------------------------

# release_version is stamped onto every feature and is what the sync flow
# reads back over WFS to decide whether GeoServer is behind the latest
# GitHub release.
# Python preserves mapping insertion order, so this one definition controls
# both the published field order and the types used by the DATE-column repair.
GPKG_ATTRIBUTE_TYPES = {
    "cusp_obs_id": "TEXT",
    "source": "TEXT",
    "citation": "TEXT",
    "site_id": "TEXT",
    "observation_date": "DATE",
    "method": "TEXT",
    "method_label": "TEXT",
    "pf_observed": "INTEGER",
    "pf_observed_label": "TEXT",
    "thaw_depth_cm": "REAL",
    "pf_depth_cm": "REAL",
    "obs_limit_cm": "REAL",
    "has_thaw_depth": "BOOLEAN",
    "has_pf_depth": "BOOLEAN",
    "has_obs_limit": "BOOLEAN",
    "quality_flags": "TEXT",
    "release_version": "TEXT",
}

PROPERTY_COLUMNS = list(GPKG_ATTRIBUTE_TYPES)


# ---------------------------------------------------------------------
# File utilities
# ---------------------------------------------------------------------


def sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    """Calculate a SHA-256 checksum."""
    digest = hashlib.sha256()

    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(chunk_size), b""):
            digest.update(chunk)

    return digest.hexdigest()


# ---------------------------------------------------------------------
# Read and validate source data
# ---------------------------------------------------------------------


def read_source_csv(config: PrepConfig) -> pd.DataFrame:
    """Read the canonical CUSP observations CSV."""
    return pd.read_csv(
        config.source_csv,
        low_memory=False,
        na_values=[
            "",
            "NA",
            "NaN",
            "nan",
            "null",
            "None",
        ],
    )


def read_source_citations(config: PrepConfig) -> dict[str, str]:
    """Read the source-keyed citations needed to curate one release."""
    return citations_by_source(config.sources_bib)


def validate_source_schema(df: pd.DataFrame) -> None:
    """Confirm that required fields are present."""
    missing = [column for column in REQUIRED_SOURCE_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(f"Source CSV is missing required columns: {missing}")


def validate_source_values(df: pd.DataFrame) -> None:
    """Validate IDs, coordinates, dates, status values, and depths."""
    if df["cusp_obs_id"].isna().any():
        raise ValueError("cusp_obs_id contains missing values.")

    if df["cusp_obs_id"].duplicated().any():
        raise ValueError("cusp_obs_id contains duplicate values.")

    if df["source"].isna().any():
        raise ValueError("source contains missing values.")

    invalid_latitude = df["lat"].isna() | ~df["lat"].between(-90, 90)

    invalid_longitude = df["lon"].isna() | ~df["lon"].between(-180, 180)

    if invalid_latitude.any():
        raise ValueError(f"{invalid_latitude.sum():,} rows have invalid latitude.")

    if invalid_longitude.any():
        raise ValueError(f"{invalid_longitude.sum():,} rows have invalid longitude.")

    if df["observation_date"].isna().any():
        raise ValueError(
            f"{df['observation_date'].isna().sum():,} dates could not be parsed."
        )

    invalid_status = df["pf_observed"].notna() & ~df["pf_observed"].isin([0, 1])

    if invalid_status.any():
        values = sorted(
            df.loc[invalid_status, "pf_observed"].dropna().unique().tolist()
        )

        raise ValueError(f"Unexpected pf_observed values: {values}")

    for column in [
        "thaw_depth",
        "pf_depth",
        "obs_limit",
    ]:
        negative = df[column].notna() & (df[column] < 0)

        if negative.any():
            raise ValueError(f"{negative.sum():,} rows have negative {column}.")


# ---------------------------------------------------------------------
# Curate fields
# ---------------------------------------------------------------------


def normalize_text(series: pd.Series) -> pd.Series:
    """Strip whitespace and convert blank strings to null."""
    return series.astype("string").str.strip().replace("", pd.NA)


def _add_optional_source_columns(df: pd.DataFrame) -> None:
    """Add nullable optional source fields that are absent from a release."""
    for column in OPTIONAL_SOURCE_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA


def _normalize_source_text_columns(df: pd.DataFrame) -> None:
    """Normalize the source text fields that are published without remapping."""
    for column in ("cusp_obs_id", "source", "site_id", "quality_flags"):
        df[column] = normalize_text(df[column])


def _coerce_source_value_columns(df: pd.DataFrame) -> None:
    """Coerce raw source measurements into the dtypes used for validation."""
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["observation_date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["pf_observed"] = pd.to_numeric(df["pf_observed"], errors="coerce").astype(
        "Int64"
    )

    for column in ("thaw_depth", "pf_depth", "obs_limit"):
        df[column] = pd.to_numeric(df[column], errors="coerce")


def _add_citation_column(
    df: pd.DataFrame,
    citations: Mapping[str, str],
) -> None:
    """Add the formatted citation for each normalized source key."""
    df["citation"] = resolve_citations(df["source"], citations)


def _add_derived_observation_columns(
    df: pd.DataFrame,
    release_version: str,
) -> None:
    """Add the public labels, flags, and release metadata for each observation."""
    df["method"] = normalize_text(df["method"]).fillna("unknown").str.lower()
    df["method_label"] = df["method"].map(METHOD_LABELS).fillna(df["method"])
    df["pf_observed_label"] = (
        df["pf_observed"].map(PF_OBSERVED_LABELS).fillna("Missing or unknown status")
    )

    for source_column, output_column in (
        ("thaw_depth", "thaw_depth_cm"),
        ("pf_depth", "pf_depth_cm"),
        ("obs_limit", "obs_limit_cm"),
    ):
        df[output_column] = df[source_column]
        df[f"has_{source_column}"] = df[output_column].notna()

    df["observation_date"] = df["observation_date"].dt.date
    df["release_version"] = release_version


def _build_observation_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Build the published point layer from curated observation attributes."""
    geometry = gpd.points_from_xy(x=df["lon"], y=df["lat"], crs=CRS)

    return gpd.GeoDataFrame(
        df[PROPERTY_COLUMNS],
        geometry=geometry,
        crs=CRS,
    )


def prepare_geodataframe(
    source: pd.DataFrame,
    config: PrepConfig,
    citations: Mapping[str, str],
) -> gpd.GeoDataFrame:
    """Create the public-facing GeoDataFrame without modifying ``source``."""
    df = source.copy()

    _add_optional_source_columns(df)
    _normalize_source_text_columns(df)
    _add_citation_column(df, citations)
    _coerce_source_value_columns(df)

    validate_source_values(df)
    _add_derived_observation_columns(df, config.release_version)

    return _build_observation_geodataframe(df)


# ---------------------------------------------------------------------
# Write GeoPackage
# ---------------------------------------------------------------------


def write_geopackage(
    gdf: gpd.GeoDataFrame,
    config: PrepConfig,
) -> None:
    """Write the curated observation layer to GeoPackage."""
    if config.output_gpkg.exists():
        config.output_gpkg.unlink()

    pyogrio.write_dataframe(
        gdf,
        config.output_gpkg,
        layer=LAYER_NAME,
        driver="GPKG",
        dataset_metadata={
            "TITLE": "CUSP near-surface permafrost observations",
            "RELEASE_VERSION": config.release_version,
            "SOURCE": str(config.source_csv),
        },
        layer_metadata={
            "TITLE": "CUSP near-surface permafrost observations",
            "DESCRIPTION": (
                "Individual near-surface permafrost-related "
                "observations. This is an observation-evidence "
                "layer, not a continuous permafrost map."
            ),
            "RELEASE_VERSION": config.release_version,
        },
        layer_options={
            "FID": "fid",
            "GEOMETRY_NAME": "geom",
            "SPATIAL_INDEX": "YES",
            "IDENTIFIER": LAYER_NAME,
            "DESCRIPTION": ("CUSP near-surface permafrost observations"),
        },
    )


def find_ogr2ogr() -> str | None:
    """Locate the ogr2ogr executable, falling back to the interpreter's bin dir.

    A PM2-spawned process inherits the PM2 daemon's PATH, which does not
    include the conda environment's bin directory just because the interpreter
    lives there (no shell activation ever ran). conda installs ogr2ogr beside
    python, so when the PATH lookup misses, look next to ``sys.executable``.
    """
    found = shutil.which("ogr2ogr")

    if found is not None:
        return found

    candidate = Path(sys.executable).parent / "ogr2ogr"

    if candidate.is_file():
        return str(candidate)

    return None


def _read_layer_column_type(
    geopackage_path: Path,
    column_name: str,
) -> str:
    """Return the declared SQLite type for one published layer column."""
    with sqlite3.connect(geopackage_path) as conn:
        row = conn.execute(
            """
            SELECT type
            FROM pragma_table_info(?)
            WHERE name = ?
            """,
            (LAYER_NAME, column_name),
        ).fetchone()

    if row is None:
        raise RuntimeError(
            f"GeoPackage layer {LAYER_NAME!r} has no {column_name!r} column."
        )

    return str(row[0])


def _rebuild_observation_layer_with_declared_types(geopackage_path: Path) -> None:
    """Rebuild the observation table with the schema declared for publication."""
    temp_table = f"{LAYER_NAME}__date_fix"
    attribute_columns = ",\n                ".join(
        f'"{column}" {GPKG_ATTRIBUTE_TYPES[column]}' for column in PROPERTY_COLUMNS
    )
    quoted_columns = ", ".join(f'"{column}"' for column in PROPERTY_COLUMNS)

    with sqlite3.connect(geopackage_path) as conn:
        conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        conn.execute(f"""
            CREATE TABLE "{temp_table}" (
                "fid" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                "geom" POINT,
                {attribute_columns}
            )
            """)
        conn.execute(f"""
            INSERT INTO "{temp_table}" ("fid", "geom", {quoted_columns})
            SELECT "fid", "geom", {quoted_columns}
            FROM "{LAYER_NAME}"
            """)
        conn.execute(f'DROP TABLE "{LAYER_NAME}"')
        conn.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{LAYER_NAME}"')


def _rebuild_geopackage_metadata(
    geopackage_path: Path,
    ogr2ogr: str,
) -> None:
    """Regenerate GeoPackage metadata and its spatial index after a table rebuild."""
    temp_gpkg = geopackage_path.with_suffix(".tmp.gpkg")
    if temp_gpkg.exists():
        temp_gpkg.unlink()

    subprocess.run(
        [
            ogr2ogr,
            "-f",
            "GPKG",
            str(temp_gpkg),
            str(geopackage_path),
            LAYER_NAME,
        ],
        check=True,
    )
    temp_gpkg.replace(geopackage_path)


def finalize_observation_date_column(config: PrepConfig) -> None:
    """Re-declare observation_date as DATE and rebuild the GeoPackage.

    pyogrio can write the column as a full timestamp type, but the published
    layer should expose plain dates. The fix is done in two stages: the table
    is rebuilt in SQLite with the exact attribute types from
    GPKG_ATTRIBUTE_TYPES, then ogr2ogr rewrites the whole file so GeoPackage
    internals (gpkg_contents, spatial index, metadata) stay consistent with
    the new table definition. If the column already reads DATE, this is a
    no-op.
    """
    column_type = _read_layer_column_type(
        config.output_gpkg,
        "observation_date",
    )

    if column_type.upper() == "DATE":
        return

    _rebuild_observation_layer_with_declared_types(config.output_gpkg)

    ogr2ogr = find_ogr2ogr()
    if ogr2ogr is None:
        raise RuntimeError(
            "observation_date must be stored as DATE, but ogr2ogr was not found "
            "on PATH or beside the Python interpreter to rebuild the GeoPackage "
            "after updating the column type."
        )

    _rebuild_geopackage_metadata(config.output_gpkg, ogr2ogr)


# ---------------------------------------------------------------------
# Validate GeoPackage
# ---------------------------------------------------------------------


class GeoPackageContents(TypedDict):
    """Metadata describing the published observation table."""

    table_name: str
    data_type: str
    identifier: str
    srs_id: int
    bbox: list[float | None]


class GeoPackageInspection(TypedDict):
    """The integrity and schema checks collected from a written GeoPackage."""

    driver: str
    feature_count: int
    geometry_type: str
    crs: str
    fields: list[str]
    dtypes: list[str]
    sqlite_integrity_check: str
    spatial_index_exists: bool
    gpkg_contents: GeoPackageContents


def inspect_geopackage(config: PrepConfig) -> GeoPackageInspection:
    """Inspect layer metadata and GeoPackage integrity."""
    info = pyogrio.read_info(
        config.output_gpkg,
        layer=LAYER_NAME,
    )

    with sqlite3.connect(config.output_gpkg) as conn:
        integrity_result = conn.execute("PRAGMA integrity_check").fetchone()[0]

        spatial_index_table = f"rtree_{LAYER_NAME}_geom"

        spatial_index_exists = (
            conn.execute(
                """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table'
              AND name = ?
            """,
                (spatial_index_table,),
            ).fetchone()[0]
            == 1
        )

        gpkg_contents = conn.execute(
            """
            SELECT
                table_name,
                data_type,
                identifier,
                srs_id,
                min_x,
                min_y,
                max_x,
                max_y
            FROM gpkg_contents
            WHERE table_name = ?
            """,
            (LAYER_NAME,),
        ).fetchone()

    return {
        "driver": info["driver"],
        "feature_count": int(info["features"]),
        "geometry_type": info["geometry_type"],
        "crs": info["crs"],
        "fields": info["fields"].tolist(),
        "dtypes": [str(dtype) for dtype in info["dtypes"]],
        "sqlite_integrity_check": integrity_result,
        "spatial_index_exists": spatial_index_exists,
        "gpkg_contents": {
            "table_name": gpkg_contents[0],
            "data_type": gpkg_contents[1],
            "identifier": gpkg_contents[2],
            "srs_id": gpkg_contents[3],
            "bbox": [
                gpkg_contents[4],
                gpkg_contents[5],
                gpkg_contents[6],
                gpkg_contents[7],
            ],
        },
    }


def validate_output(
    gdf: gpd.GeoDataFrame,
    info: GeoPackageInspection,
    config: PrepConfig,
) -> None:
    """Confirm the output matches the prepared GeoDataFrame."""
    if info["feature_count"] != len(gdf):
        raise RuntimeError(
            "GeoPackage QA failed: feature count mismatch. "
            f"Expected {len(gdf):,}, found "
            f"{info['feature_count']:,}."
        )

    if info["geometry_type"] != "Point":
        raise RuntimeError("GeoPackage QA failed: geometry type is not Point.")

    if info["crs"] != CRS:
        raise RuntimeError(
            f"GeoPackage QA failed: expected {CRS}, found {info['crs']}."
        )

    if info["sqlite_integrity_check"] != "ok":
        raise RuntimeError("GeoPackage QA failed SQLite integrity check.")

    if not info["spatial_index_exists"]:
        raise RuntimeError("GeoPackage QA failed: spatial index was not created.")

    observation_date_type = _read_layer_column_type(
        config.output_gpkg,
        "observation_date",
    )

    if observation_date_type.upper() != "DATE":
        raise RuntimeError(
            "GeoPackage QA failed: observation_date must be declared as DATE, "
            f"found {observation_date_type!r}."
        )


# ---------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------


def write_manifest(
    gdf: gpd.GeoDataFrame,
    output_info: GeoPackageInspection,
    config: PrepConfig,
) -> None:
    """Write provenance and QA information beside the GeoPackage."""
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "release_version": config.release_version,
        "source_csv": str(config.source_csv),
        "source_csv_sha256": sha256_file(config.source_csv),
        "sources_bib": str(config.sources_bib),
        "sources_bib_sha256": sha256_file(config.sources_bib),
        "output_geopackage": str(config.output_gpkg),
        "output_geopackage_sha256": sha256_file(config.output_gpkg),
        "output_size_bytes": config.output_gpkg.stat().st_size,
        "format": "OGC GeoPackage",
        "layer_name": LAYER_NAME,
        "geometry_column": "geom",
        "geometry_type": "Point",
        "crs": CRS,
        "feature_count": len(gdf),
        "unique_cusp_obs_ids": int(gdf["cusp_obs_id"].nunique()),
        "source_count": int(gdf["source"].nunique()),
        "method_count": int(gdf["method"].nunique()),
        "earliest_date": (gdf["observation_date"].min().isoformat()),
        "latest_date": (gdf["observation_date"].max().isoformat()),
        "total_bounds": (gdf.total_bounds.tolist()),
        "output_qa": output_info,
    }

    config.manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
        ),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Run the full prep pipeline: read, validate, curate, write, QA, manifest.

    Any validation or QA failure raises, which makes the module exit non-zero
    when run as a subprocess -- the sync flow relies on that to stop before
    publishing anything.
    """
    config = build_config(parse_args(argv))

    if not config.source_csv.exists():
        raise FileNotFoundError(
            f"Expected source CSV was not found: {config.source_csv}"
        )

    if not config.sources_bib.exists():
        raise FileNotFoundError(
            f"Expected sources bibliography was not found: {config.sources_bib}"
        )

    verify_version_matches_filename(config.source_csv, config.release_version)

    print(f"Source CSV: {config.source_csv}")
    print(f"Sources bibliography: {config.sources_bib}")
    print(f"Output GeoPackage: {config.output_gpkg}")
    print(f"Layer name: {LAYER_NAME}")
    print(f"Release version: {config.release_version}")

    print("\n1. Reading source CSV")
    source_df = read_source_csv(config)
    print(f"   Read {len(source_df):,} observations")

    print("2. Validating source schema")
    validate_source_schema(source_df)

    print("3. Reading source citations")
    citations = read_source_citations(config)

    print("4. Curating fields and creating point geometry")
    gdf = prepare_geodataframe(source_df, config, citations)

    print("5. Writing GeoPackage")
    write_geopackage(gdf, config)
    finalize_observation_date_column(config)

    print("6. Inspecting and validating GeoPackage")
    output_info = inspect_geopackage(config)
    validate_output(gdf, output_info, config)

    print("7. Writing manifest")
    write_manifest(gdf, output_info, config)

    size_mb = config.output_gpkg.stat().st_size / 1024**2

    print("\nGeoPackage preparation complete")
    print(f"  Features:      {len(gdf):,}")
    print(f"  Sources:       {gdf['source'].nunique():,}")
    print(f"  Methods:       {gdf['method'].nunique():,}")
    print(
        "  Date range:    "
        f"{gdf['observation_date'].min()} through "
        f"{gdf['observation_date'].max()}"
    )
    print(f"  Bounding box:  {gdf.total_bounds.tolist()}")
    print(f"  Spatial index: {output_info['spatial_index_exists']}")
    print(f"  File size:     {size_mb:,.1f} MB")
    print(f"  GeoPackage:    {config.output_gpkg}")
    print(f"  Manifest:      {config.manifest_path}")


if __name__ == "__main__":
    main()
