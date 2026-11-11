import json
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from cusp.prep_for_geoserver_gpkg import (
    LAYER_NAME,
    PrepConfig,
    build_config,
    find_ogr2ogr,
    main,
    parse_args,
    prepare_geodataframe,
    verify_version_matches_filename,
)

requires_ogr2ogr = pytest.mark.skipif(
    shutil.which("ogr2ogr") is None,
    reason="ogr2ogr is required to rebuild the GeoPackage with a DATE column",
)


def test_parse_args_collects_the_required_paths_and_version():
    args = parse_args(
        [
            "--source-csv",
            "/work/cusp_v1.1.csv",
            "--release-version",
            "1.1",
            "--output-gpkg",
            "/work/cusp_observations.gpkg",
            "--sources-bib",
            "/work/cusp_sources_v1.1.bib",
        ]
    )

    assert args.source_csv == Path("/work/cusp_v1.1.csv")
    assert args.release_version == "1.1"
    assert args.output_gpkg == Path("/work/cusp_observations.gpkg")
    assert args.sources_bib == Path("/work/cusp_sources_v1.1.bib")


def test_parse_args_rejects_the_removed_release_info_option():
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--source-csv",
                "/work/cusp_v1.1.csv",
                "--release-version",
                "1.1",
                "--output-gpkg",
                "/work/cusp_observations.gpkg",
                "--sources-bib",
                "/work/cusp_sources_v1.1.bib",
                "--release-info",
                "/work/RELEASE_INFO.md",
            ]
        )


def test_build_config_defaults_the_manifest_beside_the_geopackage():
    args = parse_args(
        [
            "--source-csv",
            "/work/cusp_v1.1.csv",
            "--release-version",
            "1.1",
            "--output-gpkg",
            "/work/cusp_observations.gpkg",
            "--sources-bib",
            "/work/cusp_sources_v1.1.bib",
        ]
    )

    config = build_config(args)

    assert config.manifest_path == Path("/work/cusp_observations_gpkg_manifest.json")
    assert config.sources_bib == Path("/work/cusp_sources_v1.1.bib")


def test_version_matching_the_source_filename_is_accepted():
    verify_version_matches_filename(Path("/work/cusp_v1.1.csv"), "1.1")


def test_version_disagreeing_with_the_source_filename_is_rejected():
    with pytest.raises(ValueError, match="does not match source CSV"):
        verify_version_matches_filename(Path("/work/cusp_v1.0.csv"), "1.1")


def test_an_unrecognized_source_filename_is_rejected():
    with pytest.raises(ValueError, match="cusp_v<version>.csv"):
        verify_version_matches_filename(Path("/work/observations.csv"), "1.1")


def test_find_ogr2ogr_prefers_the_path_lookup():
    with patch(
        "cusp.prep_for_geoserver_gpkg.shutil.which",
        return_value="/usr/bin/ogr2ogr",
    ):
        assert find_ogr2ogr() == "/usr/bin/ogr2ogr"


def test_find_ogr2ogr_falls_back_to_the_interpreter_directory(tmp_path):
    fake_ogr2ogr = tmp_path / "ogr2ogr"
    fake_ogr2ogr.write_text("")

    with (
        patch("cusp.prep_for_geoserver_gpkg.shutil.which", return_value=None),
        patch(
            "cusp.prep_for_geoserver_gpkg.sys.executable",
            str(tmp_path / "python"),
        ),
    ):
        assert find_ogr2ogr() == str(fake_ogr2ogr)


def test_find_ogr2ogr_returns_none_when_absent_everywhere(tmp_path):
    with (
        patch("cusp.prep_for_geoserver_gpkg.shutil.which", return_value=None),
        patch(
            "cusp.prep_for_geoserver_gpkg.sys.executable",
            str(tmp_path / "python"),
        ),
    ):
        assert find_ogr2ogr() is None


CSV_HEADER = (
    "cusp_obs_id,source,site_id,lat,lon,date,pf_observed,"
    "thaw_depth,pf_depth,obs_limit,method,quality_flags\n"
)
CSV_ROWS = (
    "obs-1,CALM,site-a,65.1,-147.7,2020-07-01,1,55,,,gp,\n"
    "obs-2,NCSS,site-b,64.8,-147.9,2019-08-15,0,,,120,pit,\n"
)


def write_source_csv(directory: Path, version: str) -> Path:
    source_csv = directory / f"cusp_v{version}.csv"
    source_csv.write_text(CSV_HEADER + CSV_ROWS, encoding="utf-8")
    return source_csv


def write_sources_bib(directory: Path) -> Path:
    sources_bib = directory / "cusp_sources_v1.1.bib"
    sources_bib.write_text(
        """
@dataset{CALM,
 author = {Streletskiy, Dmitry A},
 year = {2025},
 title = {GTN-P CALM},
 publisher = {PANGAEA},
 doi = {10.1594/PANGAEA.972777},
}

@dataset{NCSS,
 author = {{USDA Natural Resources Conservation Service}},
 year = {2026},
 title = {NCSS Lab Data Mart},
 publisher = {USDA Natural Resources Conservation Service},
 url = {https://ncsslabdatamart.sc.egov.usda.gov/},
}
""",
        encoding="utf-8",
    )
    return sources_bib


def prep_argv(source_csv: Path, output_gpkg: Path, sources_bib: Path) -> list[str]:
    return [
        "--source-csv",
        str(source_csv),
        "--release-version",
        "1.1",
        "--output-gpkg",
        str(output_gpkg),
        "--sources-bib",
        str(sources_bib),
    ]


def build_prep_config(tmp_path: Path) -> PrepConfig:
    """Build a config for dataframe-only preparation tests."""
    return build_config(
        parse_args(
            prep_argv(
                tmp_path / "cusp_v1.1.csv",
                tmp_path / "cusp_observations.gpkg",
                tmp_path / "cusp_sources_v1.1.bib",
            )
        )
    )


def build_source_dataframe() -> pd.DataFrame:
    """Build one valid source row with optional fields intentionally absent."""
    return pd.DataFrame(
        [
            {
                "cusp_obs_id": " obs-1 ",
                "source": " CALM ",
                "lat": "90",
                "lon": "-180",
                "date": "2020-07-01",
                "pf_observed": "1",
                "thaw_depth": "55",
                "pf_depth": None,
                "obs_limit": None,
                "method": " GP ",
            }
        ]
    )


def test_prepare_geodataframe_curates_fields_without_mutating_source(
    tmp_path: Path,
) -> None:
    source = build_source_dataframe()
    original = source.copy(deep=True)

    gdf = prepare_geodataframe(
        source,
        build_prep_config(tmp_path),
        {"CALM": "CALM reference"},
    )

    assert source.equals(original)
    assert gdf.loc[0, "cusp_obs_id"] == "obs-1"
    assert gdf.loc[0, "source"] == "CALM"
    assert gdf.loc[0, "citation"] == "CALM reference"
    assert pd.isna(gdf.loc[0, "site_id"])
    assert pd.isna(gdf.loc[0, "quality_flags"])
    assert gdf.loc[0, "observation_date"].isoformat() == "2020-07-01"
    assert "obs_month" not in gdf.columns
    assert gdf.loc[0, "method"] == "gp"
    assert gdf.loc[0, "method_label"] == "Ground probe"
    assert gdf.loc[0, "pf_observed_label"] == "Permafrost observed"
    assert gdf.loc[0, "thaw_depth_cm"] == 55.0
    assert gdf.loc[0, "has_thaw_depth"]
    assert not gdf.loc[0, "has_pf_depth"]
    assert not gdf.loc[0, "has_obs_limit"]
    assert gdf.loc[0, "release_version"] == "1.1"


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("lat", "90.1", "invalid latitude"),
        ("lon", "-180.1", "invalid longitude"),
        ("date", "not-a-date", "dates could not be parsed"),
        ("pf_observed", "2", "Unexpected pf_observed values"),
        ("thaw_depth", "-1", "negative thaw_depth"),
    ],
)
def test_prepare_geodataframe_rejects_invalid_source_values(
    tmp_path: Path,
    column: str,
    value: str,
    message: str,
) -> None:
    source = build_source_dataframe()
    source.loc[0, column] = value

    with pytest.raises(ValueError, match=message):
        prepare_geodataframe(
            source,
            build_prep_config(tmp_path),
            {"CALM": "CALM reference"},
        )


@requires_ogr2ogr
def test_every_feature_carries_the_release_version(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    sources_bib = write_sources_bib(tmp_path)
    output_gpkg = tmp_path / "cusp_observations.gpkg"

    main(prep_argv(source_csv, output_gpkg, sources_bib))

    with sqlite3.connect(output_gpkg) as connection:
        versions = connection.execute(
            f"SELECT DISTINCT release_version FROM {LAYER_NAME}"
        ).fetchall()
        column_type = connection.execute(
            "SELECT type FROM pragma_table_info(?) WHERE name = 'release_version'",
            (LAYER_NAME,),
        ).fetchone()[0]

    assert versions == [("1.1",)]
    assert column_type.upper() == "TEXT"

    manifest = json.loads(
        (tmp_path / "cusp_observations_gpkg_manifest.json").read_text(encoding="utf-8")
    )
    assert "release_info" not in manifest
    assert "release_info_sha256" not in manifest


@requires_ogr2ogr
def test_every_feature_carries_the_source_citation(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    sources_bib = write_sources_bib(tmp_path)
    output_gpkg = tmp_path / "cusp_observations.gpkg"

    main(prep_argv(source_csv, output_gpkg, sources_bib))

    with sqlite3.connect(output_gpkg) as connection:
        rows = connection.execute(
            f"SELECT source, citation FROM {LAYER_NAME} ORDER BY cusp_obs_id"
        ).fetchall()
        column_type = connection.execute(
            "SELECT type FROM pragma_table_info(?) WHERE name = 'citation'",
            (LAYER_NAME,),
        ).fetchone()[0]

    assert rows == [
        (
            "CALM",
            "Streletskiy, Dmitry A (2025). GTN-P CALM. PANGAEA. "
            "https://doi.org/10.1594/PANGAEA.972777",
        ),
        (
            "NCSS",
            "USDA Natural Resources Conservation Service (2026). "
            "NCSS Lab Data Mart. USDA Natural Resources Conservation Service. "
            "https://ncsslabdatamart.sc.egov.usda.gov/",
        ),
    ]
    assert column_type.upper() == "TEXT"


@requires_ogr2ogr
def test_main_writes_the_stable_layer_name_and_passes_qa(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    sources_bib = write_sources_bib(tmp_path)
    output_gpkg = tmp_path / "cusp_observations.gpkg"

    main(prep_argv(source_csv, output_gpkg, sources_bib))

    with sqlite3.connect(output_gpkg) as connection:
        layers = connection.execute("SELECT table_name FROM gpkg_contents").fetchall()
        feature_count = connection.execute(
            f"SELECT COUNT(*) FROM {LAYER_NAME}"
        ).fetchone()[0]
        date_type = connection.execute(
            "SELECT type FROM pragma_table_info(?) WHERE name = 'observation_date'",
            (LAYER_NAME,),
        ).fetchone()[0]
        fields = {
            row[1]
            for row in connection.execute(f"PRAGMA table_info({LAYER_NAME})").fetchall()
        }

    assert layers == [("cusp_observations",)]
    assert feature_count == 2
    assert date_type.upper() == "DATE"
    assert "obs_month" not in fields


def test_main_rejects_a_version_that_disagrees_with_the_csv_name(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.0")
    sources_bib = write_sources_bib(tmp_path)

    with pytest.raises(ValueError, match="does not match source CSV"):
        main(
            prep_argv(
                source_csv,
                tmp_path / "cusp_observations.gpkg",
                sources_bib,
            )
        )


def test_main_rejects_a_source_without_a_bib_entry(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    sources_bib = tmp_path / "cusp_sources_v1.1.bib"
    sources_bib.write_text(
        """
@dataset{CALM,
 author = {Streletskiy, Dmitry A},
 year = {2025},
 title = {GTN-P CALM},
}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing BibTeX entries"):
        main(
            prep_argv(
                source_csv,
                tmp_path / "cusp_observations.gpkg",
                sources_bib,
            )
        )


def test_main_rejects_a_missing_bibliography(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")

    with pytest.raises(FileNotFoundError, match="bibliography"):
        main(
            prep_argv(
                source_csv,
                tmp_path / "cusp_observations.gpkg",
                tmp_path / "missing.bib",
            )
        )
