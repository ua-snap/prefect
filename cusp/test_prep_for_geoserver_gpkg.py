import shutil
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from cusp.prep_for_geoserver_gpkg import (
    LAYER_NAME,
    build_config,
    find_ogr2ogr,
    main,
    parse_args,
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
        ]
    )

    assert args.source_csv == Path("/work/cusp_v1.1.csv")
    assert args.release_version == "1.1"
    assert args.output_gpkg == Path("/work/cusp_observations.gpkg")


def test_build_config_defaults_the_manifest_beside_the_geopackage():
    args = parse_args(
        [
            "--source-csv",
            "/work/cusp_v1.1.csv",
            "--release-version",
            "1.1",
            "--output-gpkg",
            "/work/cusp_observations.gpkg",
        ]
    )

    config = build_config(args)

    assert config.manifest_path == Path("/work/cusp_observations_gpkg_manifest.json")
    assert config.release_info is None


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


@requires_ogr2ogr
def test_every_feature_carries_the_release_version(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    output_gpkg = tmp_path / "cusp_observations.gpkg"

    main(
        [
            "--source-csv",
            str(source_csv),
            "--release-version",
            "1.1",
            "--output-gpkg",
            str(output_gpkg),
        ]
    )

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

    manifest_path = tmp_path / "cusp_observations_gpkg_manifest.json"
    assert manifest_path.exists()


@requires_ogr2ogr
def test_main_writes_the_stable_layer_name_and_passes_qa(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.1")
    output_gpkg = tmp_path / "cusp_observations.gpkg"

    main(
        [
            "--source-csv",
            str(source_csv),
            "--release-version",
            "1.1",
            "--output-gpkg",
            str(output_gpkg),
        ]
    )

    with sqlite3.connect(output_gpkg) as connection:
        layers = connection.execute("SELECT table_name FROM gpkg_contents").fetchall()
        feature_count = connection.execute(
            f"SELECT COUNT(*) FROM {LAYER_NAME}"
        ).fetchone()[0]
        date_type = connection.execute(
            "SELECT type FROM pragma_table_info(?) WHERE name = 'observation_date'",
            (LAYER_NAME,),
        ).fetchone()[0]

    assert layers == [("cusp_observations",)]
    assert feature_count == 2
    assert date_type.upper() == "DATE"


def test_main_rejects_a_version_that_disagrees_with_the_csv_name(tmp_path):
    source_csv = write_source_csv(tmp_path, "1.0")

    with pytest.raises(ValueError, match="does not match source CSV"):
        main(
            [
                "--source-csv",
                str(source_csv),
                "--release-version",
                "1.1",
                "--output-gpkg",
                str(tmp_path / "cusp_observations.gpkg"),
            ]
        )
