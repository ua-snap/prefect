from unittest.mock import Mock, patch

import pytest

from cusp.github_release import Release, ReleaseAsset
from cusp.sync_cusp_observations import (
    REST_BASE_URL,
    TYPE_NAME,
    WFS_BASE_URL,
    WORKSPACE,
    sync_cusp_observations_to_geoserver,
)

FLOW_KWARGS = {
    "gpkg_destination_path": "/data/cusp/cusp_observations.gpkg",
    "bib_destination_path": "/data/cusp/cusp_sources.bib",
}

SECRET_VALUES = {
    "geoserver-username": "gs-admin",
    "geoserver-password": "gs-secret",
}


def fake_secret_load(name):
    return Mock(get=Mock(return_value=SECRET_VALUES[name]))


def build_release(version="1.1"):
    def asset(name):
        return ReleaseAsset(
            name=name,
            download_url=f"https://example.invalid/{name}",
            sha256="a" * 64,
        )

    return Release(
        version=version,
        csv=asset(f"cusp_v{version}.csv"),
        bib=asset(f"cusp_sources_v{version}.bib"),
        release_info=asset("RELEASE_INFO.md"),
    )


def test_matching_versions_report_up_to_date_without_touching_geoserver():
    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            return_value="1.1",
        ),
        patch("cusp.sync_cusp_observations.create_markdown_artifact") as artifact,
        patch(
            "cusp.sync_cusp_observations.github_release.download_asset"
        ) as download_asset,
        patch("cusp.sync_cusp_observations.publish_file") as publish,
        patch("cusp.sync_cusp_observations.geoserver.reset_datastore") as reset,
        patch("cusp.sync_cusp_observations.Secret") as secret,
    ):
        result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert result == {
        "status": "up_to_date",
        "github_version": "1.1",
        "geoserver_version": "1.1",
        "layer": "cusp:cusp_observations",
    }
    download_asset.assert_not_called()
    publish.assert_not_called()
    reset.assert_not_called()
    secret.load.assert_not_called()
    artifact.assert_called_once()


def test_wfs_reads_are_anonymous():
    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            return_value="1.1",
        ) as read_version,
        patch("cusp.sync_cusp_observations.create_markdown_artifact"),
    ):
        sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert "auth" not in read_version.call_args.kwargs
    assert read_version.call_args.kwargs["wfs_base_url"] == WFS_BASE_URL
    assert read_version.call_args.kwargs["type_name"] == TYPE_NAME


def test_missing_published_version_bootstraps_a_full_update_in_order(tmp_path):
    events = []

    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            side_effect=[None, "1.1"],
        ),
        patch(
            "cusp.sync_cusp_observations.github_release.download_asset",
            side_effect=lambda asset, directory: (
                events.append(f"download:{asset.name}"),
                tmp_path / asset.name,
            )[1],
        ),
        patch(
            "cusp.sync_cusp_observations.subprocess.run",
            side_effect=lambda *_, **__: events.append("prep"),
        ),
        patch(
            "cusp.sync_cusp_observations.publish_file",
            side_effect=lambda **kwargs: events.append(
                f"publish:{kwargs['destination_path']}"
            ),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.reset_datastore",
            side_effect=lambda **_: events.append("reset"),
        ) as reset,
        patch("cusp.sync_cusp_observations.create_markdown_artifact"),
        patch("cusp.sync_cusp_observations.Secret") as secret,
    ):
        secret.load.side_effect = fake_secret_load
        result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert result["status"] == "updated"
    assert result["github_version"] == "1.1"
    assert result["geoserver_version"] is None
    assert events == [
        "download:cusp_v1.1.csv",
        "download:cusp_sources_v1.1.bib",
        "download:RELEASE_INFO.md",
        "prep",
        "publish:/data/cusp/cusp_observations.gpkg",
        "publish:/data/cusp/cusp_sources.bib",
        "reset",
    ]
    assert reset.call_args.kwargs["auth"] == ("gs-admin", "gs-secret")
    assert reset.call_args.kwargs["rest_base_url"] == REST_BASE_URL
    assert reset.call_args.kwargs["workspace"] == WORKSPACE
    assert reset.call_args.kwargs["datastore"] == "cusp_observations"


def test_the_geopackage_swap_is_backed_up_but_the_bib_is_not(tmp_path):
    publish_calls = []

    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            side_effect=["1.0", "1.1"],
        ),
        patch(
            "cusp.sync_cusp_observations.github_release.download_asset",
            side_effect=lambda asset, directory: tmp_path / asset.name,
        ),
        patch("cusp.sync_cusp_observations.subprocess.run"),
        patch(
            "cusp.sync_cusp_observations.publish_file",
            side_effect=lambda **kwargs: publish_calls.append(kwargs),
        ),
        patch("cusp.sync_cusp_observations.geoserver.reset_datastore"),
        patch("cusp.sync_cusp_observations.create_markdown_artifact"),
        patch("cusp.sync_cusp_observations.Secret") as secret,
    ):
        secret.load.side_effect = fake_secret_load
        sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    gpkg_call = publish_calls[0]
    bib_call = publish_calls[1]

    assert gpkg_call["destination_path"] == "/data/cusp/cusp_observations.gpkg"
    assert gpkg_call["backup"] is True
    assert bib_call["destination_path"] == "/data/cusp/cusp_sources.bib"
    assert "backup" not in bib_call


def test_geoserver_ahead_of_github_fails_without_publishing():
    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.0"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            return_value="1.1",
        ),
        patch("cusp.sync_cusp_observations.publish_file") as publish,
    ):
        with pytest.raises(RuntimeError, match="ahead of the latest GitHub release"):
            sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    publish.assert_not_called()


def test_a_missing_secret_block_fails_before_any_download_or_publish():
    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            return_value="1.0",
        ),
        patch(
            "cusp.sync_cusp_observations.github_release.download_asset"
        ) as download_asset,
        patch("cusp.sync_cusp_observations.publish_file") as publish,
        patch("cusp.sync_cusp_observations.Secret") as secret,
    ):
        secret.load.side_effect = ValueError(
            "Unable to find block document named geoserver-username"
        )

        with pytest.raises(ValueError, match="geoserver-username"):
            sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    download_asset.assert_not_called()
    publish.assert_not_called()


def test_verification_failure_names_the_rollback_file(tmp_path):
    with (
        patch("cusp.sync_cusp_observations.get_run_logger", return_value=Mock()),
        patch(
            "cusp.sync_cusp_observations.github_release.fetch_latest_release",
            return_value=build_release("1.1"),
        ),
        patch(
            "cusp.sync_cusp_observations.geoserver.get_published_release_version",
            side_effect=["1.0", "1.0", "1.0", "1.0"],
        ) as read_version,
        patch(
            "cusp.sync_cusp_observations.github_release.download_asset",
            side_effect=lambda asset, directory: tmp_path / asset.name,
        ),
        patch("cusp.sync_cusp_observations.subprocess.run"),
        patch("cusp.sync_cusp_observations.publish_file"),
        patch("cusp.sync_cusp_observations.geoserver.reset_datastore"),
        patch("cusp.sync_cusp_observations.time.sleep") as sleep,
        patch("cusp.sync_cusp_observations.Secret") as secret,
    ):
        secret.load.side_effect = fake_secret_load

        with pytest.raises(RuntimeError, match=r"cusp_observations\.gpkg\.bak"):
            sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert read_version.call_count == 4
    assert sleep.call_count == 2
