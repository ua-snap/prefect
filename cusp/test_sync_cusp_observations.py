from unittest.mock import Mock, patch

import pytest

from cusp.github_release import Release, ReleaseAsset
from cusp.release_bundle import BundleFiles
from cusp.sync_cusp_observations import (
    REST_BASE_URL,
    WORKSPACE,
    download_and_prepare_zenodo,
    run_prep,
    sync_cusp_observations_to_geoserver,
    update_geoserver,
)
from cusp.zenodo import Draft, PublishedRecord

FLOW_KWARGS = {
    "gpkg_destination_path": "/data/cusp/cusp_observations.gpkg",
    "bib_destination_path": "/data/cusp/cusp_sources.bib",
}


def build_release(version="1.1"):
    def asset(name):
        return ReleaseAsset(name, f"https://example.invalid/{name}", "a" * 64)

    return Release(
        version=version,
        csv=asset(f"cusp_v{version}.csv"),
        bib=asset(f"cusp_sources_v{version}.bib"),
        release_info=asset("RELEASE_INFO.md"),
        publication_date="2026-08-07",
    )


def build_client(version="1.1"):
    client = Mock()
    client.fetch_latest_record.return_value = PublishedRecord(
        22802827, version, "10.5281/zenodo.22802827"
    )
    client.create_new_draft.return_value = Draft(99, "https://zenodo.org/deposit/99")
    client.wait_for_published_record.return_value = PublishedRecord(
        99, "1.1", "10.5281/zenodo.99"
    )
    client.fetch_deposition.return_value = {
        "metadata": {
            "version": "1.1",
            "publication_date": "2026-08-07",
            "title": "CUSP",
        }
    }
    return client


def mock_common(monkeypatch, version="1.1", zenodo_version="1.1", gs_version="1.1"):
    from cusp import sync_cusp_observations as module

    client = build_client(zenodo_version)
    summary = Mock()
    monkeypatch.setattr(module, "get_run_logger", lambda: Mock())
    monkeypatch.setattr(module, "record_sync_summary", summary)
    monkeypatch.setattr(module, "load_zenodo_client", lambda: client)
    monkeypatch.setattr(
        module.github_release,
        "fetch_latest_release",
        lambda **_: build_release(version),
    )
    monkeypatch.setattr(
        module.geoserver, "get_published_release_version", lambda **_: gs_version
    )
    return module, client, summary


def make_github_files(release, destination_dir):
    destination_dir.mkdir()
    paths = [
        destination_dir / "RELEASE_INFO.md",
        destination_dir / f"cusp_sources_v{release.version}.bib",
        destination_dir / f"cusp_v{release.version}.csv",
    ]
    for path in paths:
        path.write_text(path.name)
    return BundleFiles(*paths)


def test_run_prep_passes_csv_and_bib_but_no_release_info(tmp_path):
    with patch("cusp.sync_cusp_observations.subprocess.run") as run:
        run_prep(
            tmp_path / "cusp_v1.1.csv",
            "1.1",
            tmp_path / "cusp_observations.gpkg",
            tmp_path / "cusp_sources_v1.1.bib",
        )
    argv = run.call_args.args[0]
    assert "--release-info" not in argv
    assert argv[argv.index("--sources-bib") + 1].endswith("cusp_sources_v1.1.bib")


def test_matching_versions_are_noop(monkeypatch):
    module, client, summary = mock_common(monkeypatch)
    download = Mock()
    monkeypatch.setattr(module, "download_and_prepare_zenodo", download)

    result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert result["status"] == "up_to_date"
    assert result["zenodo_record_id"] == 22802827
    assert result["draft_url"] is None
    download.assert_not_called()
    client.create_new_draft.assert_not_called()
    summary.assert_called_once()


def test_new_version_creates_only_a_draft_when_publish_is_false(monkeypatch):
    module, client, summary = mock_common(
        monkeypatch, version="1.1", zenodo_version="1.0", gs_version="1.0"
    )
    monkeypatch.setattr(module, "download_github_bundle", make_github_files)
    prepare = Mock()
    monkeypatch.setattr(module, "prepare_geopackage", prepare)
    download_zenodo = Mock()
    update = Mock()
    monkeypatch.setattr(module, "download_and_prepare_zenodo", download_zenodo)
    monkeypatch.setattr(module, "update_geoserver", update)
    monkeypatch.setattr(module, "load_geoserver_admin_auth", Mock())

    result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert result["status"] == "draft_created"
    assert result["draft_url"] == "https://zenodo.org/deposit/99"
    assert result["zenodo_version"] == "1.0"
    assert client.create_new_draft.call_args.args == (22802827,)
    client.delete_draft_files.assert_called_once_with(99)
    assert client.update_draft_metadata.call_args.kwargs == {
        "version": "1.1",
        "publication_date": "2026-08-07",
    }
    assert client.upload_draft_archive.call_args.args[0] == 99
    client.publish_draft.assert_not_called()
    prepare.assert_not_called()
    download_zenodo.assert_not_called()
    update.assert_not_called()
    module.load_geoserver_admin_auth.assert_not_called()
    assert summary.call_args.args[0]["draft_url"] == result["draft_url"]


def test_github_assets_are_staged_without_preprocessing(monkeypatch):
    module, client, _ = mock_common(
        monkeypatch, version="1.1", zenodo_version="1.0", gs_version="1.0"
    )
    monkeypatch.setattr(module, "download_github_bundle", make_github_files)
    prepare = Mock(side_effect=RuntimeError("preprocessing must wait for Zenodo"))
    monkeypatch.setattr(module, "prepare_geopackage", prepare)

    result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)

    assert result["status"] == "draft_created"
    prepare.assert_not_called()
    client.upload_draft_archive.assert_called_once()
    client.publish_draft.assert_not_called()


def test_geoserver_inputs_are_extracted_from_zenodo_archive(monkeypatch, tmp_path):
    from cusp import sync_cusp_observations as module
    from cusp.release_bundle import write_release_archive

    files = make_github_files(build_release(), tmp_path / "archive-sources")
    archive = write_release_archive(
        version="1.1", files=files, destination=tmp_path / "v1.1.zip"
    )
    client = Mock()
    client.download_published_archive.return_value = archive
    prepare = Mock(side_effect=lambda files, version, destination: destination)
    monkeypatch.setattr(module, "prepare_geopackage", prepare)

    gpkg, bibliography = download_and_prepare_zenodo(
        client, 99, "1.1", tmp_path / "work"
    )

    assert gpkg.name == "cusp_observations.gpkg"
    assert bibliography.read_bytes() == files.bibliography.read_bytes()
    assert (
        prepare.call_args.args[0].observations.read_bytes()
        == files.observations.read_bytes()
    )
    client.download_published_archive.assert_called_once()


def test_publish_resumes_then_rechecks_draft_and_updates_from_zenodo(
    monkeypatch, tmp_path
):
    module, client, _ = mock_common(
        monkeypatch, version="1.1", zenodo_version="1.0", gs_version="1.0"
    )
    monkeypatch.setattr(module, "download_github_bundle", make_github_files)
    prepare = Mock()
    monkeypatch.setattr(module, "prepare_geopackage", prepare)
    monkeypatch.setattr(
        module, "load_geoserver_admin_auth", lambda: ("admin", "secret")
    )
    pause = Mock()
    monkeypatch.setattr(module, "pause_flow_run", pause)
    gpkg = tmp_path / "from-zenodo.gpkg"
    bib = tmp_path / "from-zenodo.bib"
    download = Mock(return_value=(gpkg, bib))
    update = Mock()
    monkeypatch.setattr(module, "download_and_prepare_zenodo", download)
    monkeypatch.setattr(module, "update_geoserver", update)

    result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS, publish=True)

    assert pause.call_args.kwargs == {"timeout": 1200}
    prepare.assert_not_called()
    assert client.verify_draft.call_count == 2
    assert client.verify_draft.call_args.kwargs["expected_metadata"] == {
        "version": "1.1",
        "publication_date": "2026-08-07",
        "title": "CUSP",
    }
    client.publish_draft.assert_called_once_with(99)
    assert result["status"] == "updated"
    assert result["zenodo_record_id"] == 99
    assert result["zenodo_doi"] == "10.5281/zenodo.99"
    assert download.call_args.args[1:3] == (99, "1.1")
    assert download.call_args.kwargs["expected_archive_sha256"]
    assert update.call_args.args == (gpkg, bib)
    assert update.call_args.kwargs["auth"] == ("admin", "secret")


def test_matching_zenodo_version_can_force_refresh_geoserver(monkeypatch, tmp_path):
    module, client, _ = mock_common(monkeypatch)
    monkeypatch.setattr(
        module, "load_geoserver_admin_auth", lambda: ("admin", "secret")
    )
    download = Mock(
        return_value=(tmp_path / "from-zenodo.gpkg", tmp_path / "from-zenodo.bib")
    )
    update = Mock()
    monkeypatch.setattr(module, "download_and_prepare_zenodo", download)
    monkeypatch.setattr(module, "update_geoserver", update)

    result = sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS, force_refresh=True)

    assert result["status"] == "updated"
    client.create_new_draft.assert_not_called()
    assert download.call_args.args[1:3] == (22802827, "1.1")
    update.assert_called_once()


def test_zenodo_ahead_refuses_to_create_a_draft(monkeypatch):
    _, client, _ = mock_common(
        monkeypatch, version="1.0", zenodo_version="1.1", gs_version="1.0"
    )
    with pytest.raises(RuntimeError, match="Zenodo publishes 1.1, ahead"):
        sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)
    client.create_new_draft.assert_not_called()


def test_geoserver_ahead_refuses_to_touch_zenodo(monkeypatch):
    module, _, _ = mock_common(
        monkeypatch, version="1.0", zenodo_version="1.0", gs_version="1.1"
    )
    loading = Mock()
    monkeypatch.setattr(module, "load_zenodo_client", loading)
    with pytest.raises(RuntimeError, match="GeoServer publishes 1.1, ahead"):
        sync_cusp_observations_to_geoserver.fn(**FLOW_KWARGS)
    loading.assert_not_called()


def test_geoserver_update_rolls_back_both_files_if_verification_fails(
    monkeypatch, tmp_path
):
    from cusp import sync_cusp_observations as module

    gpkg = tmp_path / "prepared.gpkg"
    bib = tmp_path / "prepared.bib"
    gpkg.write_bytes(b"new gpkg")
    bib.write_bytes(b"new bib")
    live_gpkg = tmp_path / "live.gpkg"
    live_bib = tmp_path / "live.bib"
    live_gpkg.write_bytes(b"old gpkg")
    live_bib.write_bytes(b"old bib")
    monkeypatch.setattr(module, "read_prepared_fields", lambda _: {"release_version"})
    reset = Mock()
    monkeypatch.setattr(module, "reset_geoserver", reset)
    monkeypatch.setattr(
        module, "verify_published_layer", Mock(side_effect=RuntimeError("WFS stale"))
    )

    with pytest.raises(RuntimeError, match="WFS stale"):
        update_geoserver(
            gpkg,
            bib,
            version="1.1",
            gpkg_destination=live_gpkg,
            bib_destination=live_bib,
            auth=("admin", "secret"),
        )

    assert live_gpkg.read_bytes() == b"old gpkg"
    assert live_bib.read_bytes() == b"old bib"
    assert reset.call_count == 2


def test_geoserver_reset_uses_existing_store_endpoint(monkeypatch, tmp_path):
    from cusp import sync_cusp_observations as module

    reset = Mock()
    monkeypatch.setattr(module.geoserver, "reset_datastore", reset)
    module.reset_geoserver(("admin", "secret"))
    assert reset.call_args.kwargs == {
        "rest_base_url": REST_BASE_URL,
        "workspace": WORKSPACE,
        "datastore": "cusp_observations",
        "auth": ("admin", "secret"),
    }
