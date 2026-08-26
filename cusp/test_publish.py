import stat
from unittest.mock import patch

import pytest

from cusp.publish import publish_file


def test_publish_replaces_the_destination_and_backs_up_the_previous_file(tmp_path):
    source = tmp_path / "staging" / "cusp_observations.gpkg"
    source.parent.mkdir()
    source.write_bytes(b"new gpkg bytes")

    destination = tmp_path / "live" / "cusp_observations.gpkg"
    destination.parent.mkdir()
    destination.write_bytes(b"old gpkg bytes")

    publish_file(source, destination, backup=True)

    assert destination.read_bytes() == b"new gpkg bytes"
    assert (tmp_path / "live" / "cusp_observations.gpkg.bak").read_bytes() == (
        b"old gpkg bytes"
    )
    assert not (tmp_path / "live" / "cusp_observations.gpkg.tmp").exists()


def test_publish_stages_the_temporary_file_beside_the_destination(tmp_path):
    source = tmp_path / "cusp_observations.gpkg"
    source.write_bytes(b"gpkg bytes")

    destination = tmp_path / "live" / "cusp_observations.gpkg"
    destination.parent.mkdir()

    observed = {}
    real_replace = publish_file.__globals__["os"].replace

    def spying_replace(src, dst):
        observed["src"] = str(src)
        observed["dst"] = str(dst)
        return real_replace(src, dst)

    with patch("cusp.publish.os.replace", side_effect=spying_replace):
        publish_file(source, destination)

    assert observed["src"] == str(destination) + ".tmp"
    assert observed["dst"] == str(destination)


def test_publish_without_backup_does_not_create_a_backup_file(tmp_path):
    source = tmp_path / "cusp_sources_v1.1.bib"
    source.write_text("@article{}", encoding="utf-8")

    destination = tmp_path / "cusp_sources.bib"
    destination.write_text("old bib", encoding="utf-8")

    publish_file(source, destination)

    assert destination.read_text(encoding="utf-8") == "@article{}"
    assert not (tmp_path / "cusp_sources.bib.bak").exists()


def test_publish_bootstrap_with_no_existing_destination(tmp_path):
    source = tmp_path / "cusp_observations.gpkg"
    source.write_bytes(b"gpkg bytes")

    destination = tmp_path / "live" / "cusp_observations.gpkg"
    destination.parent.mkdir()

    publish_file(source, destination, backup=True)

    assert destination.read_bytes() == b"gpkg bytes"
    assert not (tmp_path / "live" / "cusp_observations.gpkg.bak").exists()


def test_publish_applies_the_requested_file_mode(tmp_path):
    source = tmp_path / "cusp_observations.gpkg"
    source.write_bytes(b"gpkg bytes")

    destination = tmp_path / "live.gpkg"

    publish_file(source, destination, file_mode="664")

    assert stat.S_IMODE(destination.stat().st_mode) == 0o664


def test_publish_refuses_a_missing_source_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        publish_file(
            tmp_path / "absent.gpkg",
            tmp_path / "cusp_observations.gpkg",
        )
