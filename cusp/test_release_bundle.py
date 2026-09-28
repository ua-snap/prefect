from zipfile import ZipFile

import pytest

from cusp.release_bundle import (
    BundleFiles,
    extract_release_archive,
    write_release_archive,
)


def build_files(tmp_path):
    info = tmp_path / "RELEASE_INFO.md"
    bib = tmp_path / "cusp_sources_v1.1.bib"
    csv = tmp_path / "cusp_v1.1.csv"
    for path in (info, bib, csv):
        path.write_text(path.name)
    return BundleFiles(info, bib, csv)


def test_archive_matches_the_approved_layout_and_round_trips(tmp_path):
    files = build_files(tmp_path)
    archive = write_release_archive(
        version="1.1", files=files, destination=tmp_path / "v1.1.zip"
    )

    with ZipFile(archive) as zip_file:
        assert zip_file.namelist() == [
            "v1.1/RELEASE_INFO.md",
            "v1.1/cusp_sources_v1.1.bib",
            "v1.1/cusp_v1.1.csv",
        ]

    extracted = extract_release_archive(
        archive_path=archive, version="1.1", destination_dir=tmp_path / "extract"
    )
    assert extracted.release_info.read_bytes() == files.release_info.read_bytes()
    assert extracted.bibliography.read_bytes() == files.bibliography.read_bytes()
    assert extracted.observations.read_bytes() == files.observations.read_bytes()


def test_archive_rejects_extra_files(tmp_path):
    archive = tmp_path / "v1.1.zip"
    with ZipFile(archive, "w") as zip_file:
        for name in (
            "v1.1/RELEASE_INFO.md",
            "v1.1/cusp_sources_v1.1.bib",
            "v1.1/cusp_v1.1.csv",
            "v1.1/extra.txt",
        ):
            zip_file.writestr(name, name)

    with pytest.raises(ValueError, match="exactly"):
        extract_release_archive(
            archive_path=archive, version="1.1", destination_dir=tmp_path / "extract"
        )


def test_archive_rejects_wrong_asset_name(tmp_path):
    files = build_files(tmp_path)
    wrong = tmp_path / "cusp_v1.0.csv"
    wrong.write_text("wrong")

    with pytest.raises(ValueError, match="incorrectly named observations"):
        write_release_archive(
            version="1.1",
            files=BundleFiles(files.release_info, files.bibliography, wrong),
            destination=tmp_path / "v1.1.zip",
        )
