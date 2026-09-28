import hashlib
from pathlib import Path
from unittest.mock import Mock

import pytest

from cusp.zenodo import CONCEPT_DOI, ZenodoClient, validate_zenodo_url


def response(payload, *, status_code=200):
    result = Mock(status_code=status_code)
    result.json.return_value = payload
    return result


def build_client():
    session = Mock()
    session.headers = {}
    return ZenodoClient("token", session=session), session


def test_latest_record_resolves_the_concept_doi():
    client, session = build_client()
    session.request.return_value = response(
        {
            "id": 22802827,
            "conceptdoi": CONCEPT_DOI,
            "status": "published",
            "metadata": {"version": "1.1"},
            "doi": "10.5281/zenodo.22802827",
        }
    )

    latest = client.fetch_latest_record()

    assert latest.id == 22802827
    assert latest.version == "1.1"
    assert session.request.call_args.args == (
        "GET",
        "https://zenodo.org/api/records/22802355",
    )
    assert session.headers["Authorization"] == "Bearer token"


def test_existing_new_version_draft_is_not_overwritten():
    client, session = build_client()
    session.request.return_value = response(
        {"links": {"latest_draft": "https://zenodo.org/api/deposit/depositions/99"}}
    )

    with pytest.raises(RuntimeError, match="https://zenodo.org/deposit/99"):
        client.create_new_draft(22802827)

    assert session.request.call_count == 1


def test_new_version_uses_latest_published_id_and_old_response_link():
    client, session = build_client()
    session.request.side_effect = [
        response(
            {
                "links": {
                    "latest_draft": "https://zenodo.org/api/deposit/depositions/22802827"
                }
            }
        ),
        response(
            {"links": {"latest_draft": "https://zenodo.org/api/deposit/depositions/99"}}
        ),
    ]

    draft = client.create_new_draft(22802827)

    assert draft.id == 99
    assert draft.review_url == "https://zenodo.org/deposit/99"
    assert session.request.call_args.args == (
        "POST",
        "https://zenodo.org/api/deposit/depositions/22802827/actions/newversion",
    )


def test_metadata_update_changes_only_version_and_publication_date():
    client, session = build_client()
    session.request.side_effect = [
        response(
            {
                "metadata": {
                    "title": "CUSP",
                    "version": "1.0",
                    "publication_date": "2026-01-01",
                }
            }
        ),
        response({}),
    ]

    client.update_draft_metadata(99, version="1.1", publication_date="2026-08-07")

    assert session.request.call_args.kwargs["json"] == {
        "metadata": {
            "title": "CUSP",
            "version": "1.1",
            "publication_date": "2026-08-07",
        }
    }


def test_verify_draft_detects_changed_archive(tmp_path):
    archive = tmp_path / "v1.1.zip"
    archive.write_bytes(b"reviewed archive")
    client, session = build_client()
    session.request.side_effect = [
        response({"metadata": {"version": "1.1", "publication_date": "2026-08-07"}}),
        response([{"filename": "v1.1.zip", "checksum": "md5:changed"}]),
    ]

    with pytest.raises(RuntimeError, match="changed during review"):
        client.verify_draft(
            99, version="1.1", publication_date="2026-08-07", archive_path=archive
        )


def test_verify_draft_detects_other_metadata_changes(tmp_path):
    archive = tmp_path / "v1.1.zip"
    archive.write_bytes(b"reviewed archive")
    client, session = build_client()
    session.request.return_value = response(
        {
            "metadata": {
                "version": "1.1",
                "publication_date": "2026-08-07",
                "title": "changed",
            }
        }
    )

    with pytest.raises(RuntimeError, match="metadata changed during review"):
        client.verify_draft(
            99,
            version="1.1",
            publication_date="2026-08-07",
            archive_path=archive,
            expected_metadata={
                "version": "1.1",
                "publication_date": "2026-08-07",
                "title": "CUSP",
            },
        )


def test_download_published_archive_checks_its_md5(tmp_path):
    body = b"zenodo archive bytes"
    client, session = build_client()
    session.request.return_value = response(
        [{"filename": "v1.1.zip", "checksum": f"md5:{hashlib.md5(body).hexdigest()}"}]
    )
    streamed = Mock()
    streamed.__enter__ = Mock(return_value=streamed)
    streamed.__exit__ = Mock(return_value=False)
    streamed.iter_content.return_value = [body]
    session.get.return_value = streamed

    path = client.download_published_archive(
        99, version="1.1", destination_dir=tmp_path
    )

    assert path == Path(tmp_path) / "v1.1.zip"
    assert path.read_bytes() == body
    assert session.get.call_args.args[0] == (
        "https://zenodo.org/api/records/99/files/v1.1.zip/content"
    )


def test_download_prefers_deposition_file_link_for_restricted_record(tmp_path):
    body = b"restricted archive"
    client, session = build_client()
    download_link = "https://zenodo.org/api/files/bucket/v1.1.zip"
    session.request.return_value = response(
        [
            {
                "filename": "v1.1.zip",
                "checksum": hashlib.md5(body).hexdigest(),
                "links": {"download": download_link},
            }
        ]
    )
    streamed = Mock()
    streamed.__enter__ = Mock(return_value=streamed)
    streamed.__exit__ = Mock(return_value=False)
    streamed.iter_content.return_value = [body]
    session.get.return_value = streamed

    client.download_published_archive(99, version="1.1", destination_dir=tmp_path)

    assert session.get.call_args.args == (download_link,)


def test_token_cannot_be_sent_to_a_foreign_host():
    with pytest.raises(ValueError, match="Unexpected Zenodo API link"):
        validate_zenodo_url("https://example.com/steal")


def test_published_record_must_retain_the_github_release_date():
    client, session = build_client()
    session.get.return_value = response(
        {
            "status": "published",
            "conceptdoi": CONCEPT_DOI,
            "metadata": {"version": "1.1", "publication_date": "2026-08-08"},
            "doi": "10.5281/zenodo.99",
        }
    )

    with pytest.raises(ValueError, match="date does not match GitHub"):
        client.wait_for_published_record(
            99, version="1.1", publication_date="2026-08-07", attempts=1
        )
