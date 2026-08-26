import hashlib
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cusp.github_release import (
    ReleaseAsset,
    download_asset,
    fetch_latest_release,
    parse_release,
    select_asset,
)


def build_asset(name, digest="sha256:" + "a" * 64):
    return {
        "name": name,
        "browser_download_url": f"https://example.invalid/{name}",
        "digest": digest,
    }


def build_payload():
    return {
        "tag_name": "v1.1",
        "assets": [
            build_asset("cusp_sources_v1.1.bib"),
            build_asset("cusp_v1.1.csv"),
            build_asset("RELEASE_INFO.md"),
        ],
    }


def test_select_asset_returns_the_single_match():
    asset = select_asset(build_payload()["assets"], r"cusp_v1\.1\.csv")

    assert asset.name == "cusp_v1.1.csv"
    assert asset.download_url == "https://example.invalid/cusp_v1.1.csv"
    assert asset.sha256 == "a" * 64


def test_select_asset_rejects_zero_matches_and_lists_what_was_available():
    with pytest.raises(ValueError, match="found 0") as exc_info:
        select_asset(build_payload()["assets"], r"cusp_v9\.9\.csv")

    assert "cusp_v1.1.csv" in str(exc_info.value)


def test_select_asset_rejects_multiple_matches():
    assets = [build_asset("cusp_v1.1.csv"), build_asset("cusp_v1.1.csv")]

    with pytest.raises(ValueError, match="found 2"):
        select_asset(assets, r"cusp_v1\.1\.csv")


def test_select_asset_requires_a_sha256_digest():
    assets = [build_asset("cusp_v1.1.csv", digest="")]

    with pytest.raises(ValueError, match="no sha256 digest"):
        select_asset(assets, r"cusp_v1\.1\.csv")


def test_parse_release_normalizes_the_tag_and_finds_all_three_assets():
    release = parse_release(build_payload())

    assert release.version == "1.1"
    assert release.csv.name == "cusp_v1.1.csv"
    assert release.bib.name == "cusp_sources_v1.1.bib"
    assert release.release_info.name == "RELEASE_INFO.md"


def test_parse_release_does_not_match_an_asset_from_a_different_version():
    payload = build_payload()
    payload["assets"] = [
        build_asset("cusp_v1.0.csv"),
        build_asset("cusp_sources_v1.1.bib"),
        build_asset("RELEASE_INFO.md"),
    ]

    with pytest.raises(ValueError, match="found 0"):
        parse_release(payload)


def test_fetch_latest_release_calls_the_releases_latest_endpoint():
    response = Mock()
    response.json.return_value = build_payload()

    with patch("cusp.github_release.requests.get", return_value=response) as get:
        release = fetch_latest_release(owner_repo="jonschwenk/cusp")

    assert release.version == "1.1"
    response.raise_for_status.assert_called_once_with()
    assert get.call_args.args[0] == (
        "https://api.github.com/repos/jonschwenk/cusp/releases/latest"
    )


def test_fetch_latest_release_sends_a_bearer_token_when_provided():
    response = Mock()
    response.json.return_value = build_payload()

    with patch("cusp.github_release.requests.get", return_value=response) as get:
        fetch_latest_release(owner_repo="jonschwenk/cusp", token="secret-token")

    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer secret-token"


class FakeStreamedResponse:
    def __init__(self, chunks):
        self._chunks = chunks
        self.raise_for_status = Mock()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def iter_content(self, chunk_size):
        yield from self._chunks


def test_download_asset_writes_the_file_and_accepts_a_matching_checksum(tmp_path):
    payload = b"observation rows"
    asset = ReleaseAsset(
        name="cusp_v1.1.csv",
        download_url="https://example.invalid/cusp_v1.1.csv",
        sha256=hashlib.sha256(payload).hexdigest(),
    )

    with patch(
        "cusp.github_release.requests.get",
        return_value=FakeStreamedResponse([payload]),
    ):
        destination = download_asset(asset, tmp_path)

    assert destination == Path(tmp_path) / "cusp_v1.1.csv"
    assert destination.read_bytes() == payload


def test_download_asset_rejects_a_checksum_mismatch_and_removes_the_file(tmp_path):
    asset = ReleaseAsset(
        name="cusp_v1.1.csv",
        download_url="https://example.invalid/cusp_v1.1.csv",
        sha256="b" * 64,
    )

    with patch(
        "cusp.github_release.requests.get",
        return_value=FakeStreamedResponse([b"observation rows"]),
    ):
        with pytest.raises(RuntimeError, match="Checksum mismatch"):
            download_asset(asset, tmp_path)

    assert not (Path(tmp_path) / "cusp_v1.1.csv").exists()
