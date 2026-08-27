"""Discover and download CUSP release assets from GitHub.

The sync flow needs three files from each CUSP release: the observations CSV
(the preprocessing input), the sources bibliography (published beside the
GeoPackage), and the release notes (recorded for provenance). This module
resolves those assets from the GitHub releases API and downloads them with
sha256 verification, so a corrupted or partial download can never reach the
preprocessing step.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import requests

from cusp.versions import normalize_tag

GITHUB_API_URL = "https://api.github.com"
CUSP_REPO = "jonschwenk/cusp"

# Unlike the CSV and bibliography, the release notes file is not versioned in
# its filename, so it is matched by its literal name.
RELEASE_INFO_NAME = "RELEASE_INFO.md"


@dataclass(frozen=True)
class ReleaseAsset:
    """A single downloadable file attached to a GitHub release.

    Attributes:
        name: The asset filename as it appears on the release,
            e.g. ``cusp_v1.1.csv``.
        download_url: The public ``browser_download_url`` for the asset.
        sha256: The hex sha256 digest GitHub reports for the asset, without
            its ``sha256:`` prefix. Downloads are verified against this.
    """

    name: str
    download_url: str
    sha256: str


@dataclass(frozen=True)
class Release:
    """The three CUSP release assets the sync flow needs.

    Attributes:
        version: The normalized release version, e.g. ``1.1`` for tag ``v1.1``.
        csv: The observations CSV (``cusp_v{version}.csv``).
        bib: The sources bibliography (``cusp_sources_v{version}.bib``).
        release_info: The release notes (``RELEASE_INFO.md``).
    """

    version: str
    csv: ReleaseAsset
    bib: ReleaseAsset
    release_info: ReleaseAsset


GITHUB_HEADERS = {"Accept": "application/vnd.github+json"}


def select_asset(assets: list[dict], pattern: str) -> ReleaseAsset:
    """Return the one asset whose name fully matches ``pattern``.

    Exactly-one matching is deliberate: zero matches means the release layout
    changed (or the expected file was not attached), and multiple matches mean
    the pattern is ambiguous. Both should fail loudly rather than let the flow
    guess which file to publish.

    Args:
        assets: The ``assets`` list from a GitHub releases API payload.
        pattern: A regular expression matched against the full asset name.

    Returns:
        The matching asset with its download URL and sha256 digest.

    Raises:
        ValueError: If the pattern matches zero or multiple assets (the
            message lists what was available), or if the matched asset has no
            sha256 digest to verify a download against.
    """
    matches = [
        asset for asset in assets if re.fullmatch(pattern, asset.get("name", ""))
    ]

    if len(matches) != 1:
        available = ", ".join(sorted(asset.get("name", "") for asset in assets))
        raise ValueError(
            f"Expected exactly one release asset matching {pattern!r}, "
            f"found {len(matches)}. Available assets: {available or 'none'}"
        )

    asset = matches[0]
    digest = asset.get("digest") or ""

    if not digest.startswith("sha256:"):
        raise ValueError(
            f"Release asset {asset['name']!r} has no sha256 digest: {digest!r}"
        )

    return ReleaseAsset(
        name=asset["name"],
        download_url=asset["browser_download_url"],
        sha256=digest.removeprefix("sha256:"),
    )


def parse_release(payload: dict) -> Release:
    """Build a :class:`Release` from a GitHub releases API payload.

    The version parsed from the tag drives the asset patterns, so an asset
    left over from a different version (e.g. ``cusp_v1.0.csv`` on a ``v1.1``
    release) is a zero-match failure rather than a silent wrong pick.

    Raises:
        ValueError: If the tag does not parse as a version, or any of the
            three expected assets cannot be resolved unambiguously.
    """
    version = normalize_tag(payload["tag_name"])
    assets = payload.get("assets", [])
    escaped = re.escape(version)

    return Release(
        version=version,
        csv=select_asset(assets, rf"cusp_v{escaped}\.csv"),
        bib=select_asset(assets, rf"cusp_sources_v{escaped}\.bib"),
        release_info=select_asset(assets, re.escape(RELEASE_INFO_NAME)),
    )


def fetch_latest_release(
    owner_repo: str = CUSP_REPO,
    timeout: int = 30,
) -> Release:
    """Read the latest published release from the GitHub API.

    The ``releases/latest`` endpoint already excludes drafts and prereleases,
    so whatever it returns is the newest release the data provider considers
    public.

    Args:
        owner_repo: The GitHub repository in ``owner/name`` form.
        timeout: Request timeout in seconds.

    Returns:
        The parsed release with its three resolved assets.

    Raises:
        requests.HTTPError: If the API call fails.
        ValueError: If the payload cannot be parsed (see :func:`parse_release`).
    """
    response = requests.get(
        f"{GITHUB_API_URL}/repos/{owner_repo}/releases/latest",
        headers=GITHUB_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()

    return parse_release(response.json())


def download_asset(
    asset: ReleaseAsset,
    destination_dir: Path | str,
    timeout: int = 300,
    chunk_size: int = 1024 * 1024,
) -> Path:
    """Download one asset and fail unless its sha256 matches the release metadata.

    The response is streamed and hashed chunk by chunk, so the file is never
    held in memory whole and the checksum reflects exactly the bytes written
    to disk.

    Args:
        asset: The asset to download, including its expected sha256.
        destination_dir: Directory to write into; the file keeps the asset's
            own name.
        timeout: Request timeout in seconds.
        chunk_size: Streaming chunk size in bytes.

    Returns:
        The path of the verified download, ``destination_dir / asset.name``.

    Raises:
        requests.HTTPError: If the download request fails.
        RuntimeError: If the downloaded bytes do not hash to the expected
            sha256. The partial file is deleted before raising so a corrupt
            download cannot be picked up by a later step.
    """
    destination = Path(destination_dir) / asset.name
    digest = hashlib.sha256()

    with requests.get(
        asset.download_url,
        headers=GITHUB_HEADERS,
        stream=True,
        timeout=timeout,
    ) as response:
        response.raise_for_status()

        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                handle.write(chunk)
                digest.update(chunk)

    actual = digest.hexdigest()

    if actual != asset.sha256:
        destination.unlink(missing_ok=True)
        raise RuntimeError(
            f"Checksum mismatch for {asset.name}: "
            f"expected {asset.sha256}, downloaded {actual}"
        )

    return destination
