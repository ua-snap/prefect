"""Manage CUSP versions and download their archives through the Zenodo API."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

ZENODO_BASE_URL = "https://zenodo.org"
CONCEPT_DOI = "10.5281/zenodo.22802355"
CONCEPT_RECORD_ID = 22802355


@dataclass(frozen=True)
class PublishedRecord:
    """A published version within the configured CUSP concept record."""

    id: int
    version: str
    doi: str


@dataclass(frozen=True)
class Draft:
    """A new, unpublished Zenodo version awaiting human review."""

    id: int
    review_url: str


def hash_file(path: Path, *, algorithm: str) -> str:
    """Hash a file without holding the entire archive in memory."""
    digest = hashlib.new(algorithm)
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_zenodo_url(url: str) -> str:
    """Accept only HTTPS links on Zenodo before sending an access token."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "zenodo.org":
        raise ValueError(f"Unexpected Zenodo API link: {url}")
    return url


class ZenodoClient:
    """Authenticated API operations for the one CUSP concept record."""

    def __init__(self, token: str, *, session: requests.Session | None = None) -> None:
        if not token:
            raise ValueError("Zenodo access token is empty")
        self.session = session or requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def request_json(self, method: str, url: str, **kwargs: object) -> dict | list:
        """Send one authenticated API request and decode its JSON response."""
        response = self.session.request(
            method, validate_zenodo_url(url), timeout=60, **kwargs
        )
        response.raise_for_status()
        return response.json()

    def fetch_latest_record(self) -> PublishedRecord:
        """Resolve the concept record to its latest published version."""
        payload = self.request_json(
            "GET", f"{ZENODO_BASE_URL}/api/records/{CONCEPT_RECORD_ID}"
        )
        if not isinstance(payload, dict):
            raise ValueError("Zenodo latest-record response was not an object")
        if payload.get("conceptdoi") != CONCEPT_DOI:
            raise ValueError("Zenodo concept DOI does not match the CUSP dataset")
        if payload.get("status") != "published":
            raise ValueError("Zenodo latest version is not published")
        version = payload.get("metadata", {}).get("version")
        if not isinstance(version, str) or not version.strip():
            raise ValueError("Zenodo latest version has no metadata.version")
        return PublishedRecord(
            id=int(payload["id"]), version=version.strip(), doi=str(payload["doi"])
        )

    def fetch_deposition(self, record_id: int) -> dict:
        """Read an owned deposition, including draft metadata and action links."""
        payload = self.request_json(
            "GET", f"{ZENODO_BASE_URL}/api/deposit/depositions/{record_id}"
        )
        if not isinstance(payload, dict):
            raise ValueError("Zenodo deposition response was not an object")
        return payload

    def ensure_no_open_draft(self, latest_record_id: int) -> None:
        """Refuse to overwrite an existing new-version draft."""
        latest = self.fetch_deposition(latest_record_id)
        draft_link = latest.get("links", {}).get("latest_draft")
        if draft_link:
            draft_id = int(
                validate_zenodo_url(draft_link).rstrip("/").rsplit("/", 1)[1]
            )
            if draft_id != latest_record_id:
                raise RuntimeError(
                    f"Zenodo already has an unpublished draft: "
                    f"{ZENODO_BASE_URL}/deposit/{draft_id}"
                )

    def create_new_draft(self, latest_record_id: int) -> Draft:
        """Create a new version from the latest published deposition."""
        self.ensure_no_open_draft(latest_record_id)
        payload = self.request_json(
            "POST",
            f"{ZENODO_BASE_URL}/api/deposit/depositions/"
            f"{latest_record_id}/actions/newversion",
        )
        if not isinstance(payload, dict):
            raise ValueError("Zenodo new-version response was not an object")
        draft_link = payload.get("links", {}).get("latest_draft")
        if not draft_link:
            raise ValueError("Zenodo new-version response has no latest_draft link")
        draft_id = int(validate_zenodo_url(draft_link).rstrip("/").rsplit("/", 1)[1])
        if draft_id == latest_record_id:
            raise ValueError("Zenodo did not create a distinct new-version draft")
        return Draft(id=draft_id, review_url=f"{ZENODO_BASE_URL}/deposit/{draft_id}")

    def update_draft_metadata(
        self, draft_id: int, *, version: str, publication_date: str
    ) -> None:
        """Change only the inherited version and publication date fields."""
        draft = self.fetch_deposition(draft_id)
        metadata = dict(draft["metadata"])
        metadata["version"] = version
        metadata["publication_date"] = publication_date
        self.request_json(
            "PUT",
            f"{ZENODO_BASE_URL}/api/deposit/depositions/{draft_id}",
            json={"metadata": metadata},
        )

    def list_deposition_files(self, record_id: int) -> list[dict]:
        """List all files on a draft or owned published deposition."""
        payload = self.request_json(
            "GET", f"{ZENODO_BASE_URL}/api/deposit/depositions/{record_id}/files"
        )
        if not isinstance(payload, list):
            raise ValueError("Zenodo deposition file list was not an array")
        return payload

    def delete_draft_files(self, draft_id: int) -> None:
        """Remove inherited files so the draft can contain only the new ZIP."""
        for file in self.list_deposition_files(draft_id):
            file_id = file["id"]
            response = self.session.delete(
                f"{ZENODO_BASE_URL}/api/deposit/depositions/{draft_id}/files/{file_id}",
                timeout=60,
            )
            response.raise_for_status()

    def upload_draft_archive(self, draft_id: int, archive_path: Path) -> None:
        """Stream the ZIP to the draft bucket and check its returned MD5."""
        draft = self.fetch_deposition(draft_id)
        bucket = validate_zenodo_url(draft["links"]["bucket"])
        url = f"{bucket.rstrip('/')}/{quote(archive_path.name)}"
        with archive_path.open("rb") as source:
            response = self.session.put(url, data=source, timeout=600)
        response.raise_for_status()
        checksum = response.json().get("checksum", "")
        if checksum.removeprefix("md5:") != hash_file(archive_path, algorithm="md5"):
            raise RuntimeError("Zenodo draft upload checksum does not match the ZIP")

    def verify_draft(
        self,
        draft_id: int,
        *,
        version: str,
        publication_date: str,
        archive_path: Path,
        expected_metadata: dict | None = None,
    ) -> None:
        """Confirm the reviewed draft still has the intended metadata and ZIP."""
        draft = self.fetch_deposition(draft_id)
        metadata = draft["metadata"]
        if expected_metadata is not None and metadata != expected_metadata:
            raise RuntimeError("Zenodo draft metadata changed during review")
        if metadata.get("version") != version:
            raise RuntimeError("Zenodo draft version changed during review")
        if metadata.get("publication_date") != publication_date:
            raise RuntimeError("Zenodo draft publication date changed during review")
        files = self.list_deposition_files(draft_id)
        if len(files) != 1 or files[0].get("filename") != archive_path.name:
            raise RuntimeError("Zenodo draft must contain only the expected ZIP")
        checksum = str(files[0].get("checksum", "")).removeprefix("md5:")
        if checksum != hash_file(archive_path, algorithm="md5"):
            raise RuntimeError("Zenodo draft ZIP changed during review")

    def publish_draft(self, draft_id: int) -> None:
        """Submit a reviewed draft for publication."""
        self.request_json(
            "POST",
            f"{ZENODO_BASE_URL}/api/deposit/depositions/{draft_id}/actions/publish",
        )

    def wait_for_published_record(
        self,
        record_id: int,
        *,
        version: str,
        publication_date: str,
        attempts: int = 12,
        delay: int = 5,
    ) -> PublishedRecord:
        """Wait for Zenodo to expose the new version after its 202 response."""
        for attempt in range(attempts):
            response = self.session.get(
                f"{ZENODO_BASE_URL}/api/records/{record_id}", timeout=30
            )
            if response.status_code == 200:
                payload = response.json()
                if payload.get("status") == "published":
                    if payload.get("conceptdoi") != CONCEPT_DOI:
                        raise ValueError("Published version has the wrong concept DOI")
                    if payload.get("metadata", {}).get("version") != version:
                        raise ValueError(
                            "Published Zenodo version does not match GitHub"
                        )
                    if (
                        payload.get("metadata", {}).get("publication_date")
                        != publication_date
                    ):
                        raise ValueError("Published Zenodo date does not match GitHub")
                    return PublishedRecord(record_id, version, payload["doi"])
            elif response.status_code not in {404, 409}:
                response.raise_for_status()
            if attempt < attempts - 1:
                time.sleep(delay)
        raise TimeoutError(f"Zenodo record {record_id} did not become published")

    def download_published_archive(
        self, record_id: int, *, version: str, destination_dir: Path
    ) -> Path:
        """Download the one version-specific ZIP with authenticated access."""
        files = self.list_deposition_files(record_id)
        filename = f"v{version}.zip"
        if len(files) != 1 or files[0].get("filename") != filename:
            raise RuntimeError(
                f"Zenodo record {record_id} must contain only {filename}; "
                f"found {[file.get('filename') for file in files]}"
            )
        # Legacy deposition responses may supply their own authenticated
        # download link. Use it when present; the record-files endpoint is
        # the fallback for responses without a file link.
        download_link = (files[0].get("links") or {}).get("download")
        url = (
            validate_zenodo_url(download_link)
            if download_link
            else (
                f"{ZENODO_BASE_URL}/api/records/{record_id}/files/{quote(filename)}/content"
            )
        )
        destination = destination_dir / filename
        with self.session.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with destination.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    target.write(chunk)
        checksum = str(files[0].get("checksum", "")).removeprefix("md5:")
        if checksum != hash_file(destination, algorithm="md5"):
            destination.unlink(missing_ok=True)
            raise RuntimeError("Downloaded Zenodo ZIP failed its checksum")
        return destination
