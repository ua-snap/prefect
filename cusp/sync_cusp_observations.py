"""Publish a CUSP GitHub release as a reviewed Zenodo version, then sync GeoServer.

The flow is served on the GeoServer host. Zenodo is the source for every
GeoServer rebuild, including a same-version ``force_refresh``. A new version
is first validated from GitHub and staged as a draft. With ``publish=True``,
the run pauses for human review before publishing the DOI and rebuilding the
GeoServer layer from the published Zenodo archive.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Literal, TypedDict

from prefect import flow, get_run_logger, pause_flow_run
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import Secret

from cusp import geoserver, github_release, zenodo
from cusp.publish import publish_file, restore_backup
from cusp.release_bundle import (
    BundleFiles,
    extract_release_archive,
    write_release_archive,
)
from cusp.versions import GEOSERVER_AHEAD, UP_TO_DATE, decide_sync_action

TYPE_NAME = "cusp:cusp_observations"
WFS_BASE_URL = "https://gs.earthmaps.io/geoserver/wfs"
REST_BASE_URL = "https://gs.earthmaps.io/geoserver/rest"
WORKSPACE = "cusp"
DATASTORE = "cusp_observations"
GEOSERVER_DATA_DIR = "/usr/share/geoserver/data_dir/data/playground"
DEFAULT_GPKG_DESTINATION = f"{GEOSERVER_DATA_DIR}/cusp_observations.gpkg"
DEFAULT_BIB_DESTINATION = f"{GEOSERVER_DATA_DIR}/cusp_sources.bib"
PUBLISHED_GPKG_NAME = "cusp_observations.gpkg"
GEOSERVER_USERNAME_BLOCK = "geoserver-username"
GEOSERVER_PASSWORD_BLOCK = "geoserver-password"
ZENODO_TOKEN_BLOCK = "zenodo-token"


class SyncResult(TypedDict):
    """Serializable outcome of a CUSP sync run."""

    status: Literal["up_to_date", "draft_created", "zenodo_published", "updated"]
    github_version: str
    publication_date: str
    zenodo_version: str
    zenodo_record_id: int
    zenodo_doi: str
    geoserver_version: str | None
    layer: str
    force_refresh: bool
    draft_url: str | None


def build_sync_summary(result: SyncResult) -> str:
    """Describe the run and its DOI provenance in a Prefect artifact."""
    lines = [
        "# CUSP release sync",
        "",
        f"- **Status:** {result['status']}",
        f"- **GitHub release:** {result['github_version']}",
        f"- **Publication date from GitHub:** {result['publication_date']}",
        f"- **Concept DOI:** {zenodo.CONCEPT_DOI}",
        f"- **Zenodo version:** {result['zenodo_version']}",
        f"- **Published Zenodo DOI:** {result['zenodo_doi']}",
        f"- **Published Zenodo record:** https://zenodo.org/records/{result['zenodo_record_id']}",
        f"- **GeoServer layer:** {result['layer']}",
        f"- **GeoServer version before this run:** {result['geoserver_version']}",
        f"- **Force refresh:** {result['force_refresh']}",
    ]
    if result["draft_url"]:
        lines.extend([f"- **Draft for review:** {result['draft_url']}"])
    return "\n".join(lines) + "\n"


def record_sync_summary(result: SyncResult) -> None:
    """Put the current outcome on the Prefect flow run."""
    create_markdown_artifact(
        key="cusp-release-sync", markdown=build_sync_summary(result)
    )


def load_geoserver_admin_auth() -> tuple[str, str]:
    """Read credentials required to reset the GeoServer data store."""
    return (
        Secret.load(GEOSERVER_USERNAME_BLOCK).get(),
        Secret.load(GEOSERVER_PASSWORD_BLOCK).get(),
    )


def load_zenodo_client() -> zenodo.ZenodoClient:
    """Read the automation token from the Prefect Secret block."""
    return zenodo.ZenodoClient(Secret.load(ZENODO_TOKEN_BLOCK).get())


def run_prep(
    source_csv: Path,
    release_version: str,
    output_gpkg: Path,
    sources_bib: Path,
) -> None:
    """Validate and convert the source CSV to a GeoPackage in a subprocess."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "cusp.prep_for_geoserver_gpkg",
            "--source-csv",
            str(source_csv),
            "--release-version",
            release_version,
            "--output-gpkg",
            str(output_gpkg),
            "--sources-bib",
            str(sources_bib),
        ],
        check=True,
    )


def read_prepared_fields(output_gpkg: Path) -> set[str]:
    """Read the preprocessor's inspected output fields from its QA manifest."""
    manifest = output_gpkg.with_name(f"{output_gpkg.stem}_gpkg_manifest.json")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    return set(payload["output_qa"]["fields"])


def verify_published_layer(
    expected_version: str,
    expected_fields: set[str],
    *,
    attempts: int = 3,
    delay_seconds: int = 5,
) -> None:
    """Check WFS version and schema after a GeoServer store reset."""
    last_version = None
    last_fields = None
    for attempt in range(attempts):
        last_version = geoserver.get_published_release_version(
            wfs_base_url=WFS_BASE_URL, type_name=TYPE_NAME
        )
        if last_version == expected_version:
            last_fields = geoserver.get_published_property_names(
                wfs_base_url=WFS_BASE_URL, type_name=TYPE_NAME
            )
            if last_fields == expected_fields:
                return
        if attempt < attempts - 1:
            time.sleep(delay_seconds)
    raise RuntimeError(
        f"GeoServer verification failed: version {last_version!r}, "
        f"fields {last_fields!r}; expected {expected_version!r} and "
        f"{expected_fields!r}"
    )


def download_github_bundle(
    release: github_release.Release, destination_dir: Path
) -> BundleFiles:
    """Download and checksum-check exactly the three approved release assets."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    return BundleFiles(
        release_info=github_release.download_asset(
            release.release_info, destination_dir
        ),
        bibliography=github_release.download_asset(release.bib, destination_dir),
        observations=github_release.download_asset(release.csv, destination_dir),
    )


def prepare_geopackage(files: BundleFiles, version: str, destination: Path) -> Path:
    """Validate and convert files downloaded from a published Zenodo record."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    run_prep(files.observations, version, destination, files.bibliography)
    read_prepared_fields(destination)
    return destination


def create_reviewed_draft(
    client: zenodo.ZenodoClient,
    latest_record_id: int,
    release: github_release.Release,
    archive_path: Path,
) -> zenodo.Draft:
    """Create a new Zenodo draft containing only the approved archive."""
    draft = client.create_new_draft(latest_record_id)
    client.delete_draft_files(draft.id)
    client.update_draft_metadata(
        draft.id, version=release.version, publication_date=release.publication_date
    )
    client.upload_draft_archive(draft.id, archive_path)
    client.verify_draft(
        draft.id,
        version=release.version,
        publication_date=release.publication_date,
        archive_path=archive_path,
    )
    return draft


def download_and_prepare_zenodo(
    client: zenodo.ZenodoClient,
    record_id: int,
    version: str,
    work_path: Path,
    *,
    expected_archive_sha256: str | None = None,
) -> tuple[Path, Path]:
    """Prepare GeoServer inputs solely from the published Zenodo archive."""
    archive_dir = work_path / "zenodo-download"
    archive_dir.mkdir(parents=True)
    archive = client.download_published_archive(
        record_id, version=version, destination_dir=archive_dir
    )
    if (
        expected_archive_sha256
        and zenodo.hash_file(archive, algorithm="sha256") != expected_archive_sha256
    ):
        raise RuntimeError("Published Zenodo archive differs from the reviewed draft")
    files = extract_release_archive(
        archive_path=archive,
        version=version,
        destination_dir=work_path / "zenodo-files",
    )
    gpkg = prepare_geopackage(files, version, work_path / PUBLISHED_GPKG_NAME)
    return gpkg, files.bibliography


def reset_geoserver(auth: tuple[str, str]) -> None:
    """Refresh the CUSP data store after a local file swap."""
    geoserver.reset_datastore(
        rest_base_url=REST_BASE_URL,
        workspace=WORKSPACE,
        datastore=DATASTORE,
        auth=auth,
    )


def restore_published_files(
    swapped_paths: list[Path], existing_paths: set[Path], auth: tuple[str, str]
) -> None:
    """Put both live files back after a failed swap or WFS verification."""
    for destination in reversed(swapped_paths):
        if destination in existing_paths:
            restore_backup(destination)
        else:
            destination.unlink(missing_ok=True)
    reset_geoserver(auth)


def update_geoserver(
    gpkg: Path,
    bibliography: Path,
    *,
    version: str,
    gpkg_destination: Path,
    bib_destination: Path,
    auth: tuple[str, str],
) -> None:
    """Swap both files, reset the store, verify WFS, and roll back on failure."""
    expected_fields = read_prepared_fields(gpkg)
    existing = {path for path in (gpkg_destination, bib_destination) if path.exists()}
    swapped: list[Path] = []
    try:
        publish_file(gpkg, gpkg_destination, backup=True)
        swapped.append(gpkg_destination)
        publish_file(bibliography, bib_destination, backup=True)
        swapped.append(bib_destination)
        reset_geoserver(auth)
        verify_published_layer(version, expected_fields)
    except Exception as error:
        if swapped:
            try:
                restore_published_files(swapped, existing, auth)
            except Exception as rollback_error:
                raise RuntimeError(
                    f"GeoServer update failed and rollback failed: {rollback_error}"
                ) from error
        raise


@flow(name="sync-cusp-observations-to-geoserver", log_prints=True)
def sync_cusp_observations_to_geoserver(
    gpkg_destination_path: str = DEFAULT_GPKG_DESTINATION,
    bib_destination_path: str = DEFAULT_BIB_DESTINATION,
    force_refresh: bool = False,
    publish: bool = False,
) -> SyncResult:
    """Stage a Zenodo version, optionally publish it, then sync GeoServer.

    ``publish=False`` leaves any new Zenodo version as a draft and emits its
    review URL. ``publish=True`` pauses up to 20 minutes for a human to inspect
    the draft and resume the flow in Prefect. Resuming authorizes DOI
    publication. Matching GitHub and Zenodo versions skip draft creation;
    GeoServer can still be rebuilt from Zenodo with ``force_refresh=True``.
    """
    logger = get_run_logger()
    release = github_release.fetch_latest_release()
    published_version = geoserver.get_published_release_version(
        wfs_base_url=WFS_BASE_URL, type_name=TYPE_NAME
    )
    gs_action = decide_sync_action(release.version, published_version)
    if gs_action == GEOSERVER_AHEAD:
        raise RuntimeError(
            f"GeoServer publishes {published_version}, ahead of the latest GitHub "
            f"release {release.version}. Refusing to overwrite newer data."
        )

    client = load_zenodo_client()
    latest = client.fetch_latest_record()
    zenodo_action = decide_sync_action(release.version, latest.version)
    if zenodo_action == GEOSERVER_AHEAD:
        raise RuntimeError(
            f"Zenodo publishes {latest.version}, ahead of the latest GitHub "
            f"release {release.version}. Refusing to create an older version."
        )
    result: SyncResult = {
        "status": "up_to_date",
        "github_version": release.version,
        "publication_date": release.publication_date,
        "zenodo_version": latest.version,
        "zenodo_record_id": latest.id,
        "zenodo_doi": latest.doi,
        "geoserver_version": published_version,
        "layer": TYPE_NAME,
        "force_refresh": force_refresh,
        "draft_url": None,
    }
    logger.info(
        "GitHub %s; Zenodo %s (%s); GeoServer %s",
        release.version,
        latest.version,
        latest.id,
        published_version,
    )

    if zenodo_action == UP_TO_DATE and gs_action == UP_TO_DATE and not force_refresh:
        record_sync_summary(result)
        return result

    # A published DOI must have a serving path. Catch missing GeoServer
    # credentials before making any Zenodo mutation when publishing is enabled.
    reset_auth = (
        load_geoserver_admin_auth() if zenodo_action == UP_TO_DATE or publish else None
    )

    with tempfile.TemporaryDirectory(prefix="cusp-sync-") as work_dir:
        work_path = Path(work_dir)
        archive_sha256 = None
        if zenodo_action != UP_TO_DATE:
            github_files = download_github_bundle(release, work_path / "github-files")
            archive = write_release_archive(
                version=release.version,
                files=github_files,
                destination=work_path / f"v{release.version}.zip",
            )
            archive_sha256 = zenodo.hash_file(archive, algorithm="sha256")
            draft = create_reviewed_draft(client, latest.id, release, archive)
            result["status"] = "draft_created"
            result["draft_url"] = draft.review_url
            logger.info("Review the Zenodo draft at %s", draft.review_url)
            record_sync_summary(result)
            if not publish:
                return result

            reviewed_metadata = dict(client.fetch_deposition(draft.id)["metadata"])
            logger.info(
                "Paused for Zenodo draft review. Resume this run in Prefect to publish."
            )
            pause_flow_run(timeout=1200)
            client.verify_draft(
                draft.id,
                version=release.version,
                publication_date=release.publication_date,
                archive_path=archive,
                expected_metadata=reviewed_metadata,
            )
            client.publish_draft(draft.id)
            latest = client.wait_for_published_record(
                draft.id,
                version=release.version,
                publication_date=release.publication_date,
            )
            result["zenodo_version"] = latest.version
            result["zenodo_record_id"] = latest.id
            result["zenodo_doi"] = latest.doi
            result["status"] = "zenodo_published"
            logger.info("Published Zenodo DOI %s", latest.doi)
            try:
                record_sync_summary(result)
            except Exception:
                logger.exception(
                    "Could not update the Prefect DOI artifact; continuing to GeoServer"
                )

        assert reset_auth is not None
        gpkg, bibliography = download_and_prepare_zenodo(
            client,
            latest.id,
            release.version,
            work_path,
            expected_archive_sha256=archive_sha256,
        )
        update_geoserver(
            gpkg,
            bibliography,
            version=release.version,
            gpkg_destination=Path(gpkg_destination_path),
            bib_destination=Path(bib_destination_path),
            auth=reset_auth,
        )

    result["status"] = "updated"
    record_sync_summary(result)
    return result


if __name__ == "__main__":
    sync_cusp_observations_to_geoserver.serve(name="cusp-geoserver-sync")
