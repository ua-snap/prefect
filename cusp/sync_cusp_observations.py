"""Sync the published CUSP observations layer with the latest GitHub release.

The flow is a single linear sequence:

1. Read the latest release tag from GitHub.
2. Read the currently published ``release_version`` over WFS.
3. Compare. When they match, record an artifact and stop, unless
   ``force_refresh`` is set -- then the update path still runs so a
   preprocessing change can be republished onto the same release. When
   GeoServer is ahead of GitHub, fail rather than overwrite newer data.
4. Otherwise: download the release assets (checksum-verified), preprocess the
   CSV into a GeoPackage in a temporary work directory, atomically swap the
   GeoPackage and bibliography into the GeoServer data directory, reset that
   one data store, and re-verify the published version over WFS.

This flow is served on the GeoServer host itself (kept alive by PM2), so the
GeoPackage swap is a local filesystem operation and no SSH is involved. It is
on-demand only: runs are triggered from the Prefect UI or CLI, and there is no
schedule.

Credentials: WFS reads are anonymous and the file swap relies on the serving
user's filesystem permissions. The only authenticated call is the store reset
against the REST admin API, whose credentials are read from the Prefect Secret
blocks named by ``GEOSERVER_USERNAME_BLOCK`` and ``GEOSERVER_PASSWORD_BLOCK``.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Literal, TypedDict

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import Secret

from cusp import geoserver, github_release
from cusp.publish import publish_file
from cusp.versions import GEOSERVER_AHEAD, UP_TO_DATE, UPDATE, decide_sync_action

CUSP_REPO = "jonschwenk/cusp"
TYPE_NAME = "cusp:cusp_observations"
WFS_BASE_URL = "https://gs.earthmaps.io/geoserver/wfs"
REST_BASE_URL = "https://gs.earthmaps.io/geoserver/rest"
WORKSPACE = "cusp"
DATASTORE = "cusp_observations"

GEOSERVER_DATA_DIR = "/usr/share/geoserver/data_dir/data/playground"
DEFAULT_GPKG_DESTINATION = f"{GEOSERVER_DATA_DIR}/cusp_observations.gpkg"
DEFAULT_BIB_DESTINATION = f"{GEOSERVER_DATA_DIR}/cusp_sources.bib"

# The stable filename inside the work directory. The published path on
# GeoServer never changes; only the file's contents (and its embedded
# release_version attribute) do.
PUBLISHED_GPKG_NAME = "cusp_observations.gpkg"

# Prefect Secret blocks holding the GeoServer admin credentials used for the
# store reset. These live encrypted on the Prefect server, matching how the
# wildfire flows handle their credentials.
GEOSERVER_USERNAME_BLOCK = "geoserver-username"
GEOSERVER_PASSWORD_BLOCK = "geoserver-password"


class SyncResult(TypedDict):
    """The stable, serialized result returned by a CUSP sync run."""

    status: Literal["up_to_date", "updated"]
    github_version: str
    geoserver_version: str | None
    layer: str
    force_refresh: bool


def build_sync_summary(result: SyncResult) -> str:
    """Render the flow result as the body of the run's markdown artifact.

    The artifact shows up on the flow run page in the Prefect UI, so someone
    checking a past run can see at a glance whether it published anything and
    which versions were involved, without reading the logs.
    """
    headline = (
        "No action needed. GeoServer already publishes the latest release."
        if result["status"] == UP_TO_DATE
        else (
            "Refreshed the published layer in place."
            if result.get("force_refresh")
            and result["github_version"] == result["geoserver_version"]
            else "Published a new release to GeoServer."
        )
    )

    return (
        "# CUSP GeoServer sync\n\n"
        f"{headline}\n\n"
        f"- **Status:** {result['status']}\n"
        f"- **Layer:** {result['layer']}\n"
        f"- **Latest GitHub release:** {result['github_version']}\n"
        f"- **GeoServer version before this run:** {result['geoserver_version']}\n"
        f"- **Force refresh:** {result['force_refresh']}\n"
    )


def load_geoserver_admin_auth() -> tuple[str, str]:
    """Read the GeoServer admin credentials from their Prefect Secret blocks.

    Called at the start of the update path, before any file is downloaded or
    swapped, so a missing or misnamed block fails the run while the live
    layer is still untouched and still being served.
    """
    return (
        Secret.load(GEOSERVER_USERNAME_BLOCK).get(),
        Secret.load(GEOSERVER_PASSWORD_BLOCK).get(),
    )


def run_prep(
    source_csv: Path,
    release_version: str,
    output_gpkg: Path,
    sources_bib: Path,
) -> None:
    """Convert the release CSV into the GeoPackage GeoServer will publish.

    The prep script is run as a subprocess rather than imported so its heavy
    dependencies (geopandas, pyogrio) load in their own interpreter, and so a
    hard crash in native code cannot take the serve process down with it.
    ``check=True`` turns any prep failure -- validation, QA, or crash -- into
    an exception here, which stops the flow before anything is published.

    Raises:
        subprocess.CalledProcessError: If preprocessing exits non-zero.
    """
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


def verify_published_version(
    expected_version: str,
    attempts: int = 3,
    delay_seconds: int = 5,
) -> str | None:
    """Re-read the published version, giving the reset a moment to take effect.

    The store reset drops GeoServer's caches lazily -- the reconnect happens
    on the next request -- so the first read after a swap can occasionally
    race it. A few short retries absorb that without masking a real failure.

    Args:
        expected_version: The version that should now be published.
        attempts: Total read attempts before giving up.
        delay_seconds: Pause between attempts.

    Returns:
        ``expected_version`` as soon as a read matches, otherwise whatever the
        final read returned (possibly ``None``) so the caller can report what
        GeoServer is actually serving.
    """
    published = None

    for attempt in range(attempts):
        published = geoserver.get_published_release_version(
            wfs_base_url=WFS_BASE_URL,
            type_name=TYPE_NAME,
        )

        if published == expected_version:
            return published

        if attempt < attempts - 1:
            time.sleep(delay_seconds)

    return published


@flow(name="sync-cusp-observations-to-geoserver", log_prints=True)
def sync_cusp_observations_to_geoserver(
    gpkg_destination_path: str = DEFAULT_GPKG_DESTINATION,
    bib_destination_path: str = DEFAULT_BIB_DESTINATION,
    owner_repo: str = CUSP_REPO,
    force_refresh: bool = False,
) -> SyncResult:
    """Publish the latest CUSP release to GeoServer when the layer is behind.

    GeoServer admin credentials are not flow parameters: they are read from
    the Prefect Secret blocks ``geoserver-username`` and
    ``geoserver-password``, and used only for the store reset. WFS reads are
    anonymous.

    Args:
        gpkg_destination_path: Stable path of the published GeoPackage in the
            GeoServer data directory. Never versioned; the previous file is
            kept beside it as ``<path>.bak``.
        bib_destination_path: Stable path for the sources bibliography.
        owner_repo: GitHub repository to sync from, in ``owner/name`` form.
        force_refresh: When True, run the full download / prep / publish path
            even if GitHub and GeoServer already report the same version.
            Use this after a preprocessing change (a new field, a dropped
            field) that must be burned into the currently published release.
            Does not override the GeoServer-ahead refusal.

    Returns:
        A structured result recorded on the run::

            {
                "status": "up_to_date" | "updated",
                "github_version": "1.1",
                "geoserver_version": "1.0",  # as observed BEFORE this run,
                                             # so None on a bootstrap run
                "layer": "cusp:cusp_observations",
                "force_refresh": False,
            }

    Raises:
        RuntimeError: When GeoServer publishes a version newer than the latest
            GitHub release (unexpected drift), or when the published version
            still does not match after the swap, reset, and verification
            retries. In the latter case the message names the ``.bak`` file
            to roll back to.
    """
    logger = get_run_logger()

    release = github_release.fetch_latest_release(owner_repo=owner_repo)
    published_version = geoserver.get_published_release_version(
        wfs_base_url=WFS_BASE_URL,
        type_name=TYPE_NAME,
    )

    logger.info(
        "Latest GitHub release is %s; GeoServer publishes %s",
        release.version,
        published_version,
    )

    action = decide_sync_action(release.version, published_version)

    if action == GEOSERVER_AHEAD:
        raise RuntimeError(
            f"GeoServer publishes {published_version}, which is ahead of the latest "
            f"GitHub release {release.version}. Refusing to overwrite newer data."
        )

    if action == UP_TO_DATE and force_refresh:
        logger.info(
            "Versions match (%s); force_refresh is set, so the layer will be rebuilt",
            release.version,
        )
        action = UPDATE

    # geoserver_version is deliberately the pre-run observation: on an update
    # it records what was replaced, and on a bootstrap run it is None.
    result: SyncResult = {
        "status": UP_TO_DATE if action == UP_TO_DATE else "updated",
        "github_version": release.version,
        "geoserver_version": published_version,
        "layer": TYPE_NAME,
        "force_refresh": force_refresh,
    }

    if action == UP_TO_DATE:
        create_markdown_artifact(
            key="cusp-geoserver-sync",
            markdown=build_sync_summary(result),
        )
        return result

    # The reset credentials are loaded before any file is downloaded or
    # swapped: the reset is mandatory after a swap, so failing on a missing
    # secret block now leaves the live layer untouched instead of stranding a
    # swapped file that is not yet served.
    reset_auth = load_geoserver_admin_auth()

    # Everything below runs when GeoServer is behind, or when force_refresh
    # was set on a matching-version run. Downloads and preprocessing happen
    # in an ephemeral work directory; nothing touches the GeoServer data
    # directory until prep has fully succeeded.
    with tempfile.TemporaryDirectory(prefix="cusp-sync-") as work_dir:
        work_path = Path(work_dir)

        source_csv = github_release.download_asset(release.csv, work_path)
        bibliography = github_release.download_asset(release.bib, work_path)

        output_gpkg = work_path / PUBLISHED_GPKG_NAME

        run_prep(
            source_csv=source_csv,
            release_version=release.version,
            output_gpkg=output_gpkg,
            sources_bib=bibliography,
        )

        # The GeoPackage gets a .bak backup because it is the layer's data
        # source; the bibliography is replaceable from the release itself.
        publish_file(
            source_path=output_gpkg,
            destination_path=gpkg_destination_path,
            backup=True,
        )
        publish_file(
            source_path=bibliography,
            destination_path=bib_destination_path,
        )

    geoserver.reset_datastore(
        rest_base_url=REST_BASE_URL,
        workspace=WORKSPACE,
        datastore=DATASTORE,
        auth=reset_auth,
    )

    verified = verify_published_version(
        expected_version=release.version,
    )

    if verified != release.version:
        raise RuntimeError(
            f"{TYPE_NAME} still publishes {verified} after replacing the GeoPackage "
            f"and resetting the {DATASTORE!r} store. The previous file is available "
            f"at {gpkg_destination_path}.bak for rollback."
        )
    # Record success only after WFS confirms the new release is actually live.

    create_markdown_artifact(
        key="cusp-geoserver-sync",
        markdown=build_sync_summary(result),
    )

    return result


if __name__ == "__main__":
    # No cron: this deployment is on-demand only. PM2 keeps this serve
    # process alive on the GeoServer host so it can claim triggered runs.
    sync_cusp_observations_to_geoserver.serve(
        name="cusp-geoserver-sync",
    )
