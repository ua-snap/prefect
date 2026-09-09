"""Publish prepared CUSP files into the GeoServer data directory.

The flow is served on the GeoServer host itself (kept alive by PM2), so
publishing is a local filesystem operation: no SSH is involved. The published
path is stable -- the data store keeps pointing at the same filename release
after release, and the version lives inside the data as the
``release_version`` attribute.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def publish_file(
    source_path: Path | str,
    destination_path: Path | str,
    file_mode: str = "664",
    backup: bool = False,
) -> None:
    """Swap ``source_path`` into ``destination_path`` atomically.

    The file is staged as ``<destination>.tmp`` in the destination's own
    directory, so the closing ``os.replace`` stays on one filesystem (a rename
    across filesystems is not atomic) and the live path is never missing or
    half-written, even if GeoServer reads it mid-publish.

    When ``backup`` is set, the current file is first copied -- not moved --
    to ``<destination>.bak``, so a failed run can be reverted by moving the
    backup into place while the live path keeps serving in the meantime.

    Args:
        source_path: The finished local file to publish. Publishing anything
            that has not fully passed preprocessing QA is the caller's bug;
            this function only guarantees the swap itself is safe.
        destination_path: The stable target path in the GeoServer data
            directory.
        file_mode: Octal permission string applied to the published file. The
            default ``664`` keeps it readable (and writable) by the GeoServer
            service account's group.
        backup: Copy the existing destination to ``<destination>.bak`` before
            replacing it. Used for the GeoPackage; unnecessary for the
            bibliography.

    Raises:
        FileNotFoundError: If ``source_path`` does not exist.
        OSError: If any filesystem step fails. Because the swap is staged, a
            failure before the final rename leaves the live destination
            untouched.
    """
    source = Path(source_path)
    destination = Path(destination_path)

    if not source.is_file():
        raise FileNotFoundError(f"Local file to publish does not exist: {source}")

    temporary = destination.parent / f"{destination.name}.tmp"
    backup_destination = destination.parent / f"{destination.name}.bak"

    # On a bootstrap run there is nothing to back up yet.
    if backup and destination.exists():
        shutil.copy2(destination, backup_destination)

    # Set the mode on the staged file so the destination is never observable
    # with wrong permissions, then rename into place atomically.
    shutil.copyfile(source, temporary)
    os.chmod(temporary, int(file_mode, 8))
    os.replace(temporary, destination)
