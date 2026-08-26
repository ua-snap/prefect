"""Compare CUSP release versions between GitHub and the published GeoServer layer.

This module is pure logic with no I/O, so the sync decision can be unit tested
without touching the network. Version strings are parsed with
``packaging.version.Version`` rather than a semver library because CUSP uses
two-part versions like ``1.1``, which strict semver parsers reject.
"""

from packaging.version import InvalidVersion, Version

# The three possible outcomes of a version comparison. The flow uses these to
# choose between exiting early, running the full update path, and refusing to
# overwrite data that is newer than the latest GitHub release.
UP_TO_DATE = "up_to_date"
UPDATE = "update"
GEOSERVER_AHEAD = "geoserver_ahead"


def normalize_tag(tag: str) -> str:
    """Strip a leading ``v`` from a release tag and confirm it parses as a version.

    GitHub release tags for CUSP look like ``v1.1``, while the
    ``release_version`` attribute stamped onto published features holds the
    bare ``1.1``. Normalizing both sides to the bare form lets them be
    compared directly.

    Args:
        tag: A release tag such as ``v1.1``, ``V1.1``, or ``1.1``. Surrounding
            whitespace is tolerated.

    Returns:
        The tag without its ``v`` prefix, e.g. ``1.1``.

    Raises:
        ValueError: If the tag is empty or does not parse as a version, with
            the offending value included in the message.
    """
    if not tag or not tag.strip():
        raise ValueError(f"Release tag is empty: {tag!r}")

    normalized = tag.strip()

    if normalized[0] in {"v", "V"}:
        normalized = normalized[1:]

    try:
        Version(normalized)
    except InvalidVersion as error:
        raise ValueError(
            f"Could not parse release tag as a version: {tag!r}"
        ) from error

    return normalized


def decide_sync_action(github_version: str, geoserver_version: str | None) -> str:
    """Decide whether the flow should stop, publish an update, or report drift.

    A missing or blank GeoServer version means the layer has never been stamped
    with a release, so it is treated as behind and triggers a bootstrap update.
    It is deliberately never an error: on the very first sync the published
    schema has no ``release_version`` attribute at all.

    Args:
        github_version: The latest GitHub release tag, with or without a
            ``v`` prefix.
        geoserver_version: The ``release_version`` value read from the
            published layer over WFS, or ``None`` when the attribute (or the
            layer's data) does not exist yet.

    Returns:
        One of the module constants:

        - ``UP_TO_DATE`` -- versions match; nothing to do.
        - ``UPDATE`` -- GitHub is ahead, or GeoServer has no version yet.
        - ``GEOSERVER_AHEAD`` -- GeoServer publishes a *newer* version than
          the latest GitHub release. The flow treats this as unexpected drift
          and refuses to overwrite it.
    """
    github = Version(normalize_tag(github_version))

    if geoserver_version is None or not str(geoserver_version).strip():
        return UPDATE

    geoserver = Version(normalize_tag(str(geoserver_version)))

    if github == geoserver:
        return UP_TO_DATE

    if github > geoserver:
        return UPDATE

    return GEOSERVER_AHEAD
