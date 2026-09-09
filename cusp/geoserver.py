"""Read and refresh the published CUSP layer on GeoServer.

Two responsibilities live here:

- Reading the currently published release version over WFS. The version truth
  on GeoServer is the ``release_version`` feature attribute stamped onto every
  observation by the preprocessing step -- never the layer's catalog metadata
  (Abstract/Keywords), which does not refresh when a GeoPackage file is
  replaced in place.
- Resetting the data store's caches after the GeoPackage file has been
  swapped, so GeoServer starts serving the new data.
"""

from __future__ import annotations

import requests

# The feature attribute that carries the published release version. The
# preprocessing script stamps it onto every row, and the sync flow compares it
# against the latest GitHub release tag.
VERSION_ATTRIBUTE = "release_version"


def get_published_release_version(
    wfs_base_url: str,
    type_name: str,
    auth: tuple[str, str] | None = None,
    timeout: int = 30,
) -> str | None:
    """Return the release version published on the layer, or ``None`` if absent.

    Because every feature carries the same ``release_version`` value, reading
    a single feature is enough to know which release is live.

    No ``propertyName`` filter is sent: before the first sync the published
    schema has no ``release_version`` attribute, and requesting an unknown
    property makes GeoServer return an exception report instead of features.
    Fetching one whole feature makes the missing-attribute case resolve to
    ``None``, which is what the bootstrap path expects.

    Args:
        wfs_base_url: The WFS endpoint, e.g. ``https://host/geoserver/wfs``.
        type_name: The qualified layer name, e.g. ``cusp:cusp_observations``.
        auth: Optional ``(username, password)`` when the WFS is not public.
        timeout: Request timeout in seconds.

    Returns:
        The published version string (e.g. ``1.1``), or ``None`` when the
        layer has no features, the attribute is missing from the schema, or
        its value is blank. ``None`` is the bootstrap signal, never an error.

    Raises:
        requests.HTTPError: If the WFS request itself fails. Transport and
            auth errors are deliberately not treated as "no version" -- the
            flow must not interpret an unreachable GeoServer as "behind".
    """
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": type_name,
        "count": "1",
        "outputFormat": "application/json",
    }

    response = requests.get(wfs_base_url, params=params, auth=auth, timeout=timeout)
    response.raise_for_status()

    features = response.json().get("features") or []

    if not features:
        return None

    value = (features[0].get("properties") or {}).get(VERSION_ATTRIBUTE)

    if value is None or not str(value).strip():
        return None

    return str(value).strip()


def reset_datastore(
    rest_base_url: str,
    workspace: str,
    datastore: str,
    auth: tuple[str, str],
    timeout: int = 60,
) -> None:
    """Drop the cached connection pool and feature type structure for one store.

    GeoServer caches feature type schemas and holds a pooled SQLite connection
    to the GeoPackage, so a bare file replacement can keep serving the old
    schema and rows until the store is reset. Store scope (rather than the
    narrower ``featuretypes/{name}/reset``) is required because GeoPackage
    stores keep their own feature type cache and will not notice a changed
    table structure from a feature-type-level reset alone (GSIP 214). Store
    scope also avoids the catalog-wide blast radius of ``/rest/reset`` on a
    shared GeoServer.

    Args:
        rest_base_url: The REST endpoint, e.g. ``https://host/geoserver/rest``.
        workspace: The workspace containing the store, e.g. ``cusp``.
        datastore: The data store name. Note this is the *store* name from the
            GeoServer configuration, which is not necessarily the layer name.
        auth: ``(username, password)`` with admin rights on the store.
        timeout: Request timeout in seconds.

    Raises:
        RuntimeError: On a 404, which means either the workspace/store names
            are wrong or this GeoServer predates the per-store reset endpoint.
        requests.HTTPError: On any other HTTP failure.
    """
    url = (
        f"{rest_base_url.rstrip('/')}"
        f"/workspaces/{workspace}/datastores/{datastore}/reset"
    )

    response = requests.post(url, auth=auth, timeout=timeout)

    if response.status_code == 404:
        raise RuntimeError(
            f"GeoServer store reset endpoint not found: {url}\n"
            "Confirm the workspace and data store names, and that this GeoServer "
            "provides the per-store reset endpoint (GSIP 214)."
        )

    response.raise_for_status()
