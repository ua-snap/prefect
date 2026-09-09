from unittest.mock import Mock, patch

import pytest

from cusp.geoserver import get_published_release_version, reset_datastore


def build_response(payload):
    response = Mock()
    response.json.return_value = payload
    return response


def test_returns_the_published_release_version():
    response = build_response(
        {"features": [{"properties": {"release_version": "1.1"}}]}
    )

    with patch("cusp.geoserver.requests.get", return_value=response):
        version = get_published_release_version(
            wfs_base_url="https://gs.invalid/geoserver/wfs",
            type_name="cusp:cusp_observations",
        )

    assert version == "1.1"


def test_requests_one_feature_as_geojson_without_a_property_filter():
    response = build_response(
        {"features": [{"properties": {"release_version": "1.1"}}]}
    )

    with patch("cusp.geoserver.requests.get", return_value=response) as get:
        get_published_release_version(
            wfs_base_url="https://gs.invalid/geoserver/wfs",
            type_name="cusp:cusp_observations",
        )

    params = get.call_args.kwargs["params"]

    assert params["version"] == "2.0.0"
    assert params["request"] == "GetFeature"
    assert params["typeNames"] == "cusp:cusp_observations"
    assert params["count"] == "1"
    assert params["outputFormat"] == "application/json"
    assert "propertyName" not in params


def test_returns_none_when_the_attribute_is_absent_from_the_schema():
    response = build_response({"features": [{"properties": {"source": "CALM"}}]})

    with patch("cusp.geoserver.requests.get", return_value=response):
        version = get_published_release_version(
            wfs_base_url="https://gs.invalid/geoserver/wfs",
            type_name="cusp:cusp_observations",
        )

    assert version is None


def test_returns_none_when_the_layer_has_no_features():
    response = build_response({"features": []})

    with patch("cusp.geoserver.requests.get", return_value=response):
        version = get_published_release_version(
            wfs_base_url="https://gs.invalid/geoserver/wfs",
            type_name="cusp:cusp_observations",
        )

    assert version is None


def test_returns_none_when_the_attribute_is_blank():
    response = build_response({"features": [{"properties": {"release_version": "  "}}]})

    with patch("cusp.geoserver.requests.get", return_value=response):
        version = get_published_release_version(
            wfs_base_url="https://gs.invalid/geoserver/wfs",
            type_name="cusp:cusp_observations",
        )

    assert version is None


def test_reset_posts_to_the_store_scoped_endpoint():
    response = Mock(status_code=200)

    with patch("cusp.geoserver.requests.post", return_value=response) as post:
        reset_datastore(
            rest_base_url="https://gs.invalid/geoserver/rest/",
            workspace="cusp",
            datastore="cusp_observations",
            auth=("admin", "secret"),
        )

    assert post.call_args.args[0] == (
        "https://gs.invalid/geoserver/rest"
        "/workspaces/cusp/datastores/cusp_observations/reset"
    )
    assert post.call_args.kwargs["auth"] == ("admin", "secret")
    response.raise_for_status.assert_called_once_with()


def test_reset_explains_a_404_as_a_missing_store_or_unsupported_geoserver():
    response = Mock(status_code=404)

    with patch("cusp.geoserver.requests.post", return_value=response):
        with pytest.raises(RuntimeError, match="store reset endpoint not found"):
            reset_datastore(
                rest_base_url="https://gs.invalid/geoserver/rest",
                workspace="cusp",
                datastore="wrong_name",
                auth=("admin", "secret"),
            )


def test_reset_propagates_other_http_errors():
    response = Mock(status_code=500)
    response.raise_for_status.side_effect = RuntimeError("server error")

    with patch("cusp.geoserver.requests.post", return_value=response):
        with pytest.raises(RuntimeError, match="server error"):
            reset_datastore(
                rest_base_url="https://gs.invalid/geoserver/rest",
                workspace="cusp",
                datastore="cusp_observations",
                auth=("admin", "secret"),
            )
