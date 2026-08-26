import pytest

from cusp.versions import (
    GEOSERVER_AHEAD,
    UPDATE,
    UP_TO_DATE,
    decide_sync_action,
    normalize_tag,
)


def test_normalize_tag_strips_leading_v():
    assert normalize_tag("v1.1") == "1.1"


def test_normalize_tag_accepts_a_tag_without_a_prefix():
    assert normalize_tag("1.1") == "1.1"


def test_normalize_tag_strips_surrounding_whitespace():
    assert normalize_tag("  v1.1  ") == "1.1"


def test_normalize_tag_rejects_an_unparsable_tag():
    with pytest.raises(ValueError, match="nightly-build"):
        normalize_tag("nightly-build")


def test_equal_versions_are_up_to_date():
    assert decide_sync_action("v1.1", "1.1") == UP_TO_DATE


def test_github_ahead_requires_an_update():
    assert decide_sync_action("v1.1", "1.0") == UPDATE


def test_two_part_versions_compare_numerically_not_as_strings():
    assert decide_sync_action("v1.10", "1.9") == UPDATE


def test_missing_geoserver_version_requires_a_bootstrap_update():
    assert decide_sync_action("v1.1", None) == UPDATE


def test_blank_geoserver_version_requires_a_bootstrap_update():
    assert decide_sync_action("v1.1", "   ") == UPDATE


def test_geoserver_ahead_is_reported_separately():
    assert decide_sync_action("v1.0", "1.1") == GEOSERVER_AHEAD
