"""Unit tests for the release-aware dataset-name resolver."""
import pytest
from ripple.data_access.dataset_names import (
    resolve_dataset_type, DATASET_NAMES, DEFAULT_RELEASE, LOGICAL_KEYS,
)


def test_dp1_is_default_release():
    assert DEFAULT_RELEASE == "dp1"
    assert resolve_dataset_type("calexp") == "visit_image"
    assert resolve_dataset_type("coadd") == "deep_coadd"
    assert resolve_dataset_type("object") == "object"
    assert resolve_dataset_type("src") == "source"


def test_dp02_legacy_names():
    assert resolve_dataset_type("calexp", release="dp02") == "calexp"
    assert resolve_dataset_type("coadd", release="dp02") == "deepCoadd"
    assert resolve_dataset_type("object", release="dp02") == "objectTable"
    assert resolve_dataset_type("src", release="dp02") == "src"


def test_override_takes_precedence():
    assert resolve_dataset_type("calexp", release="dp1",
                                overrides={"calexp": "my_image"}) == "my_image"
    # override only affects the named key
    assert resolve_dataset_type("coadd", release="dp1",
                                overrides={"calexp": "my_image"}) == "deep_coadd"


def test_unknown_logical_key_raises():
    with pytest.raises(KeyError):
        resolve_dataset_type("nonsense")


def test_unknown_release_raises():
    with pytest.raises(KeyError):
        resolve_dataset_type("calexp", release="dp99")


def test_logical_keys_constant_matches_tables():
    assert LOGICAL_KEYS == frozenset(DATASET_NAMES["dp1"])
    assert frozenset(DATASET_NAMES["dp02"]) == LOGICAL_KEYS
