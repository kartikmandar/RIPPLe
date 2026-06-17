"""Mock-Butler unit tests for ButlerClient dataset-name resolution + error handling.

These tests bypass the real Butler by constructing the client with
``__new__`` and a ``MagicMock`` butler, and by isolating dataset-name behavior
from dimension validation (``_validate_dataid`` is stubbed).
"""
from unittest.mock import MagicMock
import pytest
from lsst.daf.butler import DatasetNotFoundError
from ripple.data_access.butler_client import ButlerClient
from ripple.data_access.config_examples import ButlerConfig


def _client(release="dp1", overrides=None):
    """Build a ButlerClient with a mock Butler, bypassing real init."""
    c = ButlerClient.__new__(ButlerClient)
    c.butler = MagicMock()
    c.config = ButlerConfig(repo_path="/fake/repo", collections=["c"])
    c.data_release = release
    c.dataset_types = overrides
    # isolate dataset-name behavior from registry dimension validation
    c._validate_dataid = lambda dataset_type, data_id: (True, data_id)
    return c


@pytest.mark.parametrize("release,expected", [("dp1", "visit_image"), ("dp02", "calexp")])
def test_get_calexp_uses_release_name(release, expected):
    c = _client(release)
    c.butler.get.return_value = "EXPOSURE"
    out = c.get_calexp(visit=903342, detector=10)
    assert out == "EXPOSURE"
    assert c.butler.get.call_args.args[0] == expected
    assert c.butler.get.call_args.kwargs["dataId"] == {"visit": 903342, "detector": 10}


@pytest.mark.parametrize("release,expected", [("dp1", "deep_coadd"), ("dp02", "deepCoadd")])
def test_get_deepcoadd_uses_release_name(release, expected):
    c = _client(release)
    c.butler.get.return_value = "COADD"
    out = c.get_deepCoadd(tract=9813, patch=42, band="i")
    assert out == "COADD"
    assert c.butler.get.call_args.args[0] == expected


@pytest.mark.parametrize("release,expected", [("dp1", "object"), ("dp02", "objectTable")])
def test_get_object_catalog_uses_release_name(release, expected):
    c = _client(release)
    c.butler.get.return_value = "CAT"
    out = c.get_object_catalog(tract=9813, patch=42, band="i")
    assert out == "CAT"
    assert c.butler.get.call_args.args[0] == expected


@pytest.mark.parametrize("release,expected", [("dp1", "source"), ("dp02", "src")])
def test_get_source_catalog_uses_release_name(release, expected):
    c = _client(release)
    c.butler.get.return_value = "SRC"
    out = c.get_source_catalog(visit=903342, detector=10)
    assert out == "SRC"
    assert c.butler.get.call_args.args[0] == expected


def test_override_beats_release():
    c = _client("dp1", overrides={"calexp": "custom_img"})
    c.butler.get.return_value = "X"
    c.get_calexp(visit=1, detector=2)
    assert c.butler.get.call_args.args[0] == "custom_img"


def test_get_calexp_not_found_returns_none():
    c = _client("dp1")
    c.butler.get.side_effect = DatasetNotFoundError("nope")
    assert c.get_calexp(visit=1, detector=2) is None
