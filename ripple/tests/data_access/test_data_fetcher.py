"""Mock-based unit tests for LsstDataFetcher (multi-backend init + delegation)."""
from unittest.mock import MagicMock
import pytest
from ripple.data_access.data_fetcher import LsstDataFetcher
from ripple.data_access.exceptions import DataAccessError


def test_data_folder_source_initializes_no_clients():
    f = LsstDataFetcher({"type": "data_folder"})
    assert f.butler_client is None and f.rsp_tap_client is None


def test_data_release_defaults_to_dp1():
    f = LsstDataFetcher({"type": "data_folder"})
    assert f.data_release == "dp1"


def test_data_release_read_from_full_config():
    f = LsstDataFetcher({"data_source": {"type": "data_folder", "data_release": "dp02"}})
    assert f.data_release == "dp02"


def test_object_catalog_default_table_is_release_aware():
    f = LsstDataFetcher({"type": "data_folder", "data_release": "dp1"})
    f.rsp_tap_client = MagicMock()
    f.rsp_tap_client.tap_service = object()
    f.rsp_tap_client.get_object_catalog.return_value = [{"id": 1}]
    f.get_object_catalog(ra=10.0, dec=-30.0)
    assert f.rsp_tap_client.get_object_catalog.call_args.kwargs["table"] == "dp1.Object"


def test_object_catalog_dp02_table():
    f = LsstDataFetcher({"type": "data_folder", "data_release": "dp02"})
    f.rsp_tap_client = MagicMock()
    f.rsp_tap_client.tap_service = object()
    f.rsp_tap_client.get_object_catalog.return_value = []
    f.get_object_catalog(ra=10.0, dec=-30.0)
    assert f.rsp_tap_client.get_object_catalog.call_args.kwargs["table"] == "dp02_dc2_catalogs.Object"


def test_object_catalog_explicit_table_overrides_default():
    f = LsstDataFetcher({"type": "data_folder", "data_release": "dp1"})
    f.rsp_tap_client = MagicMock()
    f.rsp_tap_client.tap_service = object()
    f.rsp_tap_client.get_object_catalog.return_value = []
    f.get_object_catalog(ra=10.0, dec=-30.0, table="custom.Table")
    assert f.rsp_tap_client.get_object_catalog.call_args.kwargs["table"] == "custom.Table"


def test_get_object_catalog_without_tap_raises():
    f = LsstDataFetcher({"type": "data_folder"})
    f.rsp_tap_client = None
    with pytest.raises(DataAccessError):
        f.get_object_catalog(ra=1.0, dec=2.0)
