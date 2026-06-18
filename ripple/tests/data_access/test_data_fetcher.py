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


def test_get_cutout_dp1_uses_release_aware_dataset_type():
    """get_cutout must not forward the hardcoded 'deepCoadd' literal to the
    butler when data_release='dp1'; it should resolve to 'deep_coadd' via
    the butler_client's release-aware _dt('coadd') before forwarding."""
    f = LsstDataFetcher({"type": "data_folder", "data_release": "dp1"})
    mock_butler = MagicMock()
    mock_butler.test_connection.return_value = True
    mock_butler._dt.side_effect = lambda logical: "deep_coadd" if logical == "coadd" else logical
    mock_butler.get_cutout.return_value = "CUTOUT"
    f.butler_client = mock_butler

    f.get_cutout(ra=55.0, dec=-30.0, size_arcsec=60.0, band="i", backend="butler")

    # Confirm the call was made and dataset_type was 'deep_coadd', not 'deepCoadd'
    mock_butler.get_cutout.assert_called_once()
    _, call_kwargs = mock_butler.get_cutout.call_args
    forwarded_dataset_type = call_kwargs.get("dataset_type")
    assert forwarded_dataset_type != "deepCoadd", (
        f"Expected release-aware 'deep_coadd' for dp1, but got '{forwarded_dataset_type}'. "
        "The hardcoded 'deepCoadd' default is still bypassing the resolver."
    )
    assert forwarded_dataset_type == "deep_coadd", (
        f"Expected 'deep_coadd' for dp1 release, got '{forwarded_dataset_type}'."
    )
