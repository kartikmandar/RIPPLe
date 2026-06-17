"""Lightweight mock tests for RSPTAPClient construction and helpers.

No network or real RSP token is required: authentication, service
initialization, and the service monitor are all patched out so construction is
fully offline.
"""
import contextlib
from unittest.mock import MagicMock, patch

import pytest

from ripple.data_access import rsp_tap_client as rtc
from ripple.data_access.rsp_tap_client import RSPTAPClient, create_rsp_client


@pytest.fixture
def make_client():
    """Yield a factory that builds an RSPTAPClient with all I/O patched out."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(RSPTAPClient, "_setup_authentication", return_value=None))
        stack.enter_context(patch.object(RSPTAPClient, "_initialize_services", return_value=None))
        stack.enter_context(patch.object(RSPTAPClient, "_check_and_report_service_status", return_value=None))
        stack.enter_context(patch.object(rtc, "create_service_monitor", return_value=MagicMock()))
        yield lambda: create_rsp_client(access_token="fake-token")


def test_create_rsp_client_returns_instance(make_client):
    client = make_client()
    assert isinstance(client, RSPTAPClient)
    assert client.access_token == "fake-token"


def test_test_connection_false_when_no_tap(make_client):
    client = make_client()
    client.tap_service = None  # _initialize_services was patched, so set explicitly
    assert client.test_connection() is False


def test_test_connection_true_with_results(make_client):
    client = make_client()
    fake_tap = MagicMock()
    fake_tap.search.return_value = [object()]  # non-empty result
    client.tap_service = fake_tap
    assert client.test_connection() is True


def test_missing_token_raises():
    with patch.dict("os.environ", {}, clear=False):
        import os
        os.environ.pop("RSP_ACCESS_TOKEN", None)
        with pytest.raises(ValueError):
            RSPTAPClient(access_token=None, enable_service_monitor=False)
