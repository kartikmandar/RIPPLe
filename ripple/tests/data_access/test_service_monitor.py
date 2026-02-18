"""
Unit tests for Service Status Monitor

This module tests the service monitoring functionality including
status checking, caching, and error handling.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
from datetime import datetime, timedelta
import tempfile
import os

from ripple.data_access.service_monitor import ServiceStatusMonitor, create_service_monitor
from ripple.data_access.exceptions import DataAccessError


class TestServiceStatusMonitor(unittest.TestCase):
    """Test cases for ServiceStatusMonitor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.tap_url = "https://test.tap.service"
        self.sia_url = "https://test.sia.service"
        self.monitor = ServiceStatusMonitor(
            tap_url=self.tap_url,
            sia_url=self.sia_url,
            cache_duration_minutes=1  # Short cache for testing
        )

        # Create a temporary log file
        self.temp_log = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.temp_log.close()
        self.monitor.status_log_file = self.temp_log.name

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_log.name):
            os.unlink(self.temp_log.name)

    def test_initialization(self):
        """Test service monitor initialization."""
        self.assertEqual(self.monitor.tap_url, self.tap_url)
        self.assertEqual(self.monitor.sia_url, self.sia_url)
        self.assertEqual(self.monitor.cache_duration, timedelta(minutes=1))
        self.assertEqual(len(self.monitor._status_cache), 0)

    def test_cache_validation(self):
        """Test status cache validation."""
        # Test empty cache
        self.assertFalse(self.monitor._is_cache_valid('tap'))

        # Add cached entry
        self.monitor._status_cache['tap'] = (True, datetime.now(), "Test message")
        self.assertTrue(self.monitor._is_cache_valid('tap'))

        # Test expired cache
        old_time = datetime.now() - timedelta(minutes=2)
        self.monitor._status_cache['tap'] = (True, old_time, "Test message")
        self.assertFalse(self.monitor._is_cache_valid('tap'))

    def test_cache_update(self):
        """Test cache update functionality."""
        # Update cache
        self.monitor._update_cache('tap', True, "Service up")

        # Verify cache was updated
        self.assertIn('tap', self.monitor._status_cache)
        status, timestamp, message = self.monitor._status_cache['tap']
        self.assertTrue(status)
        self.assertIsInstance(timestamp, datetime)
        self.assertEqual(message, "Service up")

        # Verify log file was created
        self.assertTrue(os.path.exists(self.monitor.status_log_file))

    @patch('requests.post')
    def test_check_tap_service_success(self, mock_post):
        """Test successful TAP service check."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/xml'}
        mock_post.return_value = mock_response

        status, message = self.monitor._check_tap_service()

        self.assertTrue(status)
        self.assertEqual(message, "Service responding normally")
        mock_post.assert_called_once()

    @patch('requests.post')
    def test_check_tap_service_auth_failure(self, mock_post):
        """Test TAP service check with authentication failure."""
        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        status, message = self.monitor._check_tap_service()

        self.assertFalse(status)
        self.assertEqual(message, "Authentication failed")

    @patch('requests.post')
    def test_check_tap_service_timeout(self, mock_post):
        """Test TAP service check with timeout."""
        # Mock timeout
        mock_post.side_effect = requests.exceptions.Timeout()

        status, message = self.monitor._check_tap_service()

        self.assertFalse(status)
        self.assertEqual(message, "Request timeout")

    @patch('requests.post')
    def test_check_sia_service_success(self, mock_post):
        """Test successful SIAv2 service check."""
        # Mock successful JSON response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-type': 'application/json'}
        mock_response.text = '{"results": []}'
        mock_post.return_value = mock_response

        status, message = self.monitor._check_sia_service()

        self.assertTrue(status)
        self.assertEqual(message, "Service responding normally")
        mock_post.assert_called_once()

    @patch('requests.post')
    def test_check_sia_service_404_html(self, mock_post):
        """Test SIAv2 service check with HTML 404 response."""
        # Mock HTML 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.headers = {'content-type': 'text/html'}
        mock_response.text = '<html><body><h1>404 Not Found</h1></body></html>'
        mock_post.return_value = mock_response

        status, message = self.monitor._check_sia_service()

        self.assertFalse(status)
        self.assertEqual(message, "Endpoint not found (404)")

    @patch('requests.post')
    def test_check_sia_service_connection_error(self, mock_post):
        """Test SIAv2 service check with connection error."""
        # Mock connection error
        mock_post.side_effect = requests.exceptions.ConnectionError()

        status, message = self.monitor._check_sia_service()

        self.assertFalse(status)
        self.assertEqual(message, "Connection error")

    def test_get_service_status_tap(self):
        """Test getting TAP service status."""
        with patch.object(self.monitor, '_check_tap_service', return_value=(True, "Test OK")):
            status, message = self.monitor.get_service_status('tap')

            self.assertTrue(status)
            self.assertEqual(message, "Test OK")
            self.assertIn('tap', self.monitor._status_cache)

    def test_get_service_status_sia(self):
        """Test getting SIAv2 service status."""
        with patch.object(self.monitor, '_check_sia_service', return_value=(False, "Test Error")):
            status, message = self.monitor.get_service_status('sia')

            self.assertFalse(status)
            self.assertEqual(message, "Test Error")
            self.assertIn('sia', self.monitor._status_cache)

    def test_get_service_status_invalid_service(self):
        """Test getting status for invalid service."""
        with self.assertRaises(ValueError):
            self.monitor.get_service_status('invalid_service')

    def test_get_service_status_cached(self):
        """Test getting cached service status."""
        # Add cached entry - correct tuple structure is (status, timestamp, message)
        test_time = datetime.now()
        self.monitor._status_cache['tap'] = (True, test_time, "Cached message")

        with patch.object(self.monitor, '_check_tap_service') as mock_check:
            status, message = self.monitor.get_service_status('tap')

            # Should not call check method due to cache
            mock_check.assert_not_called()
            self.assertTrue(status)
            # The message comes from the cache
            self.assertEqual(message, "Cached message")

    def test_check_all_services(self):
        """Test checking all services."""
        with patch.object(self.monitor, 'get_service_status') as mock_get_status:
            mock_get_status.side_effect = [
                (True, "TAP OK"),
                (False, "SIA Error")
            ]

            results = self.monitor.check_all_services()

            self.assertEqual(results, {
                'tap': (True, "TAP OK"),
                'sia': (False, "SIA Error")
            })
            self.assertEqual(mock_get_status.call_count, 2)

    def test_is_service_available(self):
        """Test checking if service is available."""
        with patch.object(self.monitor, 'get_service_status', return_value=(True, "OK")):
            self.assertTrue(self.monitor.is_service_available('tap'))

        with patch.object(self.monitor, 'get_service_status', return_value=(False, "Error")):
            self.assertFalse(self.monitor.is_service_available('sia'))

    @patch('time.sleep')
    @patch.object(ServiceStatusMonitor, 'get_service_status')
    def test_wait_for_service_success(self, mock_get_status, mock_sleep):
        """Test waiting for service to become available - success case."""
        # First call returns False, second returns True
        mock_get_status.side_effect = [
            (False, "Service down"),
            (True, "Service up")
        ]

        result = self.monitor.wait_for_service('tap', max_wait_minutes=1, check_interval_seconds=1)

        self.assertTrue(result)
        self.assertEqual(mock_get_status.call_count, 2)
        mock_sleep.assert_called_once_with(1)

    @patch('time.sleep')
    @patch.object(ServiceStatusMonitor, 'get_service_status')
    def test_wait_for_service_timeout(self, mock_get_status, mock_sleep):
        """Test waiting for service to become available - timeout case."""
        # Always returns False
        mock_get_status.return_value = (False, "Service down")

        result = self.monitor.wait_for_service('tap', max_wait_minutes=0.02, check_interval_seconds=0.01)

        self.assertFalse(result)
        # Should be called at least once and then timeout
        self.assertGreater(mock_get_status.call_count, 1)

    def test_get_service_recommendations_both_available(self):
        """Test service recommendations when both services are available."""
        with patch.object(self.monitor, 'check_all_services', return_value={
            'tap': (True, "TAP OK"),
            'sia': (True, "SIA OK")
        }):
            recs = self.monitor.get_service_recommendations()

            self.assertIn('tap', recs['available_services'])
            self.assertIn('sia', recs['available_services'])
            self.assertEqual(len(recs['unavailable_services']), 0)
            self.assertIn("Both services are available", recs['recommendations'][0])

    def test_get_service_recommendations_tap_only(self):
        """Test service recommendations when only TAP is available."""
        with patch.object(self.monitor, 'check_all_services', return_value={
            'tap': (True, "TAP OK"),
            'sia': (False, "SIA down")
        }):
            recs = self.monitor.get_service_recommendations()

            self.assertIn('tap', recs['available_services'])
            self.assertEqual(len(recs['unavailable_services']), 1)
            self.assertIn("Use TAP service for all data access queries", recs['recommendations'][0])
            self.assertIn("Query image metadata through TAP", recs['workarounds'][0])

    def test_get_service_recommendations_none_available(self):
        """Test service recommendations when no services are available."""
        with patch.object(self.monitor, 'check_all_services', return_value={
            'tap': (False, "TAP down"),
            'sia': (False, "SIA down")
        }):
            recs = self.monitor.get_service_recommendations()

            self.assertEqual(len(recs['available_services']), 0)
            self.assertEqual(len(recs['unavailable_services']), 2)
            self.assertIn("Both services are unavailable", recs['recommendations'][0])


class TestCreateServiceMonitor(unittest.TestCase):
    """Test cases for create_service_monitor factory function."""

    def test_create_service_monitor_default_urls(self):
        """Test creating service monitor with default URLs."""
        monitor = create_service_monitor()

        self.assertEqual(monitor.tap_url, "https://data.lsst.cloud/api/tap")
        self.assertEqual(monitor.sia_url, "https://data.lsst.cloud/api/dp02/query")

    def test_create_service_monitor_custom_urls(self):
        """Test creating service monitor with custom URLs."""
        custom_tap = "https://custom.tap.service"
        custom_sia = "https://custom.sia.service"

        monitor = create_service_monitor(tap_url=custom_tap, sia_url=custom_sia)

        self.assertEqual(monitor.tap_url, custom_tap)
        self.assertEqual(monitor.sia_url, custom_sia)


if __name__ == '__main__':
    unittest.main()