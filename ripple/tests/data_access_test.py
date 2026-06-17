"""
Test Suite for Enhanced LSST Data Access

This module provides comprehensive testing for the enhanced LSST data access
functionality including Butler integration, coordinate resolution, and performance monitoring.
"""

import unittest
import logging
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Import the modules to test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from ripple.data_access.butler_client import ButlerClient
from ripple.data_access.data_fetcher import LsstDataFetcher
from ripple.data_access.coordinate_resolver import CoordinateResolver, create_coordinate_resolver
from ripple.utils.performance_monitor import PerformanceMonitor, get_performance_monitor
from ripple.data_access.exceptions import DataAccessError, ButlerConnectionError


class TestButlerClientEnhancements(unittest.TestCase):
    """Test Butler Client enhancements."""

    def setUp(self):
        """Set up test environment."""
        self.mock_butler = Mock()
        self.config = {
            "repo_path": "/test/repo",
            "collections": ["test/collection"]
        }

    def _make_client_with_mock_butler(self, mock_butler):
        """Build a real ButlerClient instance backed by a mock Butler.

        ButlerClient.__init__ now eagerly constructs a real Butler from the
        repo_path. For unit testing the higher-level methods we bypass the
        config validation and real Butler initialization, injecting a mock
        Butler instead.
        """
        with patch.object(ButlerClient, '_validate_config', return_value=None), \
             patch.object(ButlerClient, '_initialize_butler', return_value=mock_butler):
            client = ButlerClient(repo_path="/test", collections=["test"])
        return client

    def test_dataid_validation(self):
        """Test DataId validation functionality."""
        mock_butler = Mock()
        # Mock successful validation
        mock_butler.registry.expandDataId.return_value = {"tract": 9813, "patch": 42}
        mock_butler.find_dataset.return_value = Mock()

        client = self._make_client_with_mock_butler(mock_butler)
        is_valid, result = client._validate_dataid('deepCoadd', {"tract": 9813, "patch": 42})

        self.assertTrue(is_valid)
        self.assertEqual(result, {"tract": 9813, "patch": 42})

    def test_bbox_retry_mechanism(self):
        """Test bbox-based retrieval with retry logic."""
        mock_butler = Mock()
        # Mock successful bbox retrieval
        mock_butler.get.return_value = "test_cutout"

        client = self._make_client_with_mock_butler(mock_butler)
        from lsst.geom import Box2I, Point2I, Extent2I
        bbox = Box2I(Point2I(0, 0), Extent2I(100, 100))

        result = client._get_with_bbox_retry('deepCoadd', {"tract": 9813}, bbox)
        self.assertEqual(result, "test_cutout")

    def test_coordinate_based_cutout(self):
        """Test coordinate-based cutout retrieval."""
        mock_butler = Mock()

        # Mock skymap and coordinate conversion
        mock_skymap = Mock()
        mock_tract_info = Mock()
        mock_tract_info.tract_id = 9813

        mock_patch_info = Mock()
        mock_patch_info.getIndex.return_value = 42

        mock_skymap.findTract.return_value = mock_tract_info
        mock_tract_info.findPatch.return_value = mock_patch_info

        # WCS used for coordinate -> pixel conversion in get_cutout
        mock_wcs = Mock()
        mock_pixel_pos = Mock()
        mock_pixel_pos.x = 1000.0
        mock_pixel_pos.y = 1000.0
        mock_wcs.skyToPixel.return_value = mock_pixel_pos
        mock_wcs.getPixelScale.return_value.asArcseconds.return_value = 0.2

        mock_butler.get.side_effect = [
            mock_skymap,    # skyMap
            mock_wcs,       # deepCoadd.wcs
            "test_cutout",  # final cutout via _get_with_bbox_retry
        ]

        client = self._make_client_with_mock_butler(mock_butler)
        result = client.get_cutout(150.0, 2.5, 60.0, "i")

        self.assertEqual(result, "test_cutout")
        mock_butler.get.assert_called()


class TestCoordinateResolver(unittest.TestCase):
    """Test coordinate resolution functionality."""

    def setUp(self):
        """Set up test environment."""
        self.mock_butler = Mock()
        self.resolver = CoordinateResolver(self.mock_butler)

    def test_ra_dec_to_tract_patch(self):
        """Test RA/Dec to tract/patch conversion."""
        # Mock skymap responses
        mock_skymap = Mock()
        mock_tract_info = Mock()
        mock_tract_info.tract_id = 9813

        mock_patch_info = Mock()
        mock_patch_info.getIndex.return_value = 42

        mock_skymap.findTract.return_value = mock_tract_info
        mock_tract_info.findPatch.return_value = mock_patch_info
        self.mock_butler.get.return_value = mock_skymap

        result = self.resolver.ra_dec_to_tract_patch(150.0, 2.5)

        self.assertEqual(result, (9813, 42))
        mock_skymap.findTract.assert_called()

    def test_coord_to_dataid(self):
        """Test coordinate to DataId conversion."""
        # Mock skymap responses
        mock_skymap = Mock()
        mock_tract_info = Mock()
        mock_tract_info.tract_id = 9813

        mock_patch_info = Mock()
        mock_patch_info.getIndex.return_value = 42

        mock_skymap.findTract.return_value = mock_tract_info
        mock_tract_info.findPatch.return_value = mock_patch_info
        self.mock_butler.get.return_value = mock_skymap

        result = self.resolver.coord_to_dataid(150.0, 2.5, "i")

        expected = {"tract": 9813, "patch": 42, "band": "i"}
        self.assertEqual(result, expected)

    def test_tract_center_calculation(self):
        """Test tract center calculation."""
        # Mock skymap response. The skymap is subscripted (skymap[tract]) so it
        # must be a MagicMock to support __getitem__.
        mock_skymap = MagicMock()
        mock_tract_info = Mock()
        mock_center = Mock()
        # get_tract_center reads center.getRa().asDegrees() / getDec().asDegrees()
        mock_center.getRa.return_value.asDegrees.return_value = 150.0
        mock_center.getDec.return_value.asDegrees.return_value = 2.5

        mock_tract_info.getCtr.return_value = mock_center
        mock_skymap.__getitem__.return_value = mock_tract_info
        self.mock_butler.get.return_value = mock_skymap

        result = self.resolver.get_tract_center(9813)

        self.assertEqual(result, (150.0, 2.5))

    def test_sky_coverage_validation(self):
        """Test sky coverage validation."""
        from lsst.geom import SpherePoint, degrees

        # Mock successful coordinate conversion
        self.resolver.ra_dec_to_tract_patch = Mock(return_value=(9813, 42))

        # validate_sky_coverage subscripts the skymap (skymap[tract_id]) and uses
        # the tract center in a real SpherePoint.separation() call, so the skymap
        # must support __getitem__ (MagicMock) and the center must be a real
        # SpherePoint.
        mock_skymap = MagicMock()
        mock_tract_info = Mock()
        mock_tract_info.getCtr.return_value = SpherePoint(150.0 * degrees, 2.5 * degrees)
        mock_skymap.__getitem__.return_value = mock_tract_info
        self.resolver._get_skymap = Mock(return_value=mock_skymap)

        result = self.resolver.validate_sky_coverage(150.0, 2.5, 0.1)

        self.assertTrue(result["valid"])
        self.assertEqual(result["tract_id"], 9813)
        self.assertEqual(result["patch_index"], 42)


class TestEnhancedDataFetcher(unittest.TestCase):
    """Test enhanced LsstDataFetcher functionality."""

    def setUp(self):
        """Set up test environment."""
        self.config = {
            'data_source': {
                'type': 'butler_repo',
                'params': {
                    'path': '/test/repo',
                    'collections': ['test/collection']
                }
            }
        }

    @patch('ripple.data_access.data_fetcher.ButlerClient')
    def test_enhanced_cutout_retrieval(self, mock_butler_client):
        """Test enhanced cutout retrieval with fallback."""
        # Mock Butler client
        mock_instance = Mock()
        mock_butler_client.return_value = mock_instance
        mock_instance.test_connection.return_value = True
        mock_instance.get_cutout.return_value = "test_cutout"

        fetcher = LsstDataFetcher(self.config)

        result = fetcher.get_cutout(150.0, 2.5, 60.0, "i", backend="butler")

        self.assertEqual(result, "test_cutout")
        mock_instance.get_cutout.assert_called_once()

    @patch('ripple.data_access.data_fetcher.ButlerClient')
    @patch('ripple.data_access.data_fetcher.RSPTAPClient')
    def test_auto_backend_selection(self, mock_rsp_client, mock_butler_client):
        """Test automatic backend selection."""
        # Mock Butler client available
        mock_butler_instance = Mock()
        mock_butler_client.return_value = mock_butler_instance
        mock_butler_instance.test_connection.return_value = True

        # Mock RSP client
        mock_rsp_instance = Mock()
        mock_rsp_client.return_value = mock_rsp_instance

        fetcher = LsstDataFetcher(self.config)

        # Should select Butler backend when available
        result = fetcher.get_cutout(150.0, 2.5, backend="auto")

        # Verify Butler was attempted
        mock_butler_instance.get_cutout.assert_called_once()

    def test_batch_cutout_retrieval(self):
        """Test parallel batch cutout retrieval."""
        with patch.object(LsstDataFetcher, 'get_cutout') as mock_get_cutout:
            # batch_get_cutouts runs in a thread pool and collects results in
            # completion order (as_completed), which is non-deterministic. Map
            # each coordinate to its cutout by RA so the assertions are
            # order-independent rather than relying on a positional side_effect.
            cutout_by_ra = {150.0: "cutout1", 150.1: "cutout2", 150.2: "cutout3"}
            mock_get_cutout.side_effect = lambda ra, dec, *args, **kwargs: cutout_by_ra[ra]

            fetcher = LsstDataFetcher(self.config)
            coordinates = [(150.0, 2.5), (150.1, 2.5), (150.2, 2.5)]

            results = fetcher.batch_get_cutouts(coordinates, max_workers=2)

            # Check all results (order-independent, keyed by RA)
            self.assertEqual(len(results), 3)
            results_by_ra = {r["ra"]: r["cutout"] for r in results}
            self.assertEqual(results_by_ra[150.0], "cutout1")
            self.assertEqual(results_by_ra[150.1], "cutout2")
            self.assertEqual(results_by_ra[150.2], "cutout3")

    def test_multi_band_cutout(self):
        """Test multi-band cutout retrieval."""
        with patch.object(LsstDataFetcher, 'get_cutout') as mock_get_cutout:
            # Mock band-specific cutouts
            mock_get_cutout.side_effect = [
                "g_band_cutout", "r_band_cutout", "i_band_cutout"
            ]

            fetcher = LsstDataFetcher(self.config)
            bands = ["g", "r", "i"]

            results = fetcher.get_multi_band_cutout(150.0, 2.5, bands=bands)

            self.assertEqual(len(results), 3)
            self.assertEqual(results["g"], "g_band_cutout")
            self.assertEqual(results["r"], "r_band_cutout")
            self.assertEqual(results["i"], "i_band_cutout")


class TestPerformanceMonitoring(unittest.TestCase):
    """Test performance monitoring functionality."""

    def setUp(self):
        """Set up test environment."""
        self.monitor = PerformanceMonitor()

    def test_operation_tracking(self):
        """Test operation performance tracking."""
        @self.monitor.track_operation("test_operation")
        def test_function():
            return "test_result"

        result = test_function()

        self.assertEqual(result, "test_result")
        self.assertEqual(len(self.monitor.metrics), 1)
        self.assertEqual(self.monitor.metrics[0].operation_name, "test_operation")
        self.assertTrue(self.monitor.metrics[0].success)

    def test_performance_summary(self):
        """Test performance summary generation."""
        # Add some test metrics
        self.monitor.metrics = [
            type('Metric', (), {
                'operation_name': 'test1', 'start_time': 1000.0, 'end_time': 1001.0,
                'duration': 1.0, 'success': True
            }),
            type('Metric', (), {
                'operation_name': 'test2', 'start_time': 1002.0, 'end_time': 1005.0,
                'duration': 3.0, 'success': True
            }),
            type('Metric', (), {
                'operation_name': 'test1', 'start_time': 1006.0, 'end_time': 1007.0,
                'duration': 1.0, 'success': False
            })
        ]

        summary = self.monitor.get_performance_summary()

        self.assertEqual(summary["total_operations"], 3)
        self.assertEqual(summary["successful_operations"], 2)
        self.assertEqual(summary["failed_operations"], 1)
        self.assertAlmostEqual(summary["avg_duration"], 1.6667, places=4)
        self.assertEqual(summary["min_duration"], 1.0)
        self.assertEqual(summary["max_duration"], 3.0)

    def test_export_functionality(self):
        """Test metrics export functionality."""
        import tempfile
        import os

        # Create temporary file for testing
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Add test metrics
            self.monitor.metrics = [
                type('Metric', (), {
                    'operation_name': 'test_export', 'start_time': 1000.0,
                    'end_time': 1001.0, 'duration': 1.0, 'success': True,
                    'error_message': None, 'metadata': None
                })
            ]

            # Test export
            result = self.monitor.export_metrics(temp_path, "json")
            self.assertTrue(result)

            # Verify file exists and contains expected content
            self.assertTrue(os.path.exists(temp_path))

        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)


def create_performance_tests():
    """Create performance benchmarks for enhanced data access."""
    logger = logging.getLogger(__name__)
    logger.info("Running performance benchmarks...")

    # Test coordinate resolution performance
    monitor = get_performance_monitor()
    resolver = CoordinateResolver(Mock())

    with monitor.measure_operation("coordinate_resolution_test"):
        # Simulate multiple coordinate resolutions
        for i in range(100):
            resolver.ra_dec_to_tract_patch(150.0 + i*0.01, 2.5 + i*0.01)

    # Test batch retrieval simulation
    with monitor.measure_operation("batch_retrieval_test"):
        # Simulate batch processing
        results = []
        for i in range(50):
            results.append({"index": i, "data": f"test_data_{i}"})

    logger.info("Performance benchmarks completed")
    monitor.print_summary_report()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run unit tests
    unittest.main(verbosity=2)

    # Uncomment to run performance benchmarks
    # create_performance_tests()