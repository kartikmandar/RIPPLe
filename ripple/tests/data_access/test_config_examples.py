"""
Unit tests for ButlerConfig and config validation functions.

This module tests the configuration dataclass, validation logic,
and factory functions for creating Butler configurations.
"""

import unittest
from unittest.mock import patch
import os
import sys
from pathlib import Path

# Add the ripple directory to path for direct import
# This avoids importing through ripple/__init__.py which requires LSST
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Import directly from the module file to avoid LSST dependencies
from ripple.data_access.config_examples import (
    ButlerConfig,
    get_default_config,
    get_production_config,
    get_rsp_config,
    validate_config
)


class TestButlerConfig(unittest.TestCase):
    """Test cases for ButlerConfig dataclass."""

    def test_init_with_repo_path(self):
        """Test initialization with local repository path."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["raw/all"]
        )
        self.assertEqual(config.repo_path, "/path/to/repo")
        self.assertEqual(config.collections, ["raw/all"])
        self.assertIsNone(config.server_url)
        self.assertEqual(config.auth_method, "none")

    def test_init_with_server_url(self):
        """Test initialization with remote server URL."""
        config = ButlerConfig(
            server_url="https://butler.example.com",
            collections=["HSC/runs/RC2"]
        )
        self.assertEqual(config.server_url, "https://butler.example.com")
        self.assertEqual(config.collections, ["HSC/runs/RC2"])
        self.assertIsNone(config.repo_path)

    def test_init_with_token_auth(self):
        """Test initialization with token authentication."""
        config = ButlerConfig(
            server_url="https://butler.example.com",
            access_token="test_token",
            auth_method="token",
            collections=["test/collection"]
        )
        self.assertEqual(config.access_token, "test_token")
        self.assertEqual(config.auth_method, "token")
        self.assertEqual(config.token_username, "x-oauth-basic")

    def test_init_missing_repo_and_server(self):
        """Test that initialization fails without repo_path or server_url."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(collections=["test"])
        self.assertIn("Either repo_path or server_url must be specified", str(ctx.exception))

    def test_init_invalid_auth_method(self):
        """Test that initialization fails with invalid auth_method."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                auth_method="invalid"
            )
        self.assertIn("auth_method must be one of", str(ctx.exception))

    def test_init_token_without_access_token(self):
        """Test that token auth requires access_token."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                server_url="https://butler.example.com",
                auth_method="token"
            )
        self.assertIn("access_token is required when auth_method is 'token'", str(ctx.exception))

    def test_init_token_without_server_url(self):
        """Test that token auth requires server_url."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                access_token="test_token",
                auth_method="token"
            )
        self.assertIn("server_url is required when using token authentication", str(ctx.exception))

    def test_init_access_token_without_token_auth(self):
        """Test that access_token requires auth_method='token'."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                access_token="test_token",
                auth_method="none"
            )
        self.assertIn("access_token can only be used with auth_method='token'", str(ctx.exception))

    def test_init_negative_cache_size(self):
        """Test that negative cache_size is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                cache_size=-1
            )
        self.assertIn("cache_size must be non-negative", str(ctx.exception))

    def test_init_zero_timeout(self):
        """Test that zero timeout is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                timeout=0
            )
        self.assertIn("timeout must be positive", str(ctx.exception))

    def test_init_negative_timeout(self):
        """Test that negative timeout is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                timeout=-1.0
            )
        self.assertIn("timeout must be positive", str(ctx.exception))

    def test_init_negative_retry_attempts(self):
        """Test that negative retry_attempts is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                retry_attempts=-1
            )
        self.assertIn("retry_attempts must be non-negative", str(ctx.exception))

    def test_init_zero_max_connections(self):
        """Test that zero max_connections is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                max_connections=0
            )
        self.assertIn("max_connections must be positive", str(ctx.exception))

    def test_init_zero_max_workers(self):
        """Test that zero max_workers is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                max_workers=0
            )
        self.assertIn("max_workers must be positive", str(ctx.exception))

    def test_init_zero_batch_size(self):
        """Test that zero batch_size is rejected."""
        with self.assertRaises(ValueError) as ctx:
            ButlerConfig(
                repo_path="/path/to/repo",
                batch_size=0
            )
        self.assertIn("batch_size must be positive", str(ctx.exception))

    def test_default_values(self):
        """Test that default values are set correctly."""
        config = ButlerConfig(repo_path="/path/to/repo")
        self.assertEqual(config.token_username, "x-oauth-basic")
        self.assertEqual(config.auth_method, "none")
        self.assertEqual(config.collections, [])
        self.assertIsNone(config.instrument)
        self.assertEqual(config.cache_size, 1000)
        self.assertEqual(config.timeout, 30.0)
        self.assertEqual(config.retry_attempts, 3)
        self.assertEqual(config.max_connections, 5)
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.batch_size, 100)
        self.assertFalse(config.enable_performance_monitoring)

    def test_get_method_existing_key(self):
        """Test get() method with existing key."""
        config = ButlerConfig(repo_path="/path/to/repo", cache_size=500)
        self.assertEqual(config.get("cache_size"), 500)
        self.assertEqual(config.get("repo_path"), "/path/to/repo")

    def test_get_method_missing_key(self):
        """Test get() method with missing key returns default."""
        config = ButlerConfig(repo_path="/path/to/repo")
        self.assertIsNone(config.get("nonexistent_key"))
        self.assertEqual(config.get("nonexistent_key", "default"), "default")

    def test_both_repo_path_and_server_url_allowed(self):
        """Test that both repo_path and server_url can be specified."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            server_url="https://butler.example.com"
        )
        self.assertEqual(config.repo_path, "/path/to/repo")
        self.assertEqual(config.server_url, "https://butler.example.com")


class TestGetDefaultConfig(unittest.TestCase):
    """Test cases for get_default_config factory function."""

    def test_returns_butler_config(self):
        """Test that get_default_config returns a ButlerConfig instance."""
        config = get_default_config()
        self.assertIsInstance(config, ButlerConfig)

    def test_default_config_values(self):
        """Test that default config has expected values."""
        config = get_default_config()
        self.assertEqual(config.repo_path, "/tmp/butler_repo")
        self.assertEqual(config.collections, ["raw/all"])
        self.assertEqual(config.instrument, "HSC")
        self.assertEqual(config.cache_size, 1000)
        self.assertEqual(config.timeout, 30.0)
        self.assertEqual(config.retry_attempts, 3)


class TestGetProductionConfig(unittest.TestCase):
    """Test cases for get_production_config factory function."""

    def test_returns_butler_config(self):
        """Test that get_production_config returns a ButlerConfig instance."""
        config = get_production_config()
        self.assertIsInstance(config, ButlerConfig)

    def test_production_config_values(self):
        """Test that production config has expected values."""
        config = get_production_config()
        self.assertEqual(config.server_url, "https://butler.lsst.org")
        self.assertEqual(config.collections, ["HSC/runs/RC2"])
        self.assertEqual(config.cache_size, 5000)
        self.assertEqual(config.timeout, 120.0)
        self.assertEqual(config.retry_attempts, 5)
        self.assertEqual(config.max_connections, 10)
        self.assertEqual(config.max_workers, 8)
        self.assertTrue(config.enable_performance_monitoring)


class TestGetRspConfig(unittest.TestCase):
    """Test cases for get_rsp_config factory function."""

    def test_missing_token_raises_error(self):
        """Test that missing RSP_ACCESS_TOKEN raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            # Ensure the token is not set
            if "RSP_ACCESS_TOKEN" in os.environ:
                del os.environ["RSP_ACCESS_TOKEN"]
            with self.assertRaises(ValueError) as ctx:
                get_rsp_config()
            self.assertIn("RSP_ACCESS_TOKEN environment variable is required", str(ctx.exception))

    def test_with_token_returns_config(self):
        """Test that get_rsp_config returns config when token is set."""
        with patch.dict(os.environ, {"RSP_ACCESS_TOKEN": "test_token_value"}):
            config = get_rsp_config()
            self.assertIsInstance(config, ButlerConfig)
            self.assertEqual(config.server_url, "https://data.lsst.cloud/api/butler/")
            self.assertEqual(config.access_token, "test_token_value")
            self.assertEqual(config.auth_method, "token")
            self.assertEqual(config.collections, ["2.2i/runs/DP0.2"])
            self.assertEqual(config.instrument, "LSSTCam")


class TestValidateConfig(unittest.TestCase):
    """Test cases for validate_config function."""

    def test_valid_config(self):
        """Test validation of a valid configuration."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["test/collection"]
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)

    def test_empty_collections_error(self):
        """Test that empty collections generates an error."""
        config = ButlerConfig(repo_path="/path/to/repo")
        result = validate_config(config)
        self.assertFalse(result['valid'])
        self.assertIn("collections cannot be empty", result['errors'])

    def test_both_paths_warning(self):
        """Test that having both repo_path and server_url generates warning."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            server_url="https://butler.example.com",
            collections=["test"]
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])  # Still valid, just a warning
        self.assertIn("Both repo_path and server_url specified - using repo_path", result['warnings'])

    def test_large_cache_size_warning(self):
        """Test that large cache_size generates a warning."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["test"],
            cache_size=15000
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])
        self.assertIn("Large cache_size may impact memory usage", result['warnings'])

    def test_long_timeout_warning(self):
        """Test that long timeout generates a warning."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["test"],
            timeout=400.0
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])
        self.assertIn("Long timeout may cause slow failure detection", result['warnings'])

    def test_high_retry_attempts_warning(self):
        """Test that high retry_attempts generates a warning."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["test"],
            retry_attempts=15
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])
        self.assertIn("High retry_attempts may cause prolonged failures", result['warnings'])

    def test_high_max_workers_warning(self):
        """Test that high max_workers generates a warning."""
        config = ButlerConfig(
            repo_path="/path/to/repo",
            collections=["test"],
            max_workers=20
        )
        result = validate_config(config)
        self.assertTrue(result['valid'])
        self.assertIn("High max_workers may impact system performance", result['warnings'])

    def test_multiple_errors(self):
        """Test that multiple validation errors are reported."""
        # Create a config that will pass __post_init__ but fail validate_config
        config = ButlerConfig(repo_path="/path/to/repo")
        # Manually set invalid values after construction for testing validate_config
        result = validate_config(config)
        # Should have at least the empty collections error
        self.assertFalse(result['valid'])
        self.assertGreater(len(result['errors']), 0)

    def test_auth_validation_token_without_access_token(self):
        """Test auth validation when token auth is set without access_token."""
        # This test validates the validate_config function's independent checks
        # We need to create a config that bypasses __post_init__ checks
        # For this we test by creating a mock-like object
        class MockConfig:
            repo_path = None
            server_url = "https://example.com"
            auth_method = "token"
            access_token = None
            collections = ["test"]
            cache_size = 1000
            timeout = 30.0
            retry_attempts = 3
            max_workers = 4

        result = validate_config(MockConfig())
        self.assertFalse(result['valid'])
        self.assertIn("access_token is required when auth_method is 'token'", result['errors'])


if __name__ == '__main__':
    unittest.main()
