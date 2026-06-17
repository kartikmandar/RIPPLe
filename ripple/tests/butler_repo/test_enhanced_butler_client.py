#!/usr/bin/env python3
"""
Test script for enhanced ButlerClient with RSP token authentication support.

This script demonstrates how to use the enhanced ButlerClient with both local
repositories and remote Rubin Science Platform access using tokens.

The ``test_*`` functions are pytest tests: they raise on failure (via ``assert``)
and return ``None`` on success. ``main()`` runs them as a standalone script.
"""

import os
import logging
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # If python-dotenv is not available, manually load .env file
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_local_butler():
    """Test ButlerClient with local repository configuration."""
    logger.info("Testing local Butler configuration...")

    try:
        from ripple.data_access import ButlerConfig, ButlerClient

        # Create a local Butler configuration
        config = ButlerConfig(
            repo_path="/tmp/test_butler_repo",  # Replace with actual repo path
            collections=["test/collection"],
            auth_method="none"
        )

        # Test validation
        from ripple.data_access.config_examples import validate_config
        validation = validate_config(config)
        assert validation['valid'], \
            f"Local Butler configuration validation failed: {validation['errors']}"
        logger.info("✓ Local Butler configuration is valid")

        # Initialize ButlerClient (this will fail if repo doesn't exist, but tests the interface)
        try:
            client = ButlerClient(config=config)
            logger.info("✓ ButlerClient initialized successfully")
        except Exception as e:
            if "Config location" in str(e) and "does not exist" in str(e):
                logger.warning(f"ButlerClient initialization failed (expected if repo doesn't exist): {e}")
            else:
                raise

    except Exception as e:
        logger.error(f"Local Butler test failed: {e}")
        raise

def test_rsp_config():
    """Test RSP configuration without making actual connection."""
    logger.info("Testing RSP configuration...")

    try:
        from ripple.data_access import ButlerConfig, get_rsp_config
        from ripple.data_access.config_examples import validate_config

        # Test RSP config creation with mock token
        test_token = "mock_token_for_testing"
        config = ButlerConfig(
            server_url="https://data.lsst.cloud/api/butler/",
            access_token=test_token,
            token_username="x-oauth-basic",
            auth_method="token",
            collections=["2.2i/runs/DP0.2"],
            instrument="LSSTCam"
        )

        # Validate configuration
        validation = validate_config(config)
        assert validation['valid'], \
            f"RSP Butler configuration validation failed: {validation['errors']}"
        logger.info("✓ RSP Butler configuration is valid")

        # Test configuration parameters
        assert config.auth_method == "token"
        assert config.access_token == test_token
        assert config.token_username == "x-oauth-basic"
        assert config.server_url == "https://data.lsst.cloud/api/butler/"

        logger.info("✓ RSP configuration parameters are correct")

    except Exception as e:
        logger.error(f"RSP configuration test failed: {e}")
        raise

def test_rsp_config_with_env():
    """Test RSP configuration using environment variable."""
    logger.info("Testing RSP configuration with environment variable...")

    try:
        from ripple.data_access import get_rsp_config

        # Set mock environment variable
        os.environ["RSP_ACCESS_TOKEN"] = "mock_token_from_env"

        try:
            # This should work with the mock token
            config = get_rsp_config()
            logger.info("✓ get_rsp_config() works with environment variable")

            # Verify the token was loaded
            assert config.access_token == "mock_token_from_env"
            logger.info("✓ Token loaded correctly from environment")

        except ValueError as e:
            if "RSP_ACCESS_TOKEN environment variable is required" in str(e):
                raise AssertionError(
                    f"get_rsp_config() not finding environment variable: {e}"
                )
            else:
                raise
        finally:
            # Clean up environment variable
            if "RSP_ACCESS_TOKEN" in os.environ:
                del os.environ["RSP_ACCESS_TOKEN"]

    except Exception as e:
        logger.error(f"RSP environment variable test failed: {e}")
        raise

def test_butler_client_interface():
    """Test ButlerClient interface with different initialization methods."""
    logger.info("Testing ButlerClient interface...")

    try:
        from ripple.data_access import ButlerClient, ButlerConfig

        # Test 1: Initialize with individual parameters
        try:
            client1 = ButlerClient(
                repo_path="/tmp/test_repo",
                collection="test_collection",
                auth_method="none"
            )
            logger.info("✓ ButlerClient initialized with individual parameters")
        except Exception as e:
            if "Config location" in str(e) and "does not exist" in str(e):
                logger.info("✓ ButlerClient interface works (repo doesn't exist, as expected)")
            else:
                raise

        # Test 2: Initialize with real RSP configuration (if token available)
        rsp_token = os.environ.get("RSP_ACCESS_TOKEN")
        if rsp_token:
            logger.info("Testing with real RSP token...")
            config = ButlerConfig(
                server_url="https://data.lsst.cloud/api/butler/",
                access_token=rsp_token,
                token_username="x-oauth-basic",
                auth_method="token",
                collections=["2.2i/runs/DP0.2"],
                instrument="LSSTCam"
            )
            try:
                client2 = ButlerClient(config=config)
                logger.info("✓ ButlerClient initialized with real RSP config")

                # Test connection
                if client2.test_connection():
                    logger.info("✓ RSP connection test successful")
                else:
                    logger.warning("⚠ RSP connection test failed (token may be invalid)")

            except Exception as e:
                logger.warning(f"RSP connection failed (may be token/auth issue): {e}")
        else:
            logger.info("Skipping RSP connection test - no RSP_ACCESS_TOKEN found")
            # Test with placeholder config (no actual connection)
            config = ButlerConfig(
                server_url="https://data.lsst.cloud/api/butler/",
                access_token="placeholder_token",
                auth_method="token",
                collections=["2.2i/runs/DP0.2"]
            )
            # Just validate the configuration, don't actually connect
            from ripple.data_access.config_examples import validate_config
            validation = validate_config(config)
            assert validation['valid'], \
                f"RSP configuration validation failed: {validation['errors']}"
            logger.info("✓ RSP configuration validation passed (no actual connection)")

        # Test 3: Test backward compatibility (old interface)
        try:
            client3 = ButlerClient(
                repo_path="/tmp/test_repo",
                collection="test_collection"
            )
            logger.info("✓ ButlerClient backward compatibility maintained")
        except Exception as e:
            if "Config location" in str(e) and "does not exist" in str(e):
                logger.info("✓ ButlerClient backward compatibility works (repo doesn't exist)")
            else:
                logger.warning(f"Backward compatibility test: {e}")

    except Exception as e:
        logger.error(f"ButlerClient interface test failed: {e}")
        raise

def test_authentication_validation():
    """Test authentication validation logic."""
    logger.info("Testing authentication validation...")

    try:
        from ripple.data_access import ButlerConfig
        from ripple.data_access.config_examples import validate_config

        # Test 1: Missing token with token auth
        try:
            config = ButlerConfig(
                server_url="https://test.example.com",
                auth_method="token",
                # Missing access_token
            )
            validation = validate_config(config)
            assert not validation['valid']
            assert "access_token is required when auth_method is 'token'" in validation['errors']
            logger.info("✓ Correctly rejected token auth without token")
        except ValueError:
            # This should be caught by __post_init__ validation too
            logger.info("✓ Correctly rejected token auth without token (post_init)")

        # Test 2: Token with wrong auth method
        try:
            config = ButlerConfig(
                repo_path="/tmp/test",
                access_token="test_token",
                auth_method="none"  # Wrong method
            )
            validation = validate_config(config)
            assert not validation['valid']
            assert "access_token can only be used with auth_method='token'" in validation['errors']
            logger.info("✓ Correctly rejected token with wrong auth method")
        except ValueError:
            logger.info("✓ Correctly rejected token with wrong auth method (post_init)")

        # Test 3: Valid configuration
        config = ButlerConfig(
            server_url="https://test.example.com",
            access_token="test_token",
            auth_method="token",
            collections=["test"]
        )
        validation = validate_config(config)
        assert validation['valid']
        logger.info("✓ Valid token configuration accepted")

    except Exception as e:
        logger.error(f"Authentication validation test failed: {e}")
        raise

def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Running enhanced ButlerClient tests")
    logger.info("=" * 60)

    tests = [
        ("Local Butler Configuration", test_local_butler),
        ("RSP Configuration", test_rsp_config),
        ("RSP Environment Variable", test_rsp_config_with_env),
        ("ButlerClient Interface", test_butler_client_interface),
        ("Authentication Validation", test_authentication_validation),
    ]

    results = []
    for test_name, test_func in tests:
        logger.info(f"\n--- Testing {test_name} ---")
        try:
            # pytest-style tests pass by returning None and fail by raising.
            test_func()
            results.append((test_name, True))
            logger.info(f"{test_name}: PASS")
        except Exception as e:
            logger.error(f"{test_name}: FAIL - {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {test_name}")

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed! Enhanced ButlerClient is ready for use.")
    else:
        logger.warning("Some tests failed. Please check the implementation.")

    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
