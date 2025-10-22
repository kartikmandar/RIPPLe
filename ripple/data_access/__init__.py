"""
Data Access Layer for RIPPLe

This module provides the core data access functionality for LSST data retrieval
using Butler Gen3 architecture with optimized performance and comprehensive
error handling.

Key Components:
- LsstDataFetcher: Main interface for LSST data retrieval with Butler
- ButlerConfig: Configuration class for Butler connections
- ButlerClient: Butler wrapper with error handling
- CoordinateConverter: Utilities for RA/Dec to tract/patch conversion
- CacheManager: In-memory caching for cutouts
- DataAccessError, ButlerConnectionError, CoordinateConversionError: Custom exceptions
"""

# Import main data access classes
from .data_fetcher import LsstDataFetcher
from .config_examples import ButlerConfig, get_rsp_config, get_production_config
from .butler_client import ButlerClient
from .rsp_tap_client import RSPTAPClient, create_rsp_client
from .coordinate_utils import CoordinateConverter
from .cache_manager import CacheManager

# Import custom exceptions
from .exceptions import (
    DataAccessError,
    ButlerConnectionError,
    CoordinateConversionError
)

# Define __all__ for explicit public API
__all__ = [
    "LsstDataFetcher",
    "ButlerConfig",
    "ButlerClient",
    "RSPTAPClient",
    "create_rsp_client",
    "CoordinateConverter",
    "CacheManager",
    "DataAccessError",
    "ButlerConnectionError",
    "CoordinateConversionError",
    "get_rsp_config",
    "get_production_config"
]
