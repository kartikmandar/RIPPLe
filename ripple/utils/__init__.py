"""
General Utility Functions for RIPPLe

This module provides a collection of utility functions and helper classes used
throughout the RIPPLe project. These tools are designed to support common operations,
enhance code reusability, and provide consistent interfaces for frequently needed tasks.

Key Components:
- ConfigManager: Utilities for loading and managing configuration files
- Logger: Logging utilities with standardized formatting and levels
- FileSystemUtils: Helper functions for file and directory operations
- MathUtils: Mathematical and statistical utility functions
- AstroUtils: Astronomy-specific calculations and coordinate transformations
- ValidationUtils: Functions for data validation and sanity checks
- SerializationUtils: Tools for data serialization and deserialization
- UtilsError, ConfigError, FileSystemError: Custom exceptions
"""

# Import configuration and logging utilities
from .config_manager import ConfigManager
from .logger import Logger

# Import file system and math utilities
from .file_system_utils import FileSystemUtils
from .math_utils import MathUtils

# Import astronomy-specific utilities
from .astro_utils import AstroUtils

# Import validation and serialization utilities
from .validation_utils import ValidationUtils
from .serialization_utils import SerializationUtils

# Import custom exceptions
from .exceptions import (
    UtilsError,
    ConfigError,
    FileSystemError
)

# Define __all__ for explicit public API
__all__ = [
    "ConfigManager",
    "Logger",
    "FileSystemUtils",
    "MathUtils",
    "AstroUtils",
    "ValidationUtils",
    "SerializationUtils",
    "UtilsError",
    "ConfigError",
    "FileSystemError"
]
