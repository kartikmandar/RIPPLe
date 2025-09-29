"""
Data Preprocessing Utilities for RIPPLe

This module provides a comprehensive suite of preprocessing functions and utilities
for preparing astronomical data for analysis and machine learning tasks within RIPPLe.
It includes tools for normalization, cutout generation, and various data transformations.

Key Components:
- ImageNormalizer: Classes and functions for normalizing image data
- CutoutCreator: Utilities for creating image cutouts from larger fields
- DataCleaner: Tools for cleaning and filtering raw data
- FeatureExtractor: Functions for extracting relevant features from data
- DataTransformer: Utilities for various data transformations and augmentations
- PreprocessingConfig: Configuration class for preprocessing parameters
- PreprocessingError, NormalizationError, CutoutError: Custom exceptions
"""

# Import image processing utilities
from .image_normalizer import ImageNormalizer
from .cutout_creator import CutoutCreator
from .data_cleaner import DataCleaner

# Import feature extraction and transformation
from .feature_extractor import FeatureExtractor
from .data_transformer import DataTransformer

# Import configuration
from .config import PreprocessingConfig

# Import custom exceptions
from .exceptions import (
    PreprocessingError,
    NormalizationError,
    CutoutError
)

# Import main Preprocessor class
from .preprocessor import Preprocessor

# Define __all__ for explicit public API
__all__ = [
    "ImageNormalizer",
    "CutoutCreator",
    "DataCleaner",
    "FeatureExtractor",
    "DataTransformer",
    "PreprocessingConfig",
    "PreprocessingError",
    "NormalizationError",
    "CutoutError",
    "Preprocessor"
]