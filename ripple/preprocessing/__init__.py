"""
Data Preprocessing Utilities for RIPPLe

This module provides a comprehensive suite of preprocessing functions and utilities
for preparing astronomical data for analysis and machine learning tasks within RIPPLe.
It includes tools for normalization, cutout generation, and various data transformations.

Key Components:
- PreprocessingConfig: Configuration class for preprocessing parameters
- Preprocessor: End-to-end preprocessing orchestrator; produces (N,C,H,W) tensor batches
- PreprocessResult: Dataclass returned by Preprocessor.run()
- ImageNormalizer: Classes and functions for normalizing image data
- DataCleaner: Tools for cleaning and filtering raw data
- CleanResult: Dataclass returned by DataCleaner.clean()
- DataTransformer: Utilities for geometry/channel/tensor conversion
- CutoutCreator: Utilities for creating image cutouts from larger fields
- PSFMatcher: Cross-band PSF homogenization (default off)
- FeatureExtractor: Placeholder for future feature extraction
- RippleCutoutDataset: Map-style torch Dataset over accepted manifest rows (torch lazy)
- group_aware_split: Split rows into train/val/test by spatial group key
- ingest_labels_from_csv: Load label rows from a CSV file
- ingest_labels_from_dirs: Load label rows from a directory tree
- ingest_deeplense_dataset: Convert DeepLense class folders into a RIPPLe manifest + cutouts
- write_manifest / read_manifest / MANIFEST_FIELDS: CSV manifest helpers
- PreprocessingError, NormalizationError, CutoutError: Custom exceptions
- CleaningError, PSFMatchError, ManifestError: Additional custom exceptions
"""

# Import configuration
from .config import PreprocessingConfig

# Import main Preprocessor class and result type
from .preprocessor import Preprocessor, PreprocessResult

# Import image processing utilities
from .image_normalizer import ImageNormalizer
from .cutout_creator import CutoutCreator

# Import data cleaning utilities
from .data_cleaner import DataCleaner, CleanResult

# Import feature extraction and transformation
from .feature_extractor import FeatureExtractor
from .data_transformer import DataTransformer

# Import PSF matching
from .psf_matcher import PSFMatcher

# Import dataset and label/split helpers (torch imported lazily inside methods)
from .dataset import (
    RippleCutoutDataset,
    group_aware_split,
    ingest_labels_from_csv,
    ingest_labels_from_dirs,
    ingest_deeplense_dataset,
)

# Import manifest helpers
from .manifest import write_manifest, read_manifest, MANIFEST_FIELDS

# Import custom exceptions
from .exceptions import (
    PreprocessingError,
    NormalizationError,
    CutoutError,
    CleaningError,
    PSFMatchError,
    ManifestError,
)

# Define __all__ for explicit public API
__all__ = [
    # configuration
    "PreprocessingConfig",
    # orchestrator
    "Preprocessor",
    "PreprocessResult",
    # normalisation
    "ImageNormalizer",
    # cutouts
    "CutoutCreator",
    # cleaning
    "DataCleaner",
    "CleanResult",
    # feature extraction (placeholder)
    "FeatureExtractor",
    # tensor/geometry transforms
    "DataTransformer",
    # PSF matching
    "PSFMatcher",
    # dataset / label / split
    "RippleCutoutDataset",
    "group_aware_split",
    "ingest_labels_from_csv",
    "ingest_labels_from_dirs",
    "ingest_deeplense_dataset",
    # manifest
    "write_manifest",
    "read_manifest",
    "MANIFEST_FIELDS",
    # exceptions
    "PreprocessingError",
    "NormalizationError",
    "CutoutError",
    "CleaningError",
    "PSFMatchError",
    "ManifestError",
]
