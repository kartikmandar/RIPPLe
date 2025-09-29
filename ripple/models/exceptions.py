"""
Custom exceptions for the RIPPLe models module.
"""

class ModelError(Exception):
    """Base exception for all model-related errors."""
    pass

class ModelConfigError(ModelError):
    """Exception raised for errors in model configuration."""
    pass

class ModelTrainingError(ModelError):
    """Exception raised for errors during model training."""
    pass