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

class ModelLoadError(ModelError):
    """Exception raised when loading model weights or a checkpoint fails."""
    pass

class ModelPredictionError(ModelError):
    """Exception raised for errors while producing a prediction."""
    pass

class ModelInferenceError(ModelError):
    """Exception raised for errors during batched/dataset inference."""
    pass

class CheckpointError(ModelError):
    """Exception raised for malformed or incompatible model checkpoints."""
    pass
