"""
Model Interfaces and Factories for RIPPLe

This module defines the core abstractions and factory patterns for machine learning
models used within the RIPPLe framework. It provides a unified interface for
various model types, enabling seamless integration and experimentation.

Key Components:
- BaseModel: Abstract base class for all ML models in the framework
- ModelFactory: Factory class for instantiating different model implementations
- ModelRegistry: Registry for discovering and managing available model types
- ModelConfig: Configuration class for model parameters and settings
- ModelTrainer: Interface and utilities for model training workflows
- ModelEvaluator: Interface and utilities for model evaluation and metrics
- ModelError, ModelConfigError, ModelTrainingError: Custom exceptions
"""

# Import base model classes and interfaces
from .base_model import ModelInterface, BaseModel
from .model_factory import ModelFactory
from .model_registry import ModelRegistry

# Import configuration and utilities
# from .config import ModelConfig  # Commented out until config.py is created
from .model_trainer import ModelTrainer
from .model_evaluator import ModelEvaluator

# Import custom exceptions
from .exceptions import (
    ModelError,
    ModelConfigError,
    ModelTrainingError
)

# Define __all__ for explicit public API
__all__ = [
    "ModelInterface",
    "BaseModel",
    "ModelFactory",
    "ModelRegistry",
    # "ModelConfig",  # Commented out until config.py is created
    "ModelTrainer",
    "ModelEvaluator",
    "ModelError",
    "ModelConfigError",
    "ModelTrainingError"
]