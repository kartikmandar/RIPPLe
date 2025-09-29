"""
Data Processing Pipeline for RIPPLe

This module provides the core pipeline functionality for orchestrating complex
data processing workflows within RIPPLe. It defines a structured approach to
building, managing, and executing sequences of data transformation and analysis tasks.

Key Components:
- Pipeline: Abstract base class for defining processing pipelines
- PipelineStep: Interface for individual steps within a pipeline
- PipelineBuilder: Builder class for constructing complex pipelines
- PipelineExecutor: Handles the execution and monitoring of pipeline runs
- PipelineConfig: Configuration class for pipeline parameters and settings
- PipelineRegistry: Registry for discovering and managing available pipeline types
- PipelineError, PipelineConfigError, PipelineExecutionError: Custom exceptions
"""

# Import core pipeline classes
from .pipeline import Pipeline
from .pipeline_step import PipelineStep
from .pipeline_builder import PipelineBuilder
from .pipeline_executor import PipelineExecutor
from .pipeline_orchestrator import PipelineOrchestrator

# Import configuration and registry
from .config import PipelineConfig
from .pipeline_registry import PipelineRegistry

# Import custom exceptions
from .exceptions import (
    PipelineError,
    PipelineConfigError,
    PipelineExecutionError
)

# Define __all__ for explicit public API
__all__ = [
    "Pipeline",
    "PipelineStep",
    "PipelineBuilder",
    "PipelineExecutor",
    "PipelineOrchestrator",
    "PipelineConfig",
    "PipelineRegistry",
    "PipelineError",
    "PipelineConfigError",
    "PipelineExecutionError"
]