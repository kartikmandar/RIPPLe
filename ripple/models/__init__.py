"""
Model layer for RIPPLe — light, torch-free public surface.

`import ripple.models` succeeds with torch absent: only configuration,
the registry facade, the (lazy) factory/base-model classes, and the
exception hierarchy are exposed here. Heavy modules — ``components``,
``model_trainer``, ``model_evaluator``, ``vendored`` and the SR adapter —
import torch / torchvision / sklearn and are therefore imported lazily
*inside* the functions and methods that need them, NEVER at this module's
top level.

``builders`` is torch-free at import time (all builder callables only
import torch lazily inside the builder functions themselves); it is
eagerly populated here via ``_ensure_builders_registered`` so that
``list_models()`` returns all canonical keys right after
``import ripple.models``.

Public symbols:
- ModelConfig / TrainerConfig: frozen dataclass configs (+ ``from_dict``).
- ModelInterface / BaseModel / PredictionResult: the model contract.
- ModelFactory: builds a configured BaseModel from a registry key.
- ModelRegistry / register / get / list_models: the torch-free registry.
- ModelError + subclasses: the exception hierarchy.
"""

# Configuration (frozen dataclasses; torch only inside resolve_device()).
from .config import ModelConfig, TrainerConfig

# Model contract (BaseModel imports torch lazily inside its methods).
from .base_model import ModelInterface, BaseModel, PredictionResult

# Factory (lazily imports builders -> torch only inside create()).
from .model_factory import ModelFactory, _ensure_builders_registered

# Registry facade + module-level helpers (torch-free to import).
from .model_registry import ModelRegistry, register, get, list_models

# Exception hierarchy (plain Python; re-exported for convenience).
from .exceptions import (
    ModelError,
    ModelConfigError,
    ModelTrainingError,
    ModelLoadError,
    ModelPredictionError,
    ModelInferenceError,
    CheckpointError,
)

# Eagerly populate the registry so list_models() works immediately.
# builders.py is torch-free at import time; the builder *functions* only
# pull torch when called (inside create()). This call is idempotent.
_ensure_builders_registered()

__all__ = [
    "ModelConfig",
    "TrainerConfig",
    "ModelInterface",
    "BaseModel",
    "PredictionResult",
    "ModelFactory",
    "ModelRegistry",
    "register",
    "get",
    "list_models",
    "ModelError",
    "ModelConfigError",
    "ModelTrainingError",
    "ModelLoadError",
    "ModelPredictionError",
    "ModelInferenceError",
    "CheckpointError",
]
