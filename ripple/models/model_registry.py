"""
Model registry for RIPPLe.

A module-level mapping of canonical model-type keys to builder callables.
Builders take a ModelConfig and return a configured BaseModel. Importing this
module is torch-free; heavy builders register themselves lazily (see
ripple.models.builders), so the registry can be listed without torch present.
"""
from __future__ import annotations

from typing import Callable, Dict, List

from .exceptions import ModelConfigError

# Canonical mapping: model_type -> builder callable (config) -> BaseModel.
_REGISTRY: Dict[str, Callable] = {}


def register(name: str) -> Callable[[Callable], Callable]:
    """Decorator that registers a builder callable under ``name``.

    Returns the original function unchanged so it can still be referenced
    directly. Raises ModelConfigError if ``name`` is already registered.
    """
    def _decorator(builder: Callable) -> Callable:
        if name in _REGISTRY:
            raise ModelConfigError(
                f"Model type '{name}' is already registered."
            )
        _REGISTRY[name] = builder
        return builder

    return _decorator


def get(name: str) -> Callable:
    """Return the builder callable registered under ``name``.

    Raises ModelConfigError if ``name`` is not registered.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise ModelConfigError(
            f"Unknown model type '{name}'. Registered types: {available}."
        )


def list_models() -> List[str]:
    """Return a sorted list of registered model-type keys."""
    return sorted(_REGISTRY)


class ModelRegistry:
    """Thin facade over the module-level registry functions."""

    @staticmethod
    def register(name: str) -> Callable[[Callable], Callable]:
        return register(name)

    @staticmethod
    def get(name: str) -> Callable:
        return get(name)

    @staticmethod
    def list_models() -> List[str]:
        return list_models()
