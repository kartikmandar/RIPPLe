"""
ModelFactory: build a configured BaseModel from a registry key.

Lazily imports ripple.models.builders so the heavy torch-backed builders
register themselves on first use; the factory then dispatches through the
torch-free registry and loads weights if the config requests them.
"""
from __future__ import annotations

import dataclasses
from typing import Optional, Union

from .config import ModelConfig
from .model_registry import get


def _ensure_builders_registered() -> None:
    """Import the builders module for its registration side effects.

    Best-effort: if torch/torchvision are absent the import will fail, but a
    key that was registered by other means (or is genuinely missing) is still
    resolved/raised by ``get`` below, so swallow only the ImportError here.
    """
    try:
        from . import builders  # noqa: F401
    except ImportError:
        pass


class ModelFactory:
    """Factory for constructing BaseModel instances from registry keys."""

    @classmethod
    def create(
        cls,
        model_type: str,
        config: Optional[Union[ModelConfig, dict]] = None,
    ):
        """Build and return a configured BaseModel for ``model_type``.

        ``config`` may be a ModelConfig, a dict (coerced via from_dict), or
        None (defaults used). The config's ``model_type`` is always set to
        ``model_type``. If the resolved config has a ``weights_path``, the
        built model's ``load_weights`` is invoked with it.
        """
        if config is None:
            config = ModelConfig()
        elif isinstance(config, dict):
            config = ModelConfig.from_dict(config)
        elif not isinstance(config, ModelConfig):
            from .exceptions import ModelConfigError
            raise ModelConfigError(
                "config must be a ModelConfig, a dict, or None; "
                f"got {type(config).__name__}."
            )

        config = dataclasses.replace(config, model_type=model_type)

        _ensure_builders_registered()
        builder = get(model_type)
        obj = builder(config)

        if config.weights_path:
            obj.load_weights(config.weights_path)

        if getattr(config, "encoder_weights_path", None):
            obj.load_encoder_weights(config.encoder_weights_path)

        return obj

    @classmethod
    def from_dict(cls, d: dict):
        """Build a BaseModel from a config dict, dispatching on ``model_type``.

        ``model_type`` defaults to the ModelConfig default when absent.
        """
        model_type = d.get("model_type", ModelConfig().model_type)
        return cls.create(model_type, config=d)
