"""Classifier builders for the canonical registry keys.

Each builder takes a ``ModelConfig`` and returns a *configured* ``BaseModel``
(not a bare ``nn.Module``); ``BaseModel._build`` lazily calls
``components.build_net``. Importing this module is the side effect that
populates the registry with ``resnet_binary``/``vit_binary``/
``resnet_multiclass``/``vit_multiclass``.

Torch is NOT imported here: registration and ``BaseModel`` construction are
torch-free; torch is pulled only when the model's network is built/run.

The optional import of ``vendored_sr_adapter`` at the bottom registers the
SR builders (``anirudh_sr`` / ``anirudh_sr_rcan``) when that module exists
(Task 21). It is wrapped in try/except so a missing adapter never prevents the
four classifier keys from registering.
"""
from __future__ import annotations

from dataclasses import asdict

from .base_model import BaseModel
from .config import ModelConfig
from .model_registry import register


def _with(config: ModelConfig, **overrides) -> ModelConfig:
    """Return a new frozen ModelConfig with ``overrides`` applied.

    ``ModelConfig`` is frozen, so builders derive a fresh config rather than
    mutating. Unknown keys are ignored by ``from_dict`` and list fields are
    coerced to tuple, matching the canonical config contract.
    """
    merged = asdict(config)
    merged.update(overrides)
    return ModelConfig.from_dict(merged)


@register("resnet_binary")
def build_resnet_binary(config: ModelConfig) -> BaseModel:
    """Binary classifier with a ResNet-18 encoder (one output logit)."""
    config = _with(
        config,
        model_type="resnet_binary",
        task="binary",
        encoder="resnet18",
        num_classes=2,
        class_names=("non_lens", "lens"),
    )
    return BaseModel(config)


@register("vit_binary")
def build_vit_binary(config: ModelConfig) -> BaseModel:
    """Binary classifier with a ViT-Small encoder (one output logit)."""
    config = _with(
        config,
        model_type="vit_binary",
        task="binary",
        encoder="vit_small",
        num_classes=2,
        class_names=("non_lens", "lens"),
    )
    return BaseModel(config)


@register("resnet_multiclass")
def build_resnet_multiclass(config: ModelConfig) -> BaseModel:
    """3-class classifier with a ResNet-18 encoder (no_sub / cdm / axion)."""
    config = _with(
        config,
        model_type="resnet_multiclass",
        task="multiclass",
        encoder="resnet18",
        num_classes=3,
        class_names=("no_sub", "cdm", "axion"),
    )
    return BaseModel(config)


@register("vit_multiclass")
def build_vit_multiclass(config: ModelConfig) -> BaseModel:
    """3-class classifier with a ViT-Small encoder (no_sub / cdm / axion)."""
    config = _with(
        config,
        model_type="vit_multiclass",
        task="multiclass",
        encoder="vit_small",
        num_classes=3,
        class_names=("no_sub", "cdm", "axion"),
    )
    return BaseModel(config)


@register("mae_vit_binary")
def build_mae_vit_binary(config: ModelConfig) -> BaseModel:
    """Binary classifier on the MAE ViT-Tiny encoder (one output logit)."""
    config = _with(
        config,
        model_type="mae_vit_binary",
        task="binary",
        encoder="mae_vit_tiny",
        num_classes=2,
        class_names=("non_lens", "lens"),
    )
    return BaseModel(config)


@register("mae_vit_multiclass")
def build_mae_vit_multiclass(config: ModelConfig) -> BaseModel:
    """3-class substructure classifier on the MAE ViT-Tiny encoder."""
    config = _with(
        config,
        model_type="mae_vit_multiclass",
        task="multiclass",
        encoder="mae_vit_tiny",
        num_classes=config.num_classes if config.num_classes >= 2 else 3,
        class_names=config.class_names if len(config.class_names) >= 2
        else ("no_sub", "cdm", "axion"),
    )
    return BaseModel(config)


@register("mae_vit_sr")
def build_mae_vit_sr(config: ModelConfig):
    """Super-resolution head on the MAE ViT-Tiny encoder (image-out)."""
    from .ssl.sr_model import MAEViTSR
    config = _with(
        config,
        model_type="mae_vit_sr",
        task="super_res",
        encoder="mae_vit_tiny",
    )
    return MAEViTSR(config)


# Side-effect import: registers anirudh_sr / anirudh_sr_rcan when the SR
# adapter (Task 21) is present. Wrapped defensively so the four classifier
# builders above always register even if the adapter is not yet implemented.
try:
    from . import vendored_sr_adapter  # noqa: F401  registers anirudh_sr / anirudh_sr_rcan when present
except ImportError:
    pass
