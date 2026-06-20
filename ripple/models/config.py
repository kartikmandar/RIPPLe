"""Typed configuration for the RIPPLe models module.

Frozen dataclasses mirroring ``ripple/preprocessing/config.py``: ``from_dict``
ignores unknown keys and coerces list -> tuple for tuple fields. No pydantic.
``torch`` is imported lazily inside ``resolve_device`` so importing this module
(and ``ripple.models``) succeeds with torch absent.
"""
from dataclasses import dataclass, fields
from typing import Optional, Tuple

from ripple.models.exceptions import ModelConfigError

_TASKS = {"binary", "multiclass", "super_res"}
_ENCODERS = {"resnet18", "resnet34", "vit_b_16", "vit_small", "mae_vit_tiny"}
_OPTIMIZERS = {"adamw", "adam", "sgd"}
_IMBALANCE = {"pos_weight", "weighted_sampler", "none"}


def _resolve_device(device: Optional[str]) -> str:
    """Resolve ``'auto'``/``None`` to cuda > mps > cpu; pass other values through."""
    if device and device != "auto":
        return device
    try:
        import torch
    except ImportError:
        return "cpu"
    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) is not None and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


@dataclass(frozen=True)
class ModelConfig:
    """Immutable model settings. See the Phase-3 design spec.

    Attributes:
        model_type: Registry key, e.g. ``'resnet_binary'``.
        task: ``'binary'`` (one logit + sigmoid) | ``'multiclass'`` | ``'super_res'``.
        encoder: Backbone, one of ``{resnet18, resnet34, vit_b_16, vit_small, mae_vit_tiny}``.
        in_channels: Input channel count (3 for g,r,i).
        num_classes: Class count for multiclass; binary still emits one logit.
        input_size: Square input side length in pixels.
        class_names: Ordered human-readable class labels.
        weights_path: Optional path to a checkpoint/state-dict to load.
        device: ``'auto'`` resolves to cuda > mps > cpu; else passed through.
        dropout: Classifier-head dropout.
        pretrained: Use ImageNet-pretrained backbone weights.
        apply_imagenet_norm: Apply ImageNet RGB normalization (default off).
        vit_variant: ``'b16_finetune'`` | ``'small_scratch'``.
        encoder_weights_path: Optional path to encoder-specific weights.
        patch_size: Patch size for vision transformers (default 4).
    """
    model_type: str = "resnet_binary"
    task: str = "binary"
    encoder: str = "resnet18"
    in_channels: int = 3
    num_classes: int = 2
    input_size: int = 64
    class_names: Tuple[str, ...] = ("non_lens", "lens")
    weights_path: Optional[str] = None
    device: str = "auto"
    dropout: float = 0.0
    pretrained: bool = False
    apply_imagenet_norm: bool = False
    vit_variant: str = "b16_finetune"
    encoder_weights_path: Optional[str] = None
    patch_size: int = 4

    def __post_init__(self):
        if self.task not in _TASKS:
            raise ModelConfigError(f"task must be one of {_TASKS}, got {self.task!r}")
        if self.encoder not in _ENCODERS:
            raise ModelConfigError(f"encoder must be one of {_ENCODERS}, got {self.encoder!r}")

    @property
    def head_logits(self) -> int:
        """Number of output logits: 1 for binary (sigmoid), else ``num_classes``."""
        return 1 if self.task == "binary" else self.num_classes

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "ModelConfig":
        """Build from a dict, ignoring unknown keys.

        Aliases ``model_path -> weights_path`` and ``architecture -> encoder``
        (an explicit canonical key wins over its alias); coerces a ``class_names``
        list to a tuple.
        """
        if d is None:
            return cls()
        known = {f.name for f in fields(cls)}
        kwargs = {}
        if "model_path" in d and "weights_path" not in d:
            kwargs["weights_path"] = d["model_path"]
        if "architecture" in d and "encoder" not in d:
            kwargs["encoder"] = d["architecture"]
        for k, v in d.items():
            if k not in known:
                continue
            if k == "class_names" and isinstance(v, list):
                v = tuple(v)
            kwargs[k] = v
        return cls(**kwargs)

    def resolve_device(self) -> str:
        """Resolve ``device`` to a concrete torch device string (torch lazy)."""
        return _resolve_device(self.device)


@dataclass(frozen=True)
class TrainerConfig:
    """Immutable training settings. See the Phase-3 design spec §9."""
    task: str = "binary"
    num_classes: int = 2
    epochs: int = 30
    batch_size: int = 64
    lr: float = 3e-4
    weight_decay: float = 1e-4
    optimizer: str = "adamw"
    imbalance: str = "pos_weight"
    early_stopping: bool = True
    patience: int = 5
    monitor: str = "val_auc"
    min_delta: float = 0.0
    grad_clip_norm: Optional[float] = None
    amp: bool = False
    seed: int = 0
    device: Optional[str] = None
    num_workers: int = 0
    log_every: int = 0

    def __post_init__(self):
        if self.task not in _TASKS:
            raise ModelConfigError(f"task must be one of {_TASKS}, got {self.task!r}")
        if self.optimizer not in _OPTIMIZERS:
            raise ModelConfigError(f"optimizer must be one of {_OPTIMIZERS}, got {self.optimizer!r}")
        if self.imbalance not in _IMBALANCE:
            raise ModelConfigError(f"imbalance must be one of {_IMBALANCE}, got {self.imbalance!r}")

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "TrainerConfig":
        """Build from a dict, ignoring unknown keys."""
        if d is None:
            return cls()
        known = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in d.items() if k in known}
        return cls(**kwargs)

    def resolve_device(self) -> str:
        """Resolve ``device`` to a concrete torch device string (torch lazy)."""
        return _resolve_device(self.device)
