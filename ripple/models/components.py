"""Encoder/head architecture components for RIPPLe classifiers.

Torch + torchvision only (no timm/e2cnn). This module is imported solely by
``ripple.models.builders`` (and lazily by ``BaseModel._build``), so torch and
torchvision are imported at module top — they will never be triggered at package
import time through ``ripple.models.__init__``.
"""
from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision


# ---------------------------------------------------------------------------
# Encoder base
# ---------------------------------------------------------------------------

class Encoder(nn.Module):
    """Base class for encoder modules.

    Concrete encoders subclass this, build a backbone in ``__init__``, set an
    integer ``.feature_dim``, and implement ``forward(x) -> (N, feature_dim)``.
    """
    feature_dim: int


# ---------------------------------------------------------------------------
# ClassifierHead
# ---------------------------------------------------------------------------

class ClassifierHead(nn.Module):
    """Linear head with optional dropout.

    Args:
        feature_dim: Input feature size from the encoder.
        num_logits: Output logit count (1 for binary, ``num_classes`` otherwise).
        dropout: Dropout probability applied before the linear layer (0 = off).
    """

    def __init__(self, feature_dim: int, num_logits: int, dropout: float = 0.0):
        super().__init__()
        self.dropout = nn.Dropout(dropout) if dropout and dropout > 0.0 else nn.Identity()
        self.fc = nn.Linear(feature_dim, num_logits)

    def forward(self, feat: torch.Tensor) -> torch.Tensor:
        return self.fc(self.dropout(feat))


# ---------------------------------------------------------------------------
# EncoderHeadNet
# ---------------------------------------------------------------------------

class EncoderHeadNet(nn.Module):
    """Complete classifier: encoder trunk + classifier head.

    Exposes ``.encoder``, ``.head``, ``forward(x)->(N, num_logits)`` and
    ``freeze_encoder()`` which sets all encoder parameters to
    ``requires_grad=False``.
    """

    def __init__(self, encoder: nn.Module, head: nn.Module):
        super().__init__()
        self.encoder = encoder
        self.head = head

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(self.encoder(x))

    def freeze_encoder(self) -> None:
        """Freeze all encoder parameters (for fine-tuning head only)."""
        for p in self.encoder.parameters():
            p.requires_grad = False


# ---------------------------------------------------------------------------
# ResNetEncoder
# ---------------------------------------------------------------------------

class ResNetEncoder(Encoder):
    """torchvision ResNet trunk with ``fc`` replaced by Identity.

    Exposes ``.backbone``, ``.feature_dim`` and ``forward(x) -> (N, feature_dim)``.
    ``conv1`` surgery is applied only when ``in_channels != 3``; the stock
    3-channel backbone is used verbatim otherwise.
    """

    def __init__(self, arch: str = "resnet18", pretrained: bool = False,
                 in_channels: int = 3):
        super().__init__()
        weights = "DEFAULT" if pretrained else None
        ctor = {"resnet18": torchvision.models.resnet18,
                "resnet34": torchvision.models.resnet34}[arch]
        backbone = ctor(weights=weights)
        self.feature_dim = backbone.fc.in_features
        backbone.fc = nn.Identity()
        # conv1 surgery ONLY when not the native 3-channel path.
        if in_channels != 3:
            old = backbone.conv1
            backbone.conv1 = nn.Conv2d(
                in_channels, old.out_channels,
                kernel_size=old.kernel_size, stride=old.stride,
                padding=old.padding, bias=(old.bias is not None),
            )
        self.backbone = backbone

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.backbone(x)


# ---------------------------------------------------------------------------
# ViTEncoder
# ---------------------------------------------------------------------------

class ViTEncoder(Encoder):
    """torchvision ViT trunk.

    ``b16_finetune`` self-resizes inputs to 224 in ``forward``; ``small_scratch``
    runs natively at 64. Exposes ``.backbone``, ``.feature_dim`` and
    ``forward(x) -> (N, feature_dim)``. ``conv_proj`` surgery is applied only
    when ``in_channels != 3``.
    """

    def __init__(self, variant: str = "b16_finetune", pretrained: bool = False,
                 in_channels: int = 3, input_size: int = 64):
        super().__init__()
        if variant == "b16_finetune":
            weights = "DEFAULT" if pretrained else None
            backbone = torchvision.models.vit_b_16(weights=weights)
            self.feature_dim = backbone.heads.head.in_features
            backbone.heads = nn.Identity()
            self.native_size = 224
            if in_channels != 3:
                old = backbone.conv_proj
                backbone.conv_proj = nn.Conv2d(
                    in_channels, old.out_channels,
                    kernel_size=old.kernel_size, stride=old.stride,
                    padding=old.padding,
                )
        elif variant == "small_scratch":
            backbone = torchvision.models.VisionTransformer(
                image_size=64, patch_size=8, num_layers=6, num_heads=6,
                hidden_dim=384, mlp_dim=1536,
            )
            self.feature_dim = backbone.heads.head.in_features
            backbone.heads = nn.Identity()
            self.native_size = 64
            if in_channels != 3:
                old = backbone.conv_proj
                backbone.conv_proj = nn.Conv2d(
                    in_channels, old.out_channels,
                    kernel_size=old.kernel_size, stride=old.stride,
                    padding=old.padding,
                )
        else:
            raise ValueError(f"unknown vit variant {variant!r}")
        self.backbone = backbone

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.shape[-1] != self.native_size or x.shape[-2] != self.native_size:
            x = F.interpolate(
                x, size=(self.native_size, self.native_size),
                mode="bilinear", align_corners=False,
            )
        return self.backbone(x)


# ---------------------------------------------------------------------------
# MaskedViTEncoder
# ---------------------------------------------------------------------------

class MaskedViTEncoder(Encoder):
    """Pure-torch ViT-Tiny usable as a RIPPLe encoder AND an MAE backbone.

    ``forward`` runs all tokens and returns the cls vector (classification);
    ``forward_tokens`` returns the patch-token grid (super-resolution); the
    MAE model reuses ``_embed``/``cls_token``/``pos_embed``/``blocks``/``norm``
    directly for visible-token-only encoding.
    """

    def __init__(self, hidden_dim: int = 192, depth: int = 6, num_heads: int = 3,
                 patch_size: int = 4, in_channels: int = 3, input_size: int = 64,
                 mlp_ratio: float = 4.0):
        super().__init__()
        self.feature_dim = hidden_dim
        self.patch_size = patch_size
        self.in_channels = in_channels
        self.input_size = input_size
        self.grid = input_size // patch_size
        self.num_patches = self.grid * self.grid
        self.patch_embed = nn.Conv2d(in_channels, hidden_dim,
                                     kernel_size=patch_size, stride=patch_size)
        self.cls_token = nn.Parameter(torch.zeros(1, 1, hidden_dim))
        self.pos_embed = nn.Parameter(torch.zeros(1, self.num_patches + 1, hidden_dim))
        layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim, nhead=num_heads,
            dim_feedforward=int(hidden_dim * mlp_ratio),
            activation="gelu", batch_first=True, norm_first=True,
        )
        self.blocks = nn.TransformerEncoder(layer, num_layers=depth)
        self.norm = nn.LayerNorm(hidden_dim)
        nn.init.trunc_normal_(self.pos_embed, std=0.02)
        nn.init.trunc_normal_(self.cls_token, std=0.02)

    def _embed(self, x: torch.Tensor) -> torch.Tensor:
        x = self.patch_embed(x)                 # (N, hidden, grid, grid)
        return x.flatten(2).transpose(1, 2)     # (N, num_patches, hidden)

    def _encode_all(self, x: torch.Tensor) -> torch.Tensor:
        x = self._embed(x) + self.pos_embed[:, 1:, :]
        cls = (self.cls_token + self.pos_embed[:, :1, :]).expand(x.shape[0], -1, -1)
        x = torch.cat([cls, x], dim=1)
        return self.norm(self.blocks(x))        # (N, num_patches+1, hidden)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self._encode_all(x)[:, 0]        # (N, hidden) cls token

    def forward_tokens(self, x: torch.Tensor) -> torch.Tensor:
        return self._encode_all(x)[:, 1:, :]    # (N, num_patches, hidden)


# ---------------------------------------------------------------------------
# build_net
# ---------------------------------------------------------------------------

def build_net(config) -> EncoderHeadNet:
    """Build the trainable ``EncoderHeadNet`` from a ``ModelConfig``.

    Encoder is chosen from ``config.encoder``:
    - ``resnet18`` / ``resnet34`` → ResNetEncoder
    - ``vit_b_16``               → ViTEncoder(variant="b16_finetune")
    - ``vit_small``              → ViTEncoder(variant=config.vit_variant)

    Head ``num_logits`` is 1 for the binary task (single-logit + sigmoid
    convention) else ``config.num_classes``.
    """
    encoder_name = config.encoder
    if encoder_name in ("resnet18", "resnet34"):
        encoder = ResNetEncoder(
            arch=encoder_name,
            pretrained=config.pretrained,
            in_channels=config.in_channels,
        )
    elif encoder_name == "vit_b_16":
        encoder = ViTEncoder(
            variant="b16_finetune",
            pretrained=config.pretrained,
            in_channels=config.in_channels,
            input_size=config.input_size,
        )
    elif encoder_name == "vit_small":
        encoder = ViTEncoder(
            variant=config.vit_variant,
            pretrained=config.pretrained,
            in_channels=config.in_channels,
            input_size=config.input_size,
        )
    else:
        raise ValueError(f"unknown encoder {encoder_name!r}")

    num_logits = 1 if config.task == "binary" else config.num_classes
    head = ClassifierHead(encoder.feature_dim, num_logits, dropout=config.dropout)
    return EncoderHeadNet(encoder, head)
