"""Encoder-only checkpoint I/O for SSL pretraining."""
from __future__ import annotations

import datetime

ENCODER_CKPT_VERSION = 1


def save_encoder_checkpoint(path, mae, *, arch="mae_vit_tiny", epoch=0,
                            norm_mean=None, norm_std=None,
                            image_bands=("g", "r", "i")):
    import torch
    enc = mae.encoder
    sd = {k: v.detach().cpu() for k, v in mae.encoder_state_dict().items()}
    ckpt = {
        "format_version": ENCODER_CKPT_VERSION,
        "encoder_state_dict": sd,
        "ssl_method": "mae",
        "arch": arch,
        "in_channels": enc.in_channels,
        "feature_dim": enc.feature_dim,
        "patch_size": enc.patch_size,
        "input_size": enc.input_size,
        "image_bands": list(image_bands),
        "norm_mean": list(norm_mean) if norm_mean is not None else None,
        "norm_std": list(norm_std) if norm_std is not None else None,
        "epoch": int(epoch),
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "torch_version": str(torch.__version__),
    }
    torch.save(ckpt, path)


def load_encoder_state(path) -> dict:
    import torch
    return torch.load(path, map_location="cpu", weights_only=True)
