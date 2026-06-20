"""Self-supervised degrade-and-restore pairing for super-resolution."""
from __future__ import annotations


def degrade(hr, factor: int = 4):
    import torch.nn.functional as F
    n = hr.shape[-1]
    lr = F.interpolate(hr, scale_factor=1.0 / factor, mode="bicubic",
                       align_corners=False)
    return F.interpolate(lr, size=(n, n), mode="bicubic", align_corners=False)
