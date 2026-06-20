"""PSNR / SSIM metrics for super-resolution (numpy/skimage, lazy)."""
from __future__ import annotations


def psnr(pred, target, max_val=None) -> float:
    import torch
    mse = torch.mean((pred - target) ** 2).item()
    if mse == 0:
        return float("inf")
    if max_val is None:
        max_val = float(target.max().item() - target.min().item()) or 1.0
    import math
    return 10.0 * math.log10((max_val ** 2) / mse)


def ssim(pred, target) -> float:
    from skimage.metrics import structural_similarity as sk_ssim
    import numpy as np
    p = pred.detach().cpu().numpy()
    t = target.detach().cpu().numpy()
    vals = []
    for i in range(p.shape[0]):
        a = np.transpose(p[i], (1, 2, 0))
        b = np.transpose(t[i], (1, 2, 0))
        rng = float(b.max() - b.min()) or 1.0
        vals.append(sk_ssim(a, b, channel_axis=2, data_range=rng))
    return float(np.mean(vals))
