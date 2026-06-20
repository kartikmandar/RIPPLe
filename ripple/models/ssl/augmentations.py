"""Geometry-only, physics-preserving augmentations for lensing SSL.

D4 group only (flips + k*90-degree rotations) — exact, no interpolation
artifacts. NO color jitter (would break g/r/i flux ratios), NO resized-crop
or zoom (would rescale the Einstein radius).
"""
from __future__ import annotations

import random


class D4Augment:
    def __init__(self, noise_std: float = 0.0, seed=None):
        self.noise_std = noise_std
        self._rng = random.Random(seed)

    def __call__(self, x):
        import torch
        k = self._rng.randint(0, 3)
        x = torch.rot90(x, k, dims=(-2, -1))
        if self._rng.random() < 0.5:
            x = torch.flip(x, dims=(-1,))
        if self._rng.random() < 0.5:
            x = torch.flip(x, dims=(-2,))
        if self.noise_std > 0.0:
            x = x + torch.randn_like(x) * self.noise_std
        return x
