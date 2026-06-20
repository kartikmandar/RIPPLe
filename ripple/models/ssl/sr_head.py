"""PixelShuffle x4 super-resolution head on the encoder token grid."""
from __future__ import annotations

import torch
import torch.nn as nn


class SRHead(nn.Module):
    def __init__(self, hidden_dim: int, grid: int, out_channels: int = 3, mid: int = 64):
        super().__init__()
        self.grid = grid
        self.hidden_dim = hidden_dim
        self.proj = nn.Conv2d(hidden_dim, mid * 4, kernel_size=3, padding=1)
        self.ps1 = nn.PixelShuffle(2)
        self.conv1 = nn.Conv2d(mid, mid * 4, kernel_size=3, padding=1)
        self.ps2 = nn.PixelShuffle(2)
        self.act = nn.GELU()
        self.conv_out = nn.Conv2d(mid, out_channels, kernel_size=3, padding=1)

    def forward(self, tokens: torch.Tensor) -> torch.Tensor:
        n, l, d = tokens.shape
        x = tokens.transpose(1, 2).reshape(n, d, self.grid, self.grid)
        x = self.act(self.ps1(self.proj(x)))     # grid -> 2*grid
        x = self.act(self.ps2(self.conv1(x)))    # -> 4*grid
        return self.conv_out(x)
