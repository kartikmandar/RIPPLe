"""Masked Autoencoder over a MaskedViTEncoder (pure torch)."""
from __future__ import annotations

import torch
import torch.nn as nn


def patchify(imgs: torch.Tensor, patch_size: int, in_channels: int) -> torch.Tensor:
    p, c = patch_size, in_channels
    n, _, h, w = imgs.shape
    gh, gw = h // p, w // p
    x = imgs.reshape(n, c, gh, p, gw, p)
    x = torch.einsum("nchpwq->nhwpqc", x)
    return x.reshape(n, gh * gw, p * p * c)


def unpatchify(x: torch.Tensor, patch_size: int, in_channels: int, grid: int) -> torch.Tensor:
    p, c, g = patch_size, in_channels, grid
    n = x.shape[0]
    x = x.reshape(n, g, g, p, p, c)
    x = torch.einsum("nhwpqc->nchpwq", x)
    return x.reshape(n, c, g * p, g * p)


def random_masking(x: torch.Tensor, mask_ratio: float, generator=None):
    n, l, d = x.shape
    len_keep = int(l * (1 - mask_ratio))
    noise = torch.rand(n, l, device=x.device, generator=generator)
    ids_shuffle = torch.argsort(noise, dim=1)
    ids_restore = torch.argsort(ids_shuffle, dim=1)
    ids_keep = ids_shuffle[:, :len_keep]
    x_masked = torch.gather(x, 1, ids_keep.unsqueeze(-1).repeat(1, 1, d))
    mask = torch.ones(n, l, device=x.device)
    mask[:, :len_keep] = 0
    mask = torch.gather(mask, 1, ids_restore)
    return x_masked, mask, ids_restore


class MAE(nn.Module):
    def __init__(self, encoder, decoder_dim: int = 128, decoder_depth: int = 2,
                 num_heads: int = 4, norm_pix_loss: bool = True):
        super().__init__()
        self.encoder = encoder
        self.norm_pix_loss = norm_pix_loss
        h = encoder.feature_dim
        patch_dim = encoder.patch_size * encoder.patch_size * encoder.in_channels
        self.decoder_embed = nn.Linear(h, decoder_dim)
        self.mask_token = nn.Parameter(torch.zeros(1, 1, decoder_dim))
        self.decoder_pos_embed = nn.Parameter(
            torch.zeros(1, encoder.num_patches + 1, decoder_dim))
        layer = nn.TransformerEncoderLayer(
            d_model=decoder_dim, nhead=num_heads,
            dim_feedforward=decoder_dim * 4, activation="gelu",
            batch_first=True, norm_first=True)
        self.decoder_blocks = nn.TransformerEncoder(layer, num_layers=decoder_depth,
                                                    enable_nested_tensor=False)
        self.decoder_norm = nn.LayerNorm(decoder_dim)
        self.decoder_pred = nn.Linear(decoder_dim, patch_dim)
        nn.init.trunc_normal_(self.mask_token, std=0.02)
        nn.init.trunc_normal_(self.decoder_pos_embed, std=0.02)

    def encoder_state_dict(self) -> dict:
        return self.encoder.state_dict()

    def forward_encoder(self, imgs, mask_ratio):
        e = self.encoder
        x = e._embed(imgs) + e.pos_embed[:, 1:, :]
        x, mask, ids_restore = random_masking(x, mask_ratio)
        cls = (e.cls_token + e.pos_embed[:, :1, :]).expand(x.shape[0], -1, -1)
        x = torch.cat([cls, x], dim=1)
        x = e.norm(e.blocks(x))
        return x, mask, ids_restore

    def forward_decoder(self, x, ids_restore):
        x = self.decoder_embed(x)
        n, _, d = x.shape
        n_mask = ids_restore.shape[1] + 1 - x.shape[1]
        mask_tokens = self.mask_token.repeat(n, n_mask, 1)
        x_ = torch.cat([x[:, 1:, :], mask_tokens], dim=1)
        x_ = torch.gather(x_, 1, ids_restore.unsqueeze(-1).repeat(1, 1, d))
        x = torch.cat([x[:, :1, :], x_], dim=1)
        x = x + self.decoder_pos_embed
        x = self.decoder_norm(self.decoder_blocks(x))
        return self.decoder_pred(x)[:, 1:, :]

    def forward_loss(self, imgs, pred, mask):
        e = self.encoder
        target = patchify(imgs, e.patch_size, e.in_channels)
        if self.norm_pix_loss:
            mean = target.mean(dim=-1, keepdim=True)
            var = target.var(dim=-1, keepdim=True)
            target = (target - mean) / torch.sqrt(var + 1e-6)
        loss = ((pred - target) ** 2).mean(dim=-1)
        return (loss * mask).sum() / mask.sum()

    def forward(self, imgs, mask_ratio: float = 0.75):
        latent, mask, ids_restore = self.forward_encoder(imgs, mask_ratio)
        pred = self.forward_decoder(latent, ids_restore)
        loss = self.forward_loss(imgs, pred, mask)
        return loss, pred, mask
