"""Super-resolution model on the shared MAE ViT-Tiny encoder."""
from __future__ import annotations

from ripple.models.base_model import BaseModel


class MAEViTSR(BaseModel):
    task = "super_res"
    model_type = "mae_vit_sr"
    OUTPUT_KIND = "image"

    def _build(self):
        if self._net is not None:
            return
        import torch.nn as nn
        from ripple.models.components import MaskedViTEncoder
        from ripple.models.ssl.sr_head import SRHead

        enc = MaskedViTEncoder(
            patch_size=self.config.patch_size,
            in_channels=self.config.in_channels,
            input_size=self.config.input_size,
        )
        head = SRHead(enc.feature_dim, enc.grid, out_channels=self.config.in_channels)

        class _EncoderSRNet(nn.Module):
            def __init__(self, encoder, sr_head):
                super().__init__()
                self.encoder = encoder
                self.sr_head = sr_head

            def forward(self, x):
                tokens = self.encoder.forward_tokens(x).contiguous()
                return self.sr_head(tokens)

        self._net = _EncoderSRNet(enc, head)

    def predict(self, data, *, batch_size=32, device=None):
        import torch

        self._build()
        if not torch.is_tensor(data):
            data = torch.as_tensor(data, dtype=torch.float32)
        if data.dim() == 3:
            data = data.unsqueeze(0)
        dev = device or self._resolved_device
        self._net.to(dev)
        self._net.eval()
        outs = []
        with torch.no_grad():
            for start in range(0, data.shape[0], batch_size):
                chunk = data[start:start + batch_size].to(dev)
                outs.append(self._net(chunk).cpu())
        return {"output_image": torch.cat(outs, dim=0), "scale": 4}
