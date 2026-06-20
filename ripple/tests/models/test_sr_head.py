import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_upscales_token_grid_to_64():
    from ripple.models.ssl.sr_head import SRHead
    head = SRHead(hidden_dim=192, grid=16, out_channels=3)
    out = head(torch.randn(2, 256, 192))
    assert out.shape == (2, 3, 64, 64)
