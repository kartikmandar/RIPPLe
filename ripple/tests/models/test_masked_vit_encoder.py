# ripple/tests/models/test_masked_vit_encoder.py
import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def _enc():
    from ripple.models.components import MaskedViTEncoder
    return MaskedViTEncoder()


def test_feature_dim_and_geometry():
    e = _enc()
    assert e.feature_dim == 192 and e.patch_size == 4
    assert e.grid == 16 and e.num_patches == 256


def test_forward_returns_cls_vector():
    e = _enc().eval()
    out = e(torch.randn(2, 3, 64, 64))
    assert out.shape == (2, 192)


def test_forward_tokens_returns_patch_grid():
    e = _enc().eval()
    out = e.forward_tokens(torch.randn(2, 3, 64, 64))
    assert out.shape == (2, 256, 192)
