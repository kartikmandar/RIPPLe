# ripple/tests/models/test_mae.py
import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def _mae():
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    return MAE(MaskedViTEncoder())


def test_patchify_roundtrip():
    from ripple.models.ssl.mae import patchify, unpatchify
    x = torch.randn(2, 3, 64, 64)
    p = patchify(x, 4, 3)
    assert p.shape == (2, 256, 4 * 4 * 3)
    back = unpatchify(p, 4, 3, 16)
    assert torch.allclose(back, x, atol=1e-5)


def test_random_masking_keeps_expected_count():
    from ripple.models.ssl.mae import random_masking
    x = torch.randn(2, 256, 192)
    xm, mask, ids = random_masking(x, 0.75)
    assert xm.shape == (2, 64, 192)          # 25% visible
    assert mask.shape == (2, 256)
    assert int(mask[0].sum().item()) == 192  # 75% masked


def test_forward_returns_loss_pred_mask():
    m = _mae()
    loss, pred, mask = m(torch.randn(2, 3, 64, 64), mask_ratio=0.75)
    assert loss.dim() == 0 and loss.item() >= 0.0
    assert pred.shape == (2, 256, 4 * 4 * 3)
    assert mask.shape == (2, 256)


def test_reconstruction_not_collapsed():
    # collapse sentinel: pred should vary across patch positions
    m = _mae().eval()
    with torch.no_grad():
        _, pred, _ = m(torch.randn(2, 3, 64, 64), mask_ratio=0.75)
    assert pred.std().item() > 1e-4
