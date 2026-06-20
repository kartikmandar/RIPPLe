import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_degrade_preserves_shape():
    from ripple.models.ssl.sr_data import degrade
    hr = torch.randn(2, 3, 64, 64)
    lr = degrade(hr, factor=4)
    assert lr.shape == (2, 3, 64, 64)
    assert not torch.allclose(lr, hr)   # information was lost


def test_psnr_identical_is_high():
    from ripple.models.ssl.sr_metrics import psnr
    x = torch.rand(1, 3, 16, 16)
    assert psnr(x, x.clone()) > 60.0


def test_ssim_identical_is_one():
    pytest.importorskip("skimage")
    from ripple.models.ssl.sr_metrics import ssim
    x = torch.rand(1, 3, 32, 32)
    assert ssim(x, x.clone()) == pytest.approx(1.0, abs=1e-4)
