import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_shape_and_channels_preserved():
    from ripple.models.ssl.augmentations import D4Augment
    aug = D4Augment(seed=0)
    x = torch.randn(3, 64, 64)
    out = aug(x)
    assert out.shape == (3, 64, 64)


def test_is_pixel_permutation_not_rescale():
    # D4 transforms permute pixels; the multiset of values is preserved
    from ripple.models.ssl.augmentations import D4Augment
    aug = D4Augment(noise_std=0.0, seed=1)
    x = torch.arange(3 * 64 * 64, dtype=torch.float32).reshape(3, 64, 64)
    out = aug(x)
    assert torch.allclose(torch.sort(out.flatten())[0], torch.sort(x.flatten())[0])


def test_seed_deterministic():
    from ripple.models.ssl.augmentations import D4Augment
    x = torch.randn(3, 64, 64)
    a = D4Augment(seed=7)(x.clone())
    b = D4Augment(seed=7)(x.clone())
    assert torch.equal(a, b)
