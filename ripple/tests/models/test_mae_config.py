import dataclasses
import pytest
from ripple.models.config import MAEConfig


def test_defaults():
    c = MAEConfig()
    assert c.patch_size == 4 and c.mask_ratio == 0.75
    assert c.decoder_dim == 128 and c.decoder_depth == 2
    assert c.norm_pix_loss is True and c.lr == 1e-4 and c.weight_decay == 0.0


def test_frozen():
    c = MAEConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.lr = 1.0


def test_from_dict_ignores_unknown():
    c = MAEConfig.from_dict({"mask_ratio": 0.9, "bogus": 1})
    assert c.mask_ratio == 0.9 and c.patch_size == 4
