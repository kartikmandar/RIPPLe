import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def _mae():
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    return MAE(MaskedViTEncoder())


def test_save_then_load_roundtrip(tmp_path):
    from ripple.models.ssl.checkpoint import (
        save_encoder_checkpoint, load_encoder_state)
    m = _mae()
    p = tmp_path / "enc.pt"
    save_encoder_checkpoint(str(p), m, epoch=3,
                            norm_mean=[0.0, 0.0, 0.0], norm_std=[1.0, 1.0, 1.0])
    state = load_encoder_state(str(p))
    assert state["format_version"] == 1
    assert state["arch"] == "mae_vit_tiny"
    assert state["feature_dim"] == 192 and state["patch_size"] == 4
    assert state["epoch"] == 3
    # keys must match the encoder's own state_dict (zero remapping)
    assert set(state["encoder_state_dict"].keys()) == set(m.encoder.state_dict().keys())
