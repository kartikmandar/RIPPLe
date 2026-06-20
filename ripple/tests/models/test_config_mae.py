import pytest
from ripple.models.config import ModelConfig, _ENCODERS


def test_mae_vit_tiny_is_a_valid_encoder():
    assert "mae_vit_tiny" in _ENCODERS
    cfg = ModelConfig(encoder="mae_vit_tiny", task="binary")
    assert cfg.encoder == "mae_vit_tiny"


def test_encoder_weights_path_and_patch_size_defaults_and_override():
    cfg = ModelConfig()
    assert cfg.encoder_weights_path is None
    assert cfg.patch_size == 4
    cfg2 = ModelConfig.from_dict(
        {"encoder": "mae_vit_tiny", "encoder_weights_path": "/tmp/enc.pt", "patch_size": 8}
    )
    assert cfg2.encoder_weights_path == "/tmp/enc.pt"
    assert cfg2.patch_size == 8


def test_existing_configs_unaffected():
    cfg = ModelConfig.from_dict({"model_type": "resnet_binary"})
    assert cfg.encoder == "resnet18" and cfg.patch_size == 4
