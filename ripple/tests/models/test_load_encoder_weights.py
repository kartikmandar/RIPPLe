import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def _save_encoder(tmp_path):
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.checkpoint import save_encoder_checkpoint
    m = MAE(MaskedViTEncoder())
    p = tmp_path / "enc.pt"
    save_encoder_checkpoint(str(p), m)
    return str(p), m.encoder


def test_encoder_weights_load_exact_and_identical(tmp_path):
    from ripple.models.base_model import BaseModel
    from ripple.models.config import ModelConfig
    path, src_encoder = _save_encoder(tmp_path)
    model = BaseModel(ModelConfig(encoder="mae_vit_tiny", task="binary"))
    model.load_encoder_weights(path)            # builds net + loads encoder
    model.eval()
    x = torch.randn(2, 3, 64, 64)
    with torch.no_grad():
        got = model._net.encoder(x)
        want = src_encoder.eval()(x)
    assert torch.allclose(got, want, atol=1e-5)


def test_mismatched_arch_raises(tmp_path):
    from ripple.models.base_model import BaseModel
    from ripple.models.config import ModelConfig
    from ripple.models.exceptions import ModelLoadError
    path, _ = _save_encoder(tmp_path)
    # resnet encoder cannot accept ViT-Tiny keys
    model = BaseModel(ModelConfig(encoder="resnet18", task="binary"))
    with pytest.raises(ModelLoadError):
        model.load_encoder_weights(path)
