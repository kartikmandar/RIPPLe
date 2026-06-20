import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_factory_loads_encoder_weights(tmp_path):
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.checkpoint import save_encoder_checkpoint
    from ripple.models.model_factory import ModelFactory
    src = MAE(MaskedViTEncoder())
    p = tmp_path / "enc.pt"
    save_encoder_checkpoint(str(p), src)
    model = ModelFactory.create(
        "mae_vit_binary",
        {"encoder": "mae_vit_tiny", "task": "binary",
         "encoder_weights_path": str(p)},
    )
    model.eval()
    x = torch.randn(2, 3, 64, 64)
    with torch.no_grad():
        got = model._net.encoder(x)
        want = src.encoder.eval()(x)
    assert torch.allclose(got, want, atol=1e-5)
