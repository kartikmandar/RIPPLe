import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_sr_model_predict_returns_image():
    from ripple.models.model_factory import ModelFactory
    m = ModelFactory.create("mae_vit_sr", {"encoder": "mae_vit_tiny",
                                           "task": "super_res"})
    assert getattr(m, "OUTPUT_KIND", None) == "image"
    out = m.predict(torch.randn(2, 3, 64, 64))
    assert out["output_image"].shape == (2, 3, 64, 64)
    assert out["scale"] == 4


def test_sr_model_accepts_encoder_weights(tmp_path):
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.checkpoint import save_encoder_checkpoint
    from ripple.models.model_factory import ModelFactory
    src = MAE(MaskedViTEncoder())
    p = tmp_path / "enc.pt"
    save_encoder_checkpoint(str(p), src)
    m = ModelFactory.create("mae_vit_sr", {"encoder": "mae_vit_tiny",
                                           "task": "super_res",
                                           "encoder_weights_path": str(p)})
    m.eval()
    x = torch.randn(1, 3, 64, 64)
    with torch.no_grad():
        got = m._net.encoder(x)
        want = src.encoder.eval()(x)
    assert torch.allclose(got, want, atol=1e-5)
