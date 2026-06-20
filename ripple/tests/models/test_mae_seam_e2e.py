# ripple/tests/models/test_mae_seam_e2e.py
import subprocess
import sys
import pytest


def test_light_import_lists_mae_keys_without_torch():
    # import ripple.models must succeed and list mae keys with torch import blocked
    code = (
        "import builtins, sys\n"
        "real = builtins.__import__\n"
        "def fake(name, *a, **k):\n"
        "    if name == 'torch' or name.startswith('torch.'):\n"
        "        raise ImportError('torch blocked')\n"
        "    return real(name, *a, **k)\n"
        "builtins.__import__ = fake\n"
        "import ripple.models as m\n"
        "ks = m.list_models()\n"
        "assert 'mae_vit_binary' in ks and 'mae_vit_sr' in ks, ks\n"
        "print('OK')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "OK" in out.stdout


def test_pretrain_save_load_finetune_roundtrip(tmp_path):
    pytest.importorskip("torch")
    import torch
    from torch.utils.data import TensorDataset, DataLoader
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.mae_trainer import MAETrainer
    from ripple.models.config import MAEConfig
    from ripple.models.model_factory import ModelFactory

    # tiny pretrain
    x = torch.randn(8, 3, 64, 64)
    loader = DataLoader(TensorDataset(x, torch.full((8,), -1)), batch_size=4)
    mae = MAE(MaskedViTEncoder())
    trainer = MAETrainer(MAEConfig(epochs=1, device="cpu"))
    trainer.fit(mae, loader)
    p = tmp_path / "enc.pt"
    trainer.save_encoder_checkpoint(str(p), mae, epoch=1)

    # load into a downstream classifier; encoder outputs must match the source
    clf = ModelFactory.create("mae_vit_binary",
                              {"encoder": "mae_vit_tiny", "task": "binary",
                               "encoder_weights_path": str(p)})
    clf.eval()
    with torch.no_grad():
        got = clf._net.encoder(x[:2])
        want = mae.encoder.eval()(x[:2])
    assert torch.allclose(got, want, atol=1e-5)
