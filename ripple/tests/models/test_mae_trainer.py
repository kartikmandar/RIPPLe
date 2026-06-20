import pytest
pytest.importorskip("torch")
import torch
from torch.utils.data import TensorDataset, DataLoader

pytestmark = pytest.mark.torch


def _loader(n=8):
    x = torch.randn(n, 3, 64, 64)
    y = torch.full((n,), -1, dtype=torch.long)
    return DataLoader(TensorDataset(x, y), batch_size=4)


def _mae():
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    return MAE(MaskedViTEncoder())


def test_fit_returns_history_and_decreases(monkeypatch):
    from ripple.models.config import MAEConfig
    from ripple.models.ssl.mae_trainer import MAETrainer
    torch.manual_seed(0)
    t = MAETrainer(MAEConfig(epochs=3, mask_ratio=0.5, lr=1e-3, device="cpu"))
    hist = t.fit(_mae(), _loader())
    assert len(hist) == 3 and "train_loss" in hist[0]
    assert hist[-1]["train_loss"] <= hist[0]["train_loss"]


def test_fit_seed_reproducible():
    from ripple.models.config import MAEConfig
    from ripple.models.ssl.mae_trainer import MAETrainer

    def run():
        torch.manual_seed(0)
        return MAETrainer(MAEConfig(epochs=2, device="cpu", seed=0)).fit(_mae(), _loader())

    a, b = run(), run()
    assert abs(a[-1]["train_loss"] - b[-1]["train_loss"]) < 1e-4


def test_save_encoder_checkpoint(tmp_path):
    from ripple.models.config import MAEConfig
    from ripple.models.ssl.mae_trainer import MAETrainer
    from ripple.models.ssl.checkpoint import load_encoder_state
    m = _mae()
    t = MAETrainer(MAEConfig(epochs=1, device="cpu"))
    t.fit(m, _loader())
    p = tmp_path / "enc.pt"
    t.save_encoder_checkpoint(str(p), m, epoch=1)
    state = load_encoder_state(str(p))
    assert state["arch"] == "mae_vit_tiny"
