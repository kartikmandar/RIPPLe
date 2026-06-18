"""Offline, CPU-only tests for ripple.models.model_trainer."""
import dataclasses

from ripple.models.model_trainer import History


def test_history_fields_and_default():
    h = History(epochs=[], best_epoch=-1, best_metric=float("-inf"))
    assert h.epochs == []
    assert h.best_epoch == -1
    assert h.best_metric == float("-inf")
    assert h.best_state_dict is None
    names = [f.name for f in dataclasses.fields(History)]
    assert names == ["epochs", "best_epoch", "best_metric", "best_state_dict"]


def test_history_records_epochs():
    h = History(epochs=[{"epoch": 0, "train_loss": 1.0}], best_epoch=0, best_metric=0.5)
    assert h.epochs[0]["train_loss"] == 1.0
    assert h.best_metric == 0.5


import copy

import pytest


def _tiny_binary_net():
    import torch.nn as nn
    return nn.Sequential(nn.Flatten(), nn.Linear(3 * 8 * 8, 1))


def _tiny_loader(*, labels, batch_size=4, seed=0, n=16, channels=3, hw=8):
    import torch
    from torch.utils.data import DataLoader, TensorDataset
    g = torch.Generator().manual_seed(seed)
    x = torch.randn(n, channels, hw, hw, generator=g)
    if labels == "balanced":
        y = torch.tensor(([0, 1] * (n // 2))[:n], dtype=torch.long)
    elif labels == "imbalanced":  # 12 neg / 4 pos -> pos_weight == 3.0
        y = torch.tensor([0] * 12 + [1] * 4, dtype=torch.long)
    elif labels == "unlabelled":
        y = torch.full((n,), -1, dtype=torch.long)
    else:
        raise ValueError(labels)
    ds = TensorDataset(x, y)
    return DataLoader(ds, batch_size=batch_size, shuffle=False)


@pytest.mark.torch
def test_fit_loss_decreases():
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    torch.manual_seed(0)
    net = _tiny_binary_net()
    loader = _tiny_loader(labels="balanced", seed=1)
    cfg = TrainerConfig(task="binary", epochs=8, lr=0.1, early_stopping=False,
                        device="cpu", seed=0)
    hist = ModelTrainer(cfg).fit(net, loader)
    first = hist.epochs[0]["train_loss"]
    last = hist.epochs[-1]["train_loss"]
    assert last < first


@pytest.mark.torch
def test_fit_seed_reproducible():
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    cfg = TrainerConfig(task="binary", epochs=3, lr=0.05, early_stopping=False,
                        device="cpu", seed=7)

    torch.manual_seed(0)
    net_a = _tiny_binary_net()
    init_state = copy.deepcopy(net_a.state_dict())
    hist_a = ModelTrainer(cfg).fit(net_a, _tiny_loader(labels="balanced", seed=2))

    net_b = _tiny_binary_net()
    net_b.load_state_dict(init_state)
    hist_b = ModelTrainer(cfg).fit(net_b, _tiny_loader(labels="balanced", seed=2))

    losses_a = [e["train_loss"] for e in hist_a.epochs]
    losses_b = [e["train_loss"] for e in hist_b.epochs]
    assert losses_a == pytest.approx(losses_b)


@pytest.mark.torch
def test_fit_masks_unlabelled():
    pytest.importorskip("torch")
    import math

    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    net = _tiny_binary_net()
    cfg = TrainerConfig(task="binary", epochs=2, lr=0.05, early_stopping=False,
                        device="cpu", seed=0)
    hist = ModelTrainer(cfg).fit(net, _tiny_loader(labels="unlabelled", seed=3))
    for e in hist.epochs:
        assert math.isfinite(e["train_loss"])


@pytest.mark.torch
def test_fit_pos_weight_is_n_neg_over_n_pos():
    pytest.importorskip("torch")
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    trainer = ModelTrainer(TrainerConfig(task="binary", device="cpu"))
    pw = trainer._compute_pos_weight(_tiny_loader(labels="imbalanced", seed=4))
    assert float(pw) == pytest.approx(3.0)


@pytest.mark.torch
def test_fit_early_stop_triggers():
    pytest.importorskip("torch")
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    net = _tiny_binary_net()
    # monitor train_loss but force no-improvement by huge min_delta -> stop at patience.
    cfg = TrainerConfig(task="binary", epochs=20, lr=0.05, early_stopping=True,
                        patience=2, monitor="train_loss", min_delta=1e9,
                        device="cpu", seed=0)
    hist = ModelTrainer(cfg).fit(net, _tiny_loader(labels="balanced", seed=5))
    assert len(hist.epochs) < 20
    assert len(hist.epochs) <= cfg.patience + 1


@pytest.mark.torch
def test_fit_no_val_default_monitor_falls_back_to_train_loss_minimize():
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    # Default-ish config: monitor left at "val_auc", early_stopping ON, but no
    # val_loader. The fix must fall back to monitoring train_loss in MINIMIZE
    # direction; otherwise a strictly-decreasing train_loss would be treated as
    # "never improving" under the (wrong) maximize direction and early-stop.
    torch.manual_seed(0)
    net = _tiny_binary_net()
    cfg = TrainerConfig(task="binary", epochs=8, lr=0.1, early_stopping=True,
                        patience=2, min_delta=0.0, device="cpu", seed=0)
    assert cfg.monitor == "val_auc"  # default, a validation metric
    hist = ModelTrainer(cfg).fit(net, _tiny_loader(labels="balanced", seed=1))

    losses = [e["train_loss"] for e in hist.epochs]
    # train_loss strictly decreases on this tiny problem -> no premature stop.
    assert losses == sorted(losses, reverse=True)
    assert len(hist.epochs) == cfg.epochs
    # best_metric tracks the LOWEST train_loss (minimize direction).
    assert hist.best_metric == pytest.approx(min(losses))
    assert hist.best_epoch == len(losses) - 1


@pytest.mark.torch
def test_checkpoint_round_trip_identical_logits(tmp_path):
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.model_trainer import ModelTrainer

    torch.manual_seed(0)
    net = _tiny_binary_net()
    cfg = TrainerConfig(task="binary", epochs=2, lr=0.05, early_stopping=False,
                        device="cpu", seed=0)
    trainer = ModelTrainer(cfg)
    hist = trainer.fit(net, _tiny_loader(labels="balanced", seed=6))

    ckpt_path = tmp_path / "model.pt"
    trainer.save_checkpoint(str(ckpt_path), net,
                            metrics={"auc": 0.9}, history=hist)
    assert ckpt_path.exists()

    loaded = trainer.load_checkpoint(str(ckpt_path))
    assert loaded["format_version"] == 1
    assert loaded["task"] == "binary"
    assert loaded["input_size"] == cfg.__dict__.get("input_size", 64) or 64

    fresh = _tiny_binary_net()
    fresh.load_state_dict(loaded["state_dict"])

    x = torch.randn(5, 3, 8, 8, generator=torch.Generator().manual_seed(99))
    net.eval()
    fresh.eval()
    with torch.no_grad():
        a = net(x)
        b = fresh(x)
    assert torch.allclose(a, b, atol=1e-6)


@pytest.mark.torch
def test_load_checkpoint_wrong_input_size_raises(tmp_path):
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.exceptions import CheckpointError
    from ripple.models.model_trainer import ModelTrainer

    net = _tiny_binary_net()
    saver = ModelTrainer(TrainerConfig(task="binary", device="cpu"))
    ckpt_path = tmp_path / "wrong_size.pt"
    saver.save_checkpoint(str(ckpt_path), net)

    # Corrupt the persisted input_size, then load with the default-expecting trainer.
    raw = torch.load(str(ckpt_path), weights_only=False)
    raw["input_size"] = 999
    torch.save(raw, str(ckpt_path))

    with pytest.raises(CheckpointError):
        ModelTrainer(TrainerConfig(task="binary", device="cpu")).load_checkpoint(
            str(ckpt_path))


@pytest.mark.torch
def test_load_checkpoint_missing_format_version_raises(tmp_path):
    pytest.importorskip("torch")
    import torch
    from ripple.models.config import TrainerConfig
    from ripple.models.exceptions import CheckpointError
    from ripple.models.model_trainer import ModelTrainer

    bad = tmp_path / "bad.pt"
    torch.save({"state_dict": {}, "task": "binary", "input_size": 64}, str(bad))
    with pytest.raises(CheckpointError):
        ModelTrainer(TrainerConfig(task="binary", device="cpu")).load_checkpoint(
            str(bad))
