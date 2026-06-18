"""Offline, CPU-only tests for ripple.models.model_evaluator.

All torch-dependent tests are gated with pytest.importorskip("torch").
"""
import numpy as np
import pytest


def _make_binary_loader(n=16, seed=0):
    torch = pytest.importorskip("torch")
    from torch.utils.data import TensorDataset, DataLoader
    g = torch.Generator().manual_seed(seed)
    # Two separable blobs so a trivial separating model is meaningful.
    half = n // 2
    x_neg = torch.randn(half, 3, 8, 8, generator=g) - 1.0
    x_pos = torch.randn(half, 3, 8, 8, generator=g) + 1.0
    x = torch.cat([x_neg, x_pos], dim=0)
    y = torch.cat([torch.zeros(half), torch.ones(half)]).long()
    return DataLoader(TensorDataset(x, y), batch_size=4, shuffle=False)


class _StubBinaryModel:
    """Emits ONE logit per row = mean pixel value (separates the two blobs)."""

    def __init__(self):
        self._eval = False

    def eval(self):
        self._eval = True
        return self

    def predict_logits(self, x):
        return x.mean(dim=(1, 2, 3), keepdim=True).reshape(-1, 1)

    def forward_logits(self, x):
        return self.predict_logits(x)


@pytest.mark.torch
def test_evaluator_init_accepts_str_and_config():
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    assert ModelEvaluator("binary").task == "binary"
    assert ModelEvaluator(None).task == "binary"

    class _Cfg:
        task = "multiclass"

    assert ModelEvaluator(_Cfg()).task == "multiclass"


@pytest.mark.torch
def test_evaluator_binary_metric_keys_and_ranges():
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_binary_loader()
    ev = ModelEvaluator("binary")
    metrics = ev.evaluate(_StubBinaryModel(), loader)
    for key in ("auc", "accuracy", "precision", "recall", "f1",
                "confusion_matrix", "threshold"):
        assert key in metrics
    assert 0.0 <= metrics["auc"] <= 1.0
    assert 0.0 <= metrics["accuracy"] <= 1.0
    cm = np.asarray(metrics["confusion_matrix"])
    assert cm.shape == (2, 2)
    assert metrics["threshold"] == 0.5


@pytest.mark.torch
def test_evaluator_predict_proba_masks_unlabeled():
    torch = pytest.importorskip("torch")
    from torch.utils.data import TensorDataset, DataLoader
    from ripple.models.model_evaluator import ModelEvaluator
    x = torch.randn(6, 3, 8, 8)
    y = torch.tensor([0, 1, -1, 1, -1, 0]).long()
    loader = DataLoader(TensorDataset(x, y), batch_size=2, shuffle=False)
    ev = ModelEvaluator("binary")
    y_true, y_score = ev.predict_proba(_StubBinaryModel(), loader)
    assert y_true.shape[0] == 4  # two y==-1 rows dropped
    assert y_score.shape[0] == 4
    assert ((y_score >= 0.0) & (y_score <= 1.0)).all()


@pytest.mark.torch
def test_evaluator_perfect_separation_gives_auc_one():
    """A perfectly separable dataset should yield AUC == 1.0."""
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_binary_loader(n=16, seed=42)
    ev = ModelEvaluator("binary")
    # The stub model uses mean pixel value which perfectly separates blobs
    # built with +/-1 shift; with enough separation AUC should be 1.0.
    metrics = ev.evaluate(_StubBinaryModel(), loader)
    assert metrics["auc"] == pytest.approx(1.0, abs=1e-9)


@pytest.mark.torch
def test_evaluator_predict_proba_scores_in_unit_interval():
    """All returned probabilities must be in [0, 1]."""
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_binary_loader()
    ev = ModelEvaluator("binary")
    y_true, y_score = ev.predict_proba(_StubBinaryModel(), loader)
    assert y_true.shape[0] == 16
    assert y_score.shape[0] == 16
    assert ((y_score >= 0.0) & (y_score <= 1.0)).all()
