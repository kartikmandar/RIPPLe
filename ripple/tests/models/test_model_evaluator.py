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


# ---------------------------------------------------------------------------
# Multiclass tests (Task 14)
# ---------------------------------------------------------------------------

def _make_multiclass_loader(n=18, num_classes=3, seed=1):
    torch = pytest.importorskip("torch")
    from torch.utils.data import TensorDataset, DataLoader
    g = torch.Generator().manual_seed(seed)
    per = n // num_classes
    xs, ys = [], []
    for c in range(num_classes):
        # Encode class in a channel-specific offset so a trivial model separates.
        x = torch.randn(per, 3, 8, 8, generator=g)
        x[:, c % 3, :, :] += 3.0
        xs.append(x)
        ys.append(torch.full((per,), c))
    x = torch.cat(xs, dim=0)
    y = torch.cat(ys, dim=0).long()
    return DataLoader(TensorDataset(x, y), batch_size=6, shuffle=False)


class _StubMulticlassModel:
    """Emits per-class logits = per-channel mean (separates the blobs above)."""

    def __init__(self, num_classes=3):
        self.num_classes = num_classes

    def eval(self):
        return self

    def predict_logits(self, x):
        return x.mean(dim=(2, 3))  # (N, 3) channel means -> 3 class logits

    def forward_logits(self, x):
        return self.predict_logits(x)


@pytest.mark.torch
def test_evaluator_multiclass_macro_keys():
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_multiclass_loader()
    ev = ModelEvaluator("multiclass")
    metrics = ev.evaluate(_StubMulticlassModel(), loader)
    for key in ("auc", "accuracy", "precision", "recall", "f1",
                "confusion_matrix", "threshold", "per_class"):
        assert key in metrics
    assert 0.0 <= metrics["auc"] <= 1.0
    cm = np.asarray(metrics["confusion_matrix"])
    assert cm.shape == (3, 3)
    assert len(metrics["per_class"]) == 3
    assert "f1" in metrics["per_class"][0]


@pytest.mark.torch
def test_evaluator_multiclass_perfect_separation_auc_one():
    """A perfectly separable 3-class dataset should yield macro OVR AUC == 1.0."""
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_multiclass_loader(n=18, num_classes=3, seed=42)
    ev = ModelEvaluator("multiclass")
    metrics = ev.evaluate(_StubMulticlassModel(), loader)
    assert metrics["auc"] == pytest.approx(1.0, abs=1e-9)


@pytest.mark.torch
def test_evaluator_multiclass_masks_unlabeled():
    """y == -1 rows must be excluded from multiclass metrics."""
    torch = pytest.importorskip("torch")
    from torch.utils.data import TensorDataset, DataLoader
    from ripple.models.model_evaluator import ModelEvaluator

    x = torch.randn(9, 3, 8, 8)
    # Rows 2 and 5 are unlabeled (-1); valid labels span 3 classes.
    y = torch.tensor([0, 1, -1, 2, 0, -1, 1, 2, 0]).long()
    loader = DataLoader(TensorDataset(x, y), batch_size=3, shuffle=False)
    ev = ModelEvaluator("multiclass")
    y_true, y_score = ev.predict_proba(_StubMulticlassModel(), loader)
    assert y_true.shape[0] == 7  # two y==-1 rows dropped
    assert y_score.shape == (7, 3)
    assert ((y_score >= 0.0) & (y_score <= 1.0)).all()


@pytest.mark.torch
def test_evaluator_multiclass_per_class_structure():
    """per_class list must have one entry per class with precision/recall/f1."""
    pytest.importorskip("torch")
    from ripple.models.model_evaluator import ModelEvaluator
    loader = _make_multiclass_loader()
    ev = ModelEvaluator("multiclass")
    metrics = ev.evaluate(_StubMulticlassModel(), loader)
    per_class = metrics["per_class"]
    assert len(per_class) == 3
    for entry in per_class:
        for k in ("precision", "recall", "f1"):
            assert k in entry
            assert 0.0 <= entry[k] <= 1.0


@pytest.mark.torch
@pytest.mark.filterwarnings(
    "ignore::sklearn.exceptions.UndefinedMetricWarning"
)
def test_evaluator_multiclass_single_class_no_crash():
    """OVR AUC should degrade gracefully (nan) when only one class present."""
    torch = pytest.importorskip("torch")
    from torch.utils.data import TensorDataset, DataLoader
    from ripple.models.model_evaluator import ModelEvaluator

    x = torch.randn(6, 3, 8, 8)
    y = torch.zeros(6, dtype=torch.long)  # only class 0
    loader = DataLoader(TensorDataset(x, y), batch_size=2, shuffle=False)
    ev = ModelEvaluator("multiclass")
    # Must not raise; the degenerate single-class OVR AUC is nan.
    metrics = ev.evaluate(_StubMulticlassModel(), loader)
    assert "auc" in metrics
    assert np.isnan(metrics["auc"])
