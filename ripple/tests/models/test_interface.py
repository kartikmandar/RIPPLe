"""Offline, torch-free tests for the model interface layer.

PredictionResult.to_dict is pure stdlib; postprocess / predict_batch tests
(added later) gate on torch via importorskip + @pytest.mark.torch.
"""
import pytest

from ripple.models.base_model import PredictionResult


def test_prediction_result_to_dict_binary_field_mapping():
    pr = PredictionResult(
        task="binary",
        pred_class=1,
        class_name="lens",
        score=0.83,
        probabilities={"non_lens": 0.17, "lens": 0.83},
        confidence=0.83,
    )
    d = pr.to_dict()
    assert d["task"] == "binary"
    assert d["pred_class"] == 1
    assert d["pred_class_name"] == "lens"
    assert d["score"] == pytest.approx(0.83)
    assert d["confidence"] == pytest.approx(0.83)
    # probabilities spread into prob_<name>
    assert d["prob_lens"] == pytest.approx(0.83)
    assert d["prob_non_lens"] == pytest.approx(0.17)
    # no class_name key leaks; mapped to pred_class_name only
    assert "class_name" not in d


def test_prediction_result_to_dict_multiclass_and_extra():
    pr = PredictionResult(
        task="multiclass",
        pred_class=2,
        class_name="axion",
        score=0.6,
        probabilities={"no_sub": 0.1, "cdm": 0.3, "axion": 0.6},
        confidence=0.6,
        extra={"model_name": "vit_multiclass"},
    )
    d = pr.to_dict()
    assert d["pred_class"] == 2
    assert d["pred_class_name"] == "axion"
    assert d["prob_no_sub"] == pytest.approx(0.1)
    assert d["prob_cdm"] == pytest.approx(0.3)
    assert d["prob_axion"] == pytest.approx(0.6)
    # extra merged into the row
    assert d["model_name"] == "vit_multiclass"
    # binary-only aliases absent for a multiclass result
    assert "prob_lens" not in d


def test_prediction_result_none_fields_round_trip():
    pr = PredictionResult(
        task="super_res",
        pred_class=None,
        class_name=None,
        score=None,
        probabilities={},
        confidence=None,
    )
    d = pr.to_dict()
    assert d["task"] == "super_res"
    assert d["pred_class"] is None
    assert d["pred_class_name"] is None
    assert d["score"] is None
    assert d["confidence"] is None


def _stub_config(task, num_classes, class_names):
    """Duck-typed stand-in for ModelConfig: only the attributes BaseModel reads.

    Keeps the interface test independent of the config.py task while honoring
    the contract (head_logits = 1 if binary else num_classes; resolve_device).
    """
    class _Cfg:
        pass

    cfg = _Cfg()
    cfg.model_type = "stub"
    cfg.task = task
    cfg.encoder = "resnet18"
    cfg.num_classes = num_classes
    cfg.class_names = class_names
    cfg.input_size = 8
    cfg.weights_path = None
    cfg.device = "cpu"
    cfg.head_logits = 1 if task == "binary" else num_classes
    cfg.resolve_device = lambda: "cpu"
    return cfg


def test_postprocess_binary_single_logit_sigmoid():
    torch = pytest.importorskip("torch")
    from ripple.models.base_model import BaseModel, PredictionResult

    cfg = _stub_config("binary", num_classes=2, class_names=("non_lens", "lens"))
    model = BaseModel(cfg)
    # binary head emits ONE logit per row; +2.0 -> sigmoid ~0.88 -> lens
    logits = torch.tensor([[2.0], [-2.0]])
    results = model.postprocess(logits)

    assert isinstance(results, list) and len(results) == 2
    assert all(isinstance(r, PredictionResult) for r in results)

    pos = results[0]
    assert pos.task == "binary"
    assert pos.pred_class == 1
    assert pos.class_name == "lens"
    expected = torch.sigmoid(torch.tensor(2.0)).item()
    assert pos.probabilities["lens"] == pytest.approx(expected, abs=1e-5)
    assert pos.probabilities["non_lens"] == pytest.approx(1.0 - expected, abs=1e-5)
    # score = prob_lens (catalog triage), confidence = max-class prob
    assert pos.score == pytest.approx(expected, abs=1e-5)
    assert pos.confidence == pytest.approx(expected, abs=1e-5)

    neg = results[1]
    assert neg.pred_class == 0
    assert neg.class_name == "non_lens"
    # a confident non-lens has a LOW score (must not float to catalog top)
    assert neg.score == pytest.approx(1.0 - expected, abs=1e-5)


def test_postprocess_multiclass_softmax():
    torch = pytest.importorskip("torch")
    from ripple.models.base_model import BaseModel

    cfg = _stub_config("multiclass", num_classes=3, class_names=("no_sub", "cdm", "axion"))
    model = BaseModel(cfg)
    logits = torch.tensor([[1.0, 2.0, 5.0], [4.0, 1.0, 0.0]])
    results = model.postprocess(logits)

    assert len(results) == 2
    r0 = results[0]
    assert r0.task == "multiclass"
    assert r0.pred_class == 2
    assert r0.class_name == "axion"
    probs = torch.softmax(torch.tensor([1.0, 2.0, 5.0]), dim=0)
    assert r0.probabilities["axion"] == pytest.approx(probs[2].item(), abs=1e-5)
    # score == confidence == max-class probability for multiclass
    assert r0.score == pytest.approx(probs[2].item(), abs=1e-5)
    assert r0.confidence == pytest.approx(probs[2].item(), abs=1e-5)
    assert sum(r0.probabilities.values()) == pytest.approx(1.0, abs=1e-5)

    assert results[1].pred_class == 0
    assert results[1].class_name == "no_sub"


def test_predict_batch_order_preserving():
    torch = pytest.importorskip("torch")
    import torch.nn as nn
    from ripple.models.base_model import BaseModel

    cfg = _stub_config("binary", num_classes=2, class_names=("non_lens", "lens"))
    model = BaseModel(cfg)

    # Deterministic stub net: logit = mean(x) * 10, so row order is identifiable.
    class _StubNet(nn.Module):
        def forward(self, x):
            m = x.flatten(1).mean(dim=1, keepdim=True)
            return m * 10.0

    model._net = _StubNet()

    # 4 rows of (3,8,8) with strictly increasing means -> strictly increasing prob_lens
    x = torch.stack([torch.full((3, 8, 8), float(i)) for i in range(4)])
    rows = model.predict_batch(x, batch_size=2)  # forces 2 chunks

    assert isinstance(rows, list) and len(rows) == 4
    assert all(isinstance(r, dict) for r in rows)
    scores = [r["score"] for r in rows]
    # order preserved across chunk boundary: monotonically increasing
    assert scores == sorted(scores)
    assert scores[0] < scores[-1]
    assert rows[0]["pred_class_name"] in ("non_lens", "lens")
