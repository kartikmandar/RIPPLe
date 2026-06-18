"""ModelStage wiring: both config shapes, passthrough, and prediction CSV.

No real torch needed: ModelFactory.create and predict_batch are monkeypatched
on the model_stage module so only the stage's own orchestration is exercised.
"""
import csv
import os

from ripple.pipeline.stages.model_stage import ModelStage


def test_init_full_config_shape_resolves_model_type():
    # pipeline_builder passes the FULL config dict (has a top-level 'model' key).
    stage = ModelStage(config={"model": {"operation": "prediction",
                                         "type": "vit_binary",
                                         "params": {"task": "binary"}}})
    assert stage.model_operation == "prediction"
    assert stage.model_type == "vit_binary"
    assert stage.model_params == {"task": "binary"}


def test_init_bare_subdict_shape_resolves_model_type():
    # Legacy/bare shape: the model sub-dict is passed directly (no 'model' key).
    stage = ModelStage(config={"operation": "evaluation", "type": "resnet_binary"})
    assert stage.model_operation == "evaluation"
    assert stage.model_type == "resnet_binary"


def test_init_defaults():
    stage = ModelStage(config={})
    assert stage.model_operation == "prediction"
    assert stage.model_type == "resnet_binary"
    assert stage.model_params == {}


def test_missing_tensor_passthrough_returns_data():
    stage = ModelStage(config={"model": {"operation": "prediction"}})
    data = {"foo": "bar"}  # no 'tensor' key
    out = stage.execute(data)
    assert out is data
    assert "predictions" not in out


def test_prediction_branch_writes_predictions_csv(monkeypatch, tmp_path):
    import ripple.pipeline.stages.model_stage as ms_mod

    # Stub the model object + factory: predict() unused (predict_batch is stubbed).
    class _FakeModel:
        OUTPUT_KIND = "catalog"

    class _FakeFactory:
        @classmethod
        def create(cls, model_type, config=None):
            assert model_type == "resnet_binary"
            return _FakeModel()

    def _fake_predict_batch(model, tensor, *, batch_size=32, device=None):
        # Two accepted rows; dense, order-preserving.
        return [
            {"pred_class": 1, "pred_class_name": "lens",
             "score": 0.9, "prob_lens": 0.9, "prob_non_lens": 0.1, "confidence": 0.9},
            {"pred_class": 0, "pred_class_name": "non_lens",
             "score": 0.2, "prob_lens": 0.2, "prob_non_lens": 0.8, "confidence": 0.8},
        ]

    monkeypatch.setattr(ms_mod, "ModelFactory", _FakeFactory)
    monkeypatch.setattr(ms_mod, "predict_batch", _fake_predict_batch)

    stage = ModelStage(config={"model": {"operation": "prediction",
                                         "type": "resnet_binary"}})

    # Tensor is opaque (length 2); manifest has two accepted rows joinable by index.
    data = {
        "tensor": [object(), object()],
        "preprocess_manifest": [
            {"index": 0, "status": "accepted", "ra": 1.0, "dec": 2.0,
             "tract": 1, "patch": 3, "label": 1},
            {"index": 1, "status": "accepted", "ra": 3.0, "dec": 4.0,
             "tract": 1, "patch": 3, "label": 0},
        ],
        "out_dir": str(tmp_path),
    }
    out = stage.execute(data)

    assert "predictions" in out
    assert len(out["predictions"]) == 2

    csv_path = os.path.join(str(tmp_path), "predictions.csv")
    assert os.path.exists(csv_path)
    with open(csv_path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 2
    # Join-by-index + model metadata threaded through.
    assert rows[0]["index"] == "0"
    assert rows[0]["model_type"] == "resnet_binary"
    assert float(rows[0]["prob_lens"]) == 0.9
    assert rows[1]["index"] == "1"
    assert rows[1]["pred_class"] == "0"


def test_prediction_branch_skips_rejected_rows(monkeypatch, tmp_path):
    import ripple.pipeline.stages.model_stage as ms_mod

    class _FakeModel:
        OUTPUT_KIND = "catalog"

    class _FakeFactory:
        @classmethod
        def create(cls, model_type, config=None):
            return _FakeModel()

    # One accepted tensor row -> one prediction.
    def _fake_predict_batch(model, tensor, *, batch_size=32, device=None):
        return [{"pred_class": 1, "pred_class_name": "lens",
                 "score": 0.7, "prob_lens": 0.7, "confidence": 0.7}]

    monkeypatch.setattr(ms_mod, "ModelFactory", _FakeFactory)
    monkeypatch.setattr(ms_mod, "predict_batch", _fake_predict_batch)

    stage = ModelStage(config={"model": {"operation": "prediction"}})
    data = {
        "tensor": [object()],
        "preprocess_manifest": [
            {"index": 0, "status": "rejected", "ra": 1.0, "dec": 2.0},
            {"index": 1, "status": "accepted", "ra": 3.0, "dec": 4.0,
             "tract": 2, "patch": 5, "label": 1},
        ],
        "out_dir": str(tmp_path),
    }
    out = stage.execute(data)
    # Only the accepted manifest row (index 1) is joined to the single prediction.
    assert len(out["predictions"]) == 1
    assert out["predictions"][0]["index"] == 1
    assert out["predictions"][0]["ra"] == 3.0


def test_factory_failure_is_defensive_passthrough(monkeypatch, tmp_path):
    import ripple.pipeline.stages.model_stage as ms_mod

    class _BoomFactory:
        @classmethod
        def create(cls, model_type, config=None):
            raise RuntimeError("model build boom")

    monkeypatch.setattr(ms_mod, "ModelFactory", _BoomFactory)

    stage = ModelStage(config={"model": {"operation": "prediction"}})
    data = {"tensor": [object()],
            "preprocess_manifest": [{"index": 0, "status": "accepted"}],
            "out_dir": str(tmp_path)}
    out = stage.execute(data)
    # A model failure never crashes the run; data flows through unchanged.
    assert out is data
    assert "predictions" not in out


def test_super_resolution_branch_writes_enhanced_npy(monkeypatch, tmp_path):
    import numpy as np

    import ripple.pipeline.stages.model_stage as ms_mod

    # Image-out stub model: predict() returns the SR contract dict, no predict_batch.
    class _FakeSRModel:
        OUTPUT_KIND = "image"

        def predict(self, data, *, return_image=True):
            tensor = data["tensor"]
            n = len(tensor)
            # One (1, 128, 128) enhanced cutout per accepted row.
            return {
                "alpha": np.zeros((n, 2, 128, 128), dtype=np.float32),
                "output_image": np.ones((n, 1, 128, 128), dtype=np.float32),
                "scale": 2,
            }

    class _FakeFactory:
        @classmethod
        def create(cls, model_type, config=None):
            return _FakeSRModel()

    monkeypatch.setattr(ms_mod, "ModelFactory", _FakeFactory)

    stage = ModelStage(config={"model": {"operation": "prediction",
                                         "type": "anirudh_sr"}})
    data = {
        "tensor": [object(), object()],
        "preprocess_manifest": [
            {"index": 0, "status": "accepted", "ra": 1.0, "dec": 2.0,
             "tract": 1, "patch": 3, "label": 1},
            {"index": 1, "status": "accepted", "ra": 3.0, "dec": 4.0,
             "tract": 1, "patch": 3, "label": 0},
        ],
        "out_dir": str(tmp_path),
    }
    out = stage.execute(data)

    # SR branch sets the enhanced manifest, writes .npy + enhanced_manifest.csv.
    assert "enhanced_manifest" in out
    assert len(out["enhanced_manifest"]) == 2
    assert "predictions" not in out  # SR does NOT write a prediction catalog

    assert os.path.exists(os.path.join(str(tmp_path), "enhanced_000000.npy"))
    assert os.path.exists(os.path.join(str(tmp_path), "enhanced_000001.npy"))
    assert os.path.exists(os.path.join(str(tmp_path), "enhanced_manifest.csv"))
    assert not os.path.exists(os.path.join(str(tmp_path), "predictions.csv"))

    arr = np.load(os.path.join(str(tmp_path), "enhanced_000000.npy"))
    assert arr.shape == (1, 128, 128)
    with open(os.path.join(str(tmp_path), "enhanced_manifest.csv"), newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 2
    assert rows[0]["index"] == "0"
    assert rows[0]["scale"] == "2"


def test_evaluation_branch_sets_metrics_and_predictions(monkeypatch, tmp_path):
    # ModelEvaluator drives a real torch model -> gate on torch availability.
    import pytest
    torch = pytest.importorskip("torch")

    import ripple.pipeline.stages.model_stage as ms_mod

    class _Cfg:
        task = "binary"

    # Minimal real torch binary model: one logit per row, plus a predict_batch
    # for the prediction path. forward() returns logits the evaluator sigmoids.
    class _FakeBinaryModel(torch.nn.Module):
        OUTPUT_KIND = "catalog"

        def __init__(self):
            super().__init__()
            self.config = _Cfg()
            # Deterministic per-row logits keyed by row index in the batch.
            self._logits = torch.tensor([[5.0], [-5.0], [5.0], [-5.0]])

        def forward(self, x):
            n = x.shape[0]
            return self._logits[:n]

        def eval(self):  # nn.Module.eval already exists; keep explicit no-op safe
            return super().eval()

        def predict_batch(self, tensor, *, batch_size=32, device=None):
            # Canned scored rows aligned to the 4 accepted manifest rows.
            return [
                {"pred_class": 1, "pred_class_name": "lens",
                 "score": 0.99, "prob_lens": 0.99, "confidence": 0.99},
                {"pred_class": 0, "pred_class_name": "non_lens",
                 "score": 0.01, "prob_lens": 0.01, "confidence": 0.99},
                {"pred_class": 1, "pred_class_name": "lens",
                 "score": 0.99, "prob_lens": 0.99, "confidence": 0.99},
                {"pred_class": 0, "pred_class_name": "non_lens",
                 "score": 0.01, "prob_lens": 0.01, "confidence": 0.99},
            ]

    class _FakeFactory:
        @classmethod
        def create(cls, model_type, config=None):
            return _FakeBinaryModel()

    def _fake_predict_batch(model, tensor, *, batch_size=32, device=None):
        return model.predict_batch(tensor, batch_size=batch_size, device=device)

    monkeypatch.setattr(ms_mod, "ModelFactory", _FakeFactory)
    monkeypatch.setattr(ms_mod, "predict_batch", _fake_predict_batch)

    stage = ModelStage(config={"model": {"operation": "evaluation",
                                         "type": "resnet_binary"}})

    # Real (4, 3, 8, 8) tensor so TensorDataset/DataLoader work; labels match logits.
    tensor = torch.zeros((4, 3, 8, 8), dtype=torch.float32)
    data = {
        "tensor": tensor,
        "preprocess_manifest": [
            {"index": 0, "status": "accepted", "ra": 1.0, "dec": 2.0, "label": 1},
            {"index": 1, "status": "accepted", "ra": 3.0, "dec": 4.0, "label": 0},
            {"index": 2, "status": "accepted", "ra": 5.0, "dec": 6.0, "label": 1},
            {"index": 3, "status": "accepted", "ra": 7.0, "dec": 8.0, "label": 0},
        ],
        "out_dir": str(tmp_path),
    }
    out = stage.execute(data)

    # Evaluation produces BOTH metrics and the prediction catalog.
    assert "metrics" in out
    assert "auc" in out["metrics"]
    assert "accuracy" in out["metrics"]
    # Logits perfectly separate the labels -> accuracy 1.0, auc 1.0.
    assert out["metrics"]["accuracy"] == 1.0
    assert out["metrics"]["auc"] == 1.0

    assert "predictions" in out
    assert len(out["predictions"]) == 4
    assert os.path.exists(os.path.join(str(tmp_path), "predictions.csv"))
