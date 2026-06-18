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
