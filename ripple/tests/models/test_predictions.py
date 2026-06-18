import csv
import pytest

from ripple.models.predictions import (
    PREDICTION_FIELDS,
    write_predictions,
    read_predictions,
)


def test_prediction_fields_exact_order():
    assert PREDICTION_FIELDS == [
        "index", "ra", "dec", "tract", "patch", "label",
        "model_name", "model_type",
        "score", "prob_lens", "pred_class", "pred_class_name", "confidence",
        "prob_non_lens", "prob_no_sub", "prob_cdm", "prob_axion",
        "deeplense_class", "deeplense_confidence",
        "deeplense_no_sub_prob", "deeplense_cdm_prob", "deeplense_vortex_prob",
    ]


def test_write_uses_extrasaction_ignore(tmp_path):
    rows = [{"index": 0, "prob_lens": 0.9, "junk_col": "drop me"}]
    path = tmp_path / "predictions.csv"
    write_predictions(rows, str(path))
    with open(path, newline="") as fh:
        header = next(csv.reader(fh))
    assert header == PREDICTION_FIELDS
    assert "junk_col" not in header


def test_write_read_round_trip(tmp_path):
    rows = [
        {
            "index": 0, "ra": 150.1, "dec": 2.2, "tract": 9813, "patch": 42,
            "label": 1, "model_name": "ripple_resnet", "model_type": "resnet_binary",
            "score": 0.88, "prob_lens": 0.88, "pred_class": 1,
            "pred_class_name": "lens", "confidence": 0.88, "prob_non_lens": 0.12,
        },
        {
            "index": 1, "ra": 151.0, "dec": 3.3, "tract": 9813, "patch": 7,
            "label": 0, "model_name": "ripple_resnet", "model_type": "resnet_binary",
            "score": 0.05, "prob_lens": 0.05, "pred_class": 0,
            "pred_class_name": "non_lens", "confidence": 0.95, "prob_non_lens": 0.95,
        },
    ]
    path = tmp_path / "predictions.csv"
    write_predictions(rows, str(path))
    back = read_predictions(str(path))
    assert len(back) == 2
    assert back[0]["index"] == 0
    assert back[0]["prob_lens"] == pytest.approx(0.88)
    assert back[1]["pred_class"] == 0
    assert back[1]["model_type"] == "resnet_binary"


def test_int_float_none_coercion(tmp_path):
    rows = [
        {
            "index": 3, "tract": 100, "patch": 5, "label": 1,
            "pred_class": 2, "deeplense_class": 1,
            "ra": 10.5, "prob_axion": 0.7,
            "dec": "", "score": "", "confidence": "",
        }
    ]
    path = tmp_path / "predictions.csv"
    write_predictions(rows, str(path))
    back = read_predictions(str(path))[0]
    for k in ("index", "tract", "patch", "label", "pred_class", "deeplense_class"):
        assert isinstance(back[k], int)
    assert isinstance(back["ra"], float)
    assert isinstance(back["prob_axion"], float)
    assert back["dec"] is None
    assert back["score"] is None
    assert back["confidence"] is None


def test_deeplense_vortex_alias_equals_prob_axion(tmp_path):
    rows = [{"index": 0, "prob_axion": 0.42, "deeplense_vortex_prob": 0.42}]
    path = tmp_path / "predictions.csv"
    write_predictions(rows, str(path))
    back = read_predictions(str(path))[0]
    assert back["deeplense_vortex_prob"] == pytest.approx(back["prob_axion"])


def test_join_by_index_with_manifest():
    manifest = [
        {"index": 0, "ra": 150.1, "dec": 2.2, "tract": 9813, "patch": 42, "label": 1},
        {"index": 1, "ra": 151.0, "dec": 3.3, "tract": 9813, "patch": 7, "label": 0},
    ]
    by_index = {m["index"]: m for m in manifest}
    preds = [{"index": 1, "prob_lens": 0.9}, {"index": 0, "prob_lens": 0.1}]
    joined = []
    for p in preds:
        m = by_index[p["index"]]
        joined.append({**m, **p})
    assert joined[0]["index"] == 1
    assert joined[0]["ra"] == pytest.approx(151.0)
    assert joined[0]["prob_lens"] == pytest.approx(0.9)
    assert joined[1]["index"] == 0
    assert joined[1]["label"] == 1
