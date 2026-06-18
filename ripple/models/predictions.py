"""Prediction catalog: one row per scored (accepted) cutout.

Mirrors ``ripple.preprocessing.manifest`` exactly (stdlib ``csv.DictWriter`` with
``extrasaction='ignore'``, typed coercion on read, blank -> None). No pandas.

Rows are joinable to ``manifest.csv`` on ``index``. For the binary task
``score == prob_lens`` (rank by lens likelihood); for the 3-class task
``score == confidence`` (max-class probability). ``deeplense_*`` aliases mirror
the ``to_lsst_catalog`` contract; ``deeplense_vortex_prob == prob_axion``.
"""
import csv
import os
from typing import List

PREDICTION_FIELDS: List[str] = [
    "index", "ra", "dec", "tract", "patch", "label",
    "model_name", "model_type",
    "score", "prob_lens", "pred_class", "pred_class_name", "confidence",
    "prob_non_lens", "prob_no_sub", "prob_cdm", "prob_axion",
    "deeplense_class", "deeplense_confidence",
    "deeplense_no_sub_prob", "deeplense_cdm_prob", "deeplense_vortex_prob",
]

_INT_FIELDS = {"index", "tract", "patch", "label", "pred_class", "deeplense_class"}
_FLOAT_FIELDS = {
    "ra", "dec", "score", "prob_lens", "confidence",
    "prob_non_lens", "prob_no_sub", "prob_cdm", "prob_axion",
    "deeplense_confidence",
    "deeplense_no_sub_prob", "deeplense_cdm_prob", "deeplense_vortex_prob",
}


def write_predictions(rows, path) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=PREDICTION_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in PREDICTION_FIELDS})


def read_predictions(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Predictions not found: {path}")
    out = []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            out.append(_coerce(row))
    return out


def _coerce(row):
    coerced = {}
    for k, v in row.items():
        if v == "" or v is None:
            coerced[k] = None
            continue
        if k in _INT_FIELDS:
            try:
                coerced[k] = int(float(v))
                continue
            except ValueError:
                pass
        if k in _FLOAT_FIELDS:
            try:
                coerced[k] = float(v)
                continue
            except ValueError:
                pass
        coerced[k] = v
    return coerced
