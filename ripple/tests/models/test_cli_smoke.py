"""End-to-end smoke test for the model CLIs (torch-gated, offline, tmp_path).

Builds a tiny synthetic cutout dataset + manifest in ``tmp_path``, runs a real
1-epoch ``train.main([...])`` to produce a checkpoint, then runs
``evaluate.main([...])`` on it and asserts metrics (incl. ``auc``) are emitted.
No network, no committed ``.pt`` (everything lives under ``tmp_path``).
"""
import csv
import os

import numpy as np
import pytest

from ripple.models.cli import train as train_cli
from ripple.models.cli import evaluate as eval_cli
from ripple.preprocessing.manifest import MANIFEST_FIELDS

pytestmark = pytest.mark.torch


def _write_tiny_manifest(tmp_path):
    """Create ~8 (3,8,8) .npy cutouts + a manifest.csv with train/test splits.

    Two cutouts (one per class) are placed in split='test'; the rest are
    split='train'. Each split carries both labels so the binary metrics
    (incl. AUC) are well-defined.
    """
    cutouts = tmp_path / "cutouts"
    cutouts.mkdir()
    rng = np.random.default_rng(0)

    # (split, label) assignments: balanced classes in both splits.
    plan = [
        ("train", 0), ("train", 1), ("train", 0), ("train", 1),
        ("train", 0), ("train", 1),
        ("test", 0), ("test", 1),
    ]
    rows = []
    for idx, (split, label) in enumerate(plan):
        # Make the two classes weakly separable so AUC is computable (not NaN).
        base = float(label) * 0.5
        arr = (rng.standard_normal((3, 8, 8)).astype(np.float32) * 0.1) + base
        path = cutouts / f"cut_{idx:02d}.npy"
        np.save(path, arr)
        rows.append({
            "index": idx,
            "path": str(path),
            "label": label,
            "split": split,
            "group_key": f"g{idx:02d}",
            "status": "accepted",
            "channels": 3,
            "size_px": 8,
            "band_order": "g,r,i",
        })

    manifest = tmp_path / "manifest.csv"
    with open(manifest, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS,
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in MANIFEST_FIELDS})
    return str(manifest)


def test_train_then_evaluate_end_to_end(tmp_path, capsys):
    pytest.importorskip("torch")

    manifest = _write_tiny_manifest(tmp_path)
    ckpt = str(tmp_path / "ckpt.pt")

    # --- train: a real 1-epoch run on resnet_binary over the tiny dataset -----
    train_rc = train_cli.main([
        "--manifest", manifest,
        "--model-type", "resnet_binary",
        "--task", "binary",
        "--epochs", "1",
        "--batch-size", "4",
        "--num-workers", "0",
        "--seed", "0",
        "--device", "cpu",
        "--out", ckpt,
    ])
    assert train_rc == 0
    assert os.path.exists(ckpt), "training did not write the checkpoint file"

    # --- evaluate: rebuild from the checkpoint metadata + load weights once ---
    capsys.readouterr()  # drop train output
    eval_rc = eval_cli.main([
        "--manifest", manifest,
        "--checkpoint", ckpt,
        "--model-type", "resnet_binary",
        "--split", "test",
        "--batch-size", "4",
        "--num-workers", "0",
        "--device", "cpu",
    ])
    assert eval_rc == 0
    out = capsys.readouterr().out
    assert "auc" in out, f"evaluate did not print AUC; got:\n{out}"
