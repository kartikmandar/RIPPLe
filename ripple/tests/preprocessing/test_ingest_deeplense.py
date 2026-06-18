"""Offline tests for ingest_deeplense_dataset (tiny synthetic .npy class folders)."""
import os

import numpy as np
import pytest

from ripple.preprocessing import (
    ingest_deeplense_dataset,
    read_manifest,
    MANIFEST_FIELDS,
    RippleCutoutDataset,
)


def _make_binary_fixture(root, n_train=6, n_test=4):
    for split, lens_n, non_n in (("train", n_train, n_train), ("test", n_test, n_test)):
        for cls, count in ((f"{split}_lenses", lens_n), (f"{split}_nonlenses", non_n)):
            d = os.path.join(root, cls)
            os.makedirs(d, exist_ok=True)
            for k in range(count):
                np.save(os.path.join(d, f"obj_{cls}_{k}.npy"),
                        np.random.rand(3, 8, 8).astype(np.float32))


def test_ingest_binary_manifest_and_roundtrip(tmp_path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    _make_binary_fixture(str(src))

    manifest_path = ingest_deeplense_dataset(str(src), str(out), val_fraction=0.5, seed=0)
    assert manifest_path.endswith("manifest.csv")
    assert os.path.exists(manifest_path)

    rows = read_manifest(manifest_path)
    assert rows, "expected non-empty manifest"

    # contiguous index, required Dataset fields, status pinned accepted, 3 channels
    indices = [r["index"] for r in rows]
    assert indices == list(range(len(rows)))
    for r in rows:
        assert set(MANIFEST_FIELDS).issuperset(r.keys())
        assert r["status"] == "accepted"
        assert r["channels"] == 3
        assert r["size_px"] == 8
        assert r["band_order"] == "g,r,i"
        assert os.path.exists(r["path"])

    # label convention pinned: lens=1, non-lens=0
    for r in rows:
        is_lens = "nonlens" not in os.path.basename(os.path.dirname(r["path"]))
        assert r["label"] == (1 if is_lens else 0)

    # splits present; test dirs pinned to test; val carved from train
    splits = {r["split"] for r in rows}
    assert {"train", "val", "test"}.issubset(splits)

    # leakage-safe: no group_key spans train and val
    by_group = {}
    for r in rows:
        by_group.setdefault(r["group_key"], set()).add(r["split"])
    for grp, sset in by_group.items():
        assert not ({"train", "val"}.issubset(sset)), f"group {grp} leaks train<->val"

    # Dataset + loader round-trip on the train split
    from ripple.preprocessing.dataset import make_dataloader
    ds = RippleCutoutDataset(manifest_path, split="train")
    assert len(ds) > 0
    loader = make_dataloader(ds, batch_size=2, shuffle=False, num_workers=0, seed=0)
    xb, yb = next(iter(loader))
    assert tuple(xb.shape) == (2, 3, 8, 8)
    assert xb.dtype.__str__() == "torch.float32"
    assert yb.dtype.__str__() == "torch.int64"
    assert set(yb.tolist()).issubset({0, 1})


# ---------------------------------------------------------------------------
# 3-class (no_sub / cdm / axion) tests — Task 24
# ---------------------------------------------------------------------------

def _make_3class_fixture(root, per_class=6):
    for cls in ("no_sub", "cdm", "axion"):
        d = os.path.join(root, cls)
        os.makedirs(d, exist_ok=True)
        for k in range(per_class):
            np.save(os.path.join(d, f"{cls}_{k}.npy"),
                    np.random.rand(1, 8, 8).astype(np.float32))


def test_ingest_3class_label_map_and_channels(tmp_path):
    src = tmp_path / "src3"
    out = tmp_path / "out3"
    _make_3class_fixture(str(src))

    manifest_path = ingest_deeplense_dataset(str(src), str(out), val_fraction=0.5, seed=0)
    rows = read_manifest(manifest_path)
    assert rows

    label_for = {"no_sub": 0, "cdm": 1, "axion": 2}
    for r in rows:
        assert r["channels"] == 3
        # robust: recover class from the stem prefix
        stem = os.path.basename(r["path"])
        cls = next(c for c in label_for if stem.startswith(c))
        assert r["label"] == label_for[cls]

    arr = np.load(rows[0]["path"])
    assert arr.shape == (3, 8, 8)

    ds = RippleCutoutDataset(manifest_path, split="train")
    assert len(ds) > 0
    x, y = ds[0]
    assert tuple(x.shape) == (3, 8, 8)
    assert int(y) in (0, 1, 2)


def test_ingest_3class_distinct_labels(tmp_path):
    """All three class labels {0, 1, 2} must appear in the manifest."""
    src = tmp_path / "src3b"
    out = tmp_path / "out3b"
    _make_3class_fixture(str(src), per_class=6)

    manifest_path = ingest_deeplense_dataset(str(src), str(out), val_fraction=0.3, seed=42)
    rows = read_manifest(manifest_path)

    labels_found = {r["label"] for r in rows}
    assert labels_found == {0, 1, 2}, f"expected {{0,1,2}}, got {labels_found}"


def test_ingest_3class_splits_present(tmp_path):
    """train/val/test splits must all appear; no group leaks train<->val."""
    src = tmp_path / "src3c"
    out = tmp_path / "out3c"
    _make_3class_fixture(str(src), per_class=6)

    manifest_path = ingest_deeplense_dataset(str(src), str(out), val_fraction=0.5, seed=7)
    rows = read_manifest(manifest_path)

    splits = {r["split"] for r in rows}
    assert {"train", "val", "test"}.issubset(splits), f"missing splits: {splits}"

    by_group = {}
    for r in rows:
        by_group.setdefault(r["group_key"], set()).add(r["split"])
    for grp, sset in by_group.items():
        assert not ({"train", "val"}.issubset(sset)), f"group {grp!r} leaks train<->val"


def test_ingest_3class_no_regression_binary(tmp_path):
    """Binary path must remain unaffected when src has train_lenses/train_nonlenses layout."""
    src = tmp_path / "srcbin"
    out = tmp_path / "outbin"
    _make_binary_fixture(str(src))

    manifest_path = ingest_deeplense_dataset(str(src), str(out), val_fraction=0.5, seed=0)
    rows = read_manifest(manifest_path)

    labels_found = {r["label"] for r in rows}
    assert labels_found == {0, 1}, f"binary path should produce labels {{0,1}}, got {labels_found}"
    for r in rows:
        assert r["channels"] == 3
