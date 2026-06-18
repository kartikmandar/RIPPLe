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
