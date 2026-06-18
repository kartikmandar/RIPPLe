# ripple/tests/preprocessing/test_dataset.py
import csv
import numpy as np
import pytest
from ripple.preprocessing import dataset as ds
from ripple.preprocessing.manifest import write_manifest


def test_ingest_labels_from_csv(tmp_path):
    p = tmp_path / "coords.csv"
    with open(p, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["ra", "dec", "label", "group_key"])
        w.writerow([10.0, -30.0, 1, "t1"])
        w.writerow([11.0, -31.0, 0, "t2"])
    rows = ds.ingest_labels_from_csv(p)
    assert rows[0]["label"] == 1 and rows[0]["ra"] == 10.0 and rows[0]["group_key"] == "t1"


def test_ingest_labels_from_dirs(tmp_path):
    for sub in ("train_lenses", "train_nonlenses"):
        (tmp_path / sub).mkdir()
        np.save(tmp_path / sub / "a.npy", np.zeros((3, 4, 4), np.float32))
    rows = ds.ingest_labels_from_dirs(tmp_path)
    labels = {r["label"] for r in rows}
    assert labels == {0, 1} and all("path" in r for r in rows)


def test_group_aware_split_keeps_group_together():
    rows = [{"ra": i, "dec": 0, "group_key": f"g{i % 3}"} for i in range(30)]
    out = ds.group_aware_split(rows, ratios=(0.6, 0.2, 0.2), seed=1)
    from collections import defaultdict
    group_split = defaultdict(set)
    for r in out:
        group_split[r["group_key"]].add(r["split"])
    assert all(len(s) == 1 for s in group_split.values())  # no group spans splits


def test_split_is_deterministic():
    rows = [{"ra": i, "dec": 0, "group_key": f"g{i}"} for i in range(20)]
    a = ds.group_aware_split([dict(r) for r in rows], seed=7)
    b = ds.group_aware_split([dict(r) for r in rows], seed=7)
    assert [r["split"] for r in a] == [r["split"] for r in b]


def test_dedupe_by_position():
    rows = [{"ra": 10.000001, "dec": -30.0}, {"ra": 10.000002, "dec": -30.0},
            {"ra": 12.0, "dec": -31.0}]
    out = ds.dedupe_by_position(rows, decimals=4)
    assert len(out) == 2


@pytest.mark.torch
def test_dataset_getitem_and_dataloader(tmp_path):
    pytest.importorskip("torch")
    import torch
    arr = np.ones((3, 8, 8), np.float32)
    np.save(tmp_path / "c0.npy", arr)
    write_manifest([{"index": 0, "path": str(tmp_path / "c0.npy"), "label": 1,
                     "status": "accepted", "split": "train"}], tmp_path / "manifest.csv")
    dset = ds.RippleCutoutDataset(tmp_path / "manifest.csv", split="train")
    assert len(dset) == 1
    x, y = dset[0]
    assert isinstance(x, torch.Tensor) and x.shape == (3, 8, 8) and int(y) == 1
    loader = ds.make_dataloader(dset, batch_size=1, num_workers=0)
    xb, yb = next(iter(loader))
    assert xb.shape == (1, 3, 8, 8)


@pytest.mark.torch
def test_dataset_only_includes_accepted(tmp_path):
    pytest.importorskip("torch")
    np.save(tmp_path / "c0.npy", np.ones((3, 4, 4), np.float32))
    write_manifest([
        {"index": 0, "path": str(tmp_path / "c0.npy"), "label": 1, "status": "accepted", "split": "train"},
        {"index": 1, "path": "", "label": 0, "status": "rejected", "split": "train"},
    ], tmp_path / "manifest.csv")
    dset = ds.RippleCutoutDataset(tmp_path / "manifest.csv", split="train")
    assert len(dset) == 1
