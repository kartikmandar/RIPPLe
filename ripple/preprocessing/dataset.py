# ripple/preprocessing/dataset.py
"""Label ingestion, group-aware splitting, and a torch Dataset over the manifest.

Splits are by group_key (tract/patch/cell) to prevent spatial leakage; near-duplicate
positions (patch overlap) are de-duplicated before splitting. Torch is imported lazily
so importing this module never requires torch.
"""
import csv
import os
import random
from collections import defaultdict

import numpy as np

from ripple.preprocessing import manifest as manifest_mod


def ingest_labels_from_csv(path):
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            row = dict(r)
            row["ra"] = float(row["ra"])
            row["dec"] = float(row["dec"])
            if "label" in row and row["label"] not in ("", None):
                row["label"] = int(float(row["label"]))
            rows.append(row)
    return rows


def ingest_labels_from_dirs(root):
    """Directory convention: {train,test}_{lenses,nonlenses}/ -> label 1/0, split inferred."""
    root = str(root)
    rows = []
    for name in sorted(os.listdir(root)):
        sub = os.path.join(root, name)
        if not os.path.isdir(sub):
            continue
        label = 1 if "nonlens" not in name and "lens" in name else (0 if "nonlens" in name else None)
        split = "train" if name.startswith("train") else ("test" if name.startswith("test") else None)
        for fn in sorted(os.listdir(sub)):
            if fn.endswith(".npy"):
                rows.append({"path": os.path.join(sub, fn), "label": label, "split": split,
                             "group_key": name})
    return rows


def dedupe_by_position(rows, decimals=5):
    seen, out = set(), []
    for r in rows:
        key = (round(float(r["ra"]), decimals), round(float(r["dec"]), decimals))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def group_aware_split(rows, ratios=(0.7, 0.15, 0.15), seed=0, group_key="group_key"):
    groups = defaultdict(list)
    for r in rows:
        groups[r.get(group_key)].append(r)
    keys = sorted(groups.keys(), key=lambda k: (k is None, str(k)))
    rng = random.Random(seed)
    rng.shuffle(keys)
    n = len(keys)
    n_train = int(round(ratios[0] * n))
    n_val = int(round(ratios[1] * n))
    split_of = {}
    for i, k in enumerate(keys):
        split_of[k] = "train" if i < n_train else ("val" if i < n_train + n_val else "test")
    for r in rows:
        r["split"] = split_of[r.get(group_key)]
    return rows


def seed_worker(worker_id):  # pragma: no cover - exercised via DataLoader
    import torch
    seed = (torch.initial_seed() + worker_id) % 2 ** 32
    np.random.seed(seed)
    random.seed(seed)


class RippleCutoutDataset:
    """Map-style torch Dataset over accepted manifest rows. (torch imported lazily.)"""
    def __init__(self, manifest_path, split=None, transform=None):
        self.transform = transform
        rows = manifest_mod.read_manifest(manifest_path)
        self.rows = [r for r in rows
                     if r.get("status") == "accepted" and r.get("path")
                     and (split is None or r.get("split") == split)]

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, idx):
        import torch
        row = self.rows[idx]
        arr = np.load(row["path"])
        x = torch.from_numpy(np.ascontiguousarray(arr, dtype=np.float32)).float()
        if self.transform is not None:
            x = self.transform(x)
        label = row.get("label")
        y = torch.tensor(-1 if label is None else int(label), dtype=torch.long)
        return x, y


def make_dataloader(dataset, batch_size=32, shuffle=True, num_workers=0, seed=0):
    import torch
    from torch.utils.data import DataLoader

    # DataLoader accepts any map-style object with __len__/__getitem__; no Dataset subclass needed.
    g = torch.Generator()
    g.manual_seed(seed)
    return DataLoader(dataset, batch_size=batch_size, shuffle=shuffle,
                      num_workers=num_workers, pin_memory=True,
                      worker_init_fn=seed_worker if num_workers > 0 else None,
                      generator=g,
                      persistent_workers=num_workers > 0)
