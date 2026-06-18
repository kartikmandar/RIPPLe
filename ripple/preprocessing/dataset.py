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
    # pin_memory only benefits CUDA host->device copies; gating on CUDA
    # availability silences the MPS/CPU "pin_memory not supported" warning.
    pin_memory = torch.cuda.is_available()
    return DataLoader(dataset, batch_size=batch_size, shuffle=shuffle,
                      num_workers=num_workers, pin_memory=pin_memory,
                      worker_init_fn=seed_worker if num_workers > 0 else None,
                      generator=g,
                      persistent_workers=num_workers > 0)


def _carve_val_from_train(rows, val_fraction, seed, group_key="group_key"):
    """Move a group-coherent val_fraction of the train *groups* into split='val'.

    Operates only on rows currently in split=='train'; leaves 'test' untouched so a
    group never spans train and val (leakage-safe). Mutates and returns ``rows``.
    """
    train_groups = sorted({r[group_key] for r in rows if r.get("split") == "train"})
    if not train_groups or val_fraction <= 0:
        return rows
    rng = random.Random(seed)
    rng.shuffle(train_groups)
    n_val = int(round(val_fraction * len(train_groups)))
    n_val = min(max(n_val, 1), len(train_groups) - 1) if len(train_groups) > 1 else 0
    val_groups = set(train_groups[:n_val])
    for r in rows:
        if r.get("split") == "train" and r[group_key] in val_groups:
            r["split"] = "val"
    return rows


_THREECLASS_LABELS = {"no_sub": 0, "cdm": 1, "axion": 2}


def _is_threeclass_root(src_root):
    src_root = str(src_root)
    names = {n for n in os.listdir(src_root)
             if os.path.isdir(os.path.join(src_root, n))}
    return bool(names) and names.issubset(set(_THREECLASS_LABELS))


def _crawl_threeclass(src_root):
    """Crawl no_sub/cdm/axion folders -> rows with label map and split='train' (carved later)."""
    src_root = str(src_root)
    rows = []
    for cls in sorted(os.listdir(src_root)):
        sub = os.path.join(src_root, cls)
        if not os.path.isdir(sub) or cls not in _THREECLASS_LABELS:
            continue
        for fn in sorted(os.listdir(sub)):
            if fn.endswith(".npy"):
                rows.append({"path": os.path.join(sub, fn),
                             "label": _THREECLASS_LABELS[cls],
                             "split": "train",
                             "group_key": cls})
    return rows


def ingest_deeplense_dataset(src_root, out_dir, *, val_fraction=0.1,
                             copy_arrays=True, band_order="g,r,i", seed=0):
    """Convert a DeepLense class-folder dataset into a RIPPLe manifest + cutouts.

    Supports two source layouts:

    * Binary (Task 23): ``{train,test}_{lenses,nonlenses}/`` per-object ``.npy``
      via ``ingest_labels_from_dirs`` (lens=1, non-lens=0).  Test dirs are pinned
      to split='test'; val_fraction is carved (group-coherent) from train groups.

    * 3-class (Task 24): ``{no_sub,cdm,axion}/`` per-object single-channel ``.npy``
      (auto-detected when the only subdirs are exactly that set).  Labels are mapped
      {no_sub:0, cdm:1, axion:2}; single-channel arrays are replicated 1->3; a
      deterministic 15 % test holdout is carved per class before the val carve.

    In both cases the function adds ``RippleCutoutDataset``-required fields
    (contiguous ``index``, ``status='accepted'``, ``channels=3``, ``size_px``,
    ``split``, leakage-safe ``group_key``), optionally copies arrays into
    ``out_dir/cutouts``, and writes ``out_dir/manifest.csv``.  Returns the
    manifest.csv path.
    """
    out_dir = str(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    cutouts_dir = os.path.join(out_dir, "cutouts")
    if copy_arrays:
        os.makedirs(cutouts_dir, exist_ok=True)

    if _is_threeclass_root(src_root):
        rows = _crawl_threeclass(src_root)
        threeclass = True
    else:
        rows = ingest_labels_from_dirs(src_root)
        threeclass = False
    if not rows:
        raise ValueError(f"No .npy class folders found under {src_root!r}")

    out_rows = []
    for r in rows:
        src_path = r["path"]
        stem = os.path.splitext(os.path.basename(src_path))[0]
        cls_folder = os.path.basename(os.path.dirname(src_path))
        arr = np.load(src_path)
        if arr.ndim == 2:
            arr = np.repeat(arr[None, :, :], 3, axis=0)
        elif arr.ndim == 3 and arr.shape[0] == 1:
            arr = np.repeat(arr, 3, axis=0)
        arr = np.ascontiguousarray(arr, dtype=np.float32)
        channels, size_px = int(arr.shape[0]), int(arr.shape[-1])

        if copy_arrays:
            cls_out_dir = os.path.join(cutouts_dir, cls_folder)
            os.makedirs(cls_out_dir, exist_ok=True)
            dst_path = os.path.join(cls_out_dir, f"{stem}.npy")
            np.save(dst_path, arr)
            path = dst_path
        else:
            path = src_path

        out_rows.append({
            "path": path,
            "label": r["label"],
            "split": r["split"],
            "group_key": stem,
            "status": "accepted",
            "channels": channels,
            "size_px": size_px,
            "band_order": band_order,
        })

    if threeclass:
        # 3-class source has no test dirs: carve a deterministic test holdout per class,
        # then carve val out of the remaining train groups (both group-coherent).
        rng = random.Random(seed)
        by_label = defaultdict(list)
        for r in out_rows:
            by_label[r["label"]].append(r)
        for _label, group in by_label.items():
            group_sorted = sorted(group, key=lambda r: r["group_key"])
            rng.shuffle(group_sorted)
            n_test = max(1, int(round(0.15 * len(group_sorted)))) if len(group_sorted) > 2 else 0
            for r in group_sorted[:n_test]:
                r["split"] = "test"
    _carve_val_from_train(out_rows, val_fraction, seed)

    for i, r in enumerate(out_rows):
        r["index"] = i

    manifest_path = os.path.join(out_dir, "manifest.csv")
    manifest_mod.write_manifest(out_rows, manifest_path)
    return manifest_path
