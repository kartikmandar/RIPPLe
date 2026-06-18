import csv

import numpy as np
import pytest

torch = pytest.importorskip("torch")

pytestmark = pytest.mark.torch

from ripple.models.inference import predict_batch, predict_dataset


class _OrderProbeModel:
    """Minimal model exposing the contract surface; emits one dict per row,
    tagging each with its input row-sum so order is verifiable."""

    def __init__(self):
        self.evaled = False
        self.device = "cpu"

    def eval(self):
        self.evaled = True
        return self

    def to(self, device):
        self.device = device
        return self

    def predict_batch(self, tensor, *, batch_size=32, device=None):
        out = []
        for i in range(tensor.shape[0]):
            out.append(
                {
                    "index": i,
                    "row_sum": float(tensor[i].sum().item()),
                    "score": float(tensor[i].sum().item()),
                }
            )
        return out


def test_predict_batch_order_preserving_and_dict_keys():
    model = _OrderProbeModel()
    n = 7
    tensor = torch.stack([torch.full((3, 8, 8), float(i)) for i in range(n)])
    rows = predict_batch(model, tensor, batch_size=4)
    assert model.evaled
    assert isinstance(rows, list)
    assert len(rows) == n
    assert all(isinstance(r, dict) for r in rows)
    # order preserved: row i carries the i-th input's sum
    sums = [r["row_sum"] for r in rows]
    expected = [float(torch.full((3, 8, 8), float(i)).sum().item()) for i in range(n)]
    assert sums == expected
    assert "score" in rows[0]


def _write_tiny_manifest(tmp_path, n=5):
    cutouts = tmp_path / "cutouts"
    cutouts.mkdir()
    from ripple.preprocessing.manifest import MANIFEST_FIELDS

    rows = []
    for i in range(n):
        arr = np.full((3, 8, 8), float(i), dtype=np.float32)
        npy = cutouts / f"cut_{i}.npy"
        np.save(npy, arr)
        rows.append(
            {
                "index": i, "path": str(npy), "label": i % 2,
                "ra": 150.0 + i, "dec": 2.0 + i, "tract": 1, "patch": i,
                "band_order": "g,r,i", "channels": 3, "split": "test",
                "group_key": f"obj_{i}", "pixel_scale": 0.2, "size_px": 8,
                "status": "accepted",
            }
        )
    manifest = tmp_path / "manifest.csv"
    with open(manifest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in MANIFEST_FIELDS})
    return str(manifest)


def test_predict_dataset_via_disk_fixture(tmp_path):
    from ripple.preprocessing.dataset import RippleCutoutDataset

    manifest = _write_tiny_manifest(tmp_path, n=5)
    ds = RippleCutoutDataset(manifest, split="test")
    model = _OrderProbeModel()
    rows = predict_dataset(model, ds, batch_size=2)
    assert model.evaled
    assert isinstance(rows, list)
    assert len(rows) == 5
    assert all(isinstance(r, dict) for r in rows)
    # shuffle=False => dataset order preserved; row i's input sum == i * 3 * 8 * 8
    sums = [r["row_sum"] for r in rows]
    assert sums == [float(i * 3 * 8 * 8) for i in range(5)]
