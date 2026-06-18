"""CSV manifest: the single source of truth for a preprocessing run.

One row per requested coordinate; the dense (N,C,H,W) batch is built only from
``status == 'accepted'`` rows, while rejected/failed rows retain their reason.
"""
import csv
import os
from typing import List

from ripple.preprocessing.exceptions import ManifestError

MANIFEST_FIELDS: List[str] = [
    "index", "path", "label", "ra", "dec", "tract", "patch",
    "band_order", "channels", "split", "group_key", "pixel_scale",
    "size_px", "status", "reject_reason", "norm_method", "norm_params",
    "softening_estimator", "bad_fraction", "psf_matched",
    "skymap_version", "dp1_version",
]

_INT_FIELDS = {"index", "label", "channels", "tract", "patch", "size_px"}
_FLOAT_FIELDS = {"ra", "dec", "pixel_scale", "bad_fraction"}


def write_manifest(rows, path) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in MANIFEST_FIELDS})


def read_manifest(path):
    if not os.path.exists(path):
        raise ManifestError(f"Manifest not found: {path}")
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
