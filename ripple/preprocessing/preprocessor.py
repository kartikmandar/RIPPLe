"""End-to-end preprocessing orchestrator.

Canonical, non-reorderable order per cutout:
    clean -> (optional PSF match) -> normalize (mask-aware stats)
    -> zero bad pixels -> pad/crop to NxN -> stack -> (C,H,W)
A run() produces a dense (N,C,H,W) tensor from accepted rows only, plus a CSV manifest
that records every requested coordinate (accepted/rejected/failed).
"""
import os
from dataclasses import dataclass
from typing import List, Optional

import numpy as np

from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.data_cleaner import DataCleaner
from ripple.preprocessing.data_transformer import DataTransformer
from ripple.preprocessing.image_normalizer import ImageNormalizer
from ripple.preprocessing.psf_matcher import PSFMatcher
from ripple.preprocessing import manifest as manifest_mod


@dataclass
class PreprocessResult:
    tensor: Optional["object"]   # torch.Tensor or None
    manifest: List[dict]
    accepted_indices: List[int]


class Preprocessor:
    def __init__(self, config=None):
        self.config = config or PreprocessingConfig()
        self.cleaner = DataCleaner(self.config)
        self.normalizer = ImageNormalizer(self.config)
        self.transformer = DataTransformer(self.config)
        self.psf_matcher = PSFMatcher(self.config)

    def process_band_dict(self, band_cutouts, meta=None):
        meta = dict(meta or {})
        row = self._base_row(meta)
        bands = list(self.config.bands)

        missing = [b for b in bands if band_cutouts.get(b) is None]
        if missing and self.config.partial_band_policy == "drop":
            row.update(status="rejected", reject_reason=f"missing band(s): {missing}")
            return {"row": row, "chw": None}
        if missing and self.config.partial_band_policy == "raise":
            from ripple.preprocessing.exceptions import CutoutError
            raise CutoutError(f"missing band(s): {missing}")

        # 1. clean each band
        cleaned, fwhm = {}, {}
        worst_fraction = 0.0
        for b in bands:
            cut = band_cutouts.get(b)
            if cut is None:  # zero_fill_band policy
                cleaned[b] = (np.zeros((self.config.cutout_size,) * 2, np.float32),
                              np.zeros((self.config.cutout_size,) * 2, bool))
                continue
            cr = self.cleaner.clean(cut)
            if cr.status == "rejected":
                row.update(status="rejected", reject_reason=f"{b}: {cr.reject_reason}",
                           bad_fraction=cr.bad_fraction)
                return {"row": row, "chw": None}
            worst_fraction = max(worst_fraction, cr.bad_fraction)
            cleaned[b] = (cr.image, cr.valid_mask)
            f = self._fwhm_of(cut)
            if f is not None:
                fwhm[b] = f

        images = {b: cleaned[b][0] for b in bands}

        # 2. optional PSF match
        if self.config.psf_match_enabled:
            images = self.psf_matcher.match(images, band_fwhm=fwhm or None)
            row["psf_matched"] = bool(fwhm)

        # 3. normalize (mask-aware), 4. zero bad pixels
        norm_arrays = []
        for b in bands:
            valid = cleaned[b][1]
            out = self.normalizer.normalize_image(images[b], valid_mask=valid)
            out = np.where(valid, out, 0.0).astype(np.float32)
            norm_arrays.append(out)
        row["softening_estimator"] = self.normalizer.last_softening_estimator

        # 5. pad/crop to NxN, then channel adapt, then stack
        sized = [self.transformer.to_fixed_size(a, self.config.cutout_size) for a in norm_arrays]
        sized = self.transformer.adapt_channels(sized, self.config.channels)
        chw = self.transformer.stack_to_chw(sized)

        row.update(status="accepted", reject_reason=None, bad_fraction=worst_fraction,
                   size_px=self.config.cutout_size, channels=self.config.channels)
        return {"row": row, "chw": chw}

    def run(self, items, out_dir=None):
        rows, chws, accepted = [], [], []
        for enum_idx, item in enumerate(items):
            # Prefer the stable original coordinate position stored in
            # meta["index"] (set by the stage path) over the enumerate index.
            # This keeps manifest row["index"] == original coordinate position
            # even when some items were inserted for failed extractions.
            meta = item.get("meta") or {}
            idx = meta.get("index", enum_idx)
            try:
                res = self.process_band_dict(item.get("bands", {}), meta)
                row = res["row"]
                row["index"] = idx
                if res["chw"] is not None:
                    if out_dir is not None:
                        path = os.path.join(str(out_dir), f"cutout_{idx:06d}.npy")
                        np.save(path, res["chw"])
                        row["path"] = path
                    chws.append(res["chw"])
                    accepted.append(idx)
                rows.append(row)
            except Exception as exc:  # record failure, keep going
                rows.append({**self._base_row(meta), "index": idx,
                             "status": "failed", "reject_reason": str(exc)})
        if out_dir is not None:
            manifest_mod.write_manifest(rows, os.path.join(str(out_dir), "manifest.csv"))
        tensor = self.transformer.batch(chws) if chws else None
        return PreprocessResult(tensor=tensor, manifest=rows, accepted_indices=accepted)

    def _base_row(self, meta):
        return {
            "path": "", "label": meta.get("label"), "ra": meta.get("ra"),
            "dec": meta.get("dec"), "tract": meta.get("tract"), "patch": meta.get("patch"),
            "band_order": "-".join(self.config.bands), "channels": self.config.channels,
            "split": meta.get("split"), "group_key": meta.get("group_key"),
            "pixel_scale": self.config.pixel_scale, "norm_method": self.config.norm_method,
            "skymap_version": meta.get("skymap_version"), "dp1_version": meta.get("dp1_version"),
            "psf_matched": False,
        }

    def _fwhm_of(self, cutout):
        if hasattr(cutout, "getPsf"):
            try:
                psf = cutout.getPsf()
                if psf is not None:
                    sigma = psf.computeShape().getTraceRadius()
                    return float(sigma * 2.3548200450309493)
            except Exception:
                return None
        return None
