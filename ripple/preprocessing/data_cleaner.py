"""Bad-pixel identification and quality control for cutouts.

On an ExposureF, builds a bad-pixel map from named mask planes (never bit numbers),
combined with finite and positive-variance checks. On a bare ndarray (the RSP path
with no mask/variance), degrades to a finite-only check.
"""
from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class CleanResult:
    image: np.ndarray
    valid_mask: np.ndarray
    bad_fraction: float
    status: str
    reject_reason: Optional[str]
    qc: dict


class DataCleaner:
    def __init__(self, config):
        self.config = config

    def clean(self, cutout) -> CleanResult:
        image = self._image_array(cutout)
        valid = np.isfinite(image)
        bad_planes_hit_core = False

        mask_arr = self._mask_array(cutout)
        if mask_arr is not None:
            bitmask = self._bad_bitmask(cutout)
            bad_from_mask = (mask_arr & bitmask) != 0
            valid &= ~bad_from_mask
            bad_planes_hit_core = self._core_hit(cutout, mask_arr)

        var_arr = self._variance_array(cutout)
        if var_arr is not None:
            valid &= (var_arr > 0)

        bad_fraction = float((~valid).mean())
        status, reason = self._verdict(bad_fraction, bad_planes_hit_core)
        qc = {
            "bad_fraction": bad_fraction,
            "snr": self._snr(image, valid),
            "shape": tuple(image.shape),
        }
        return CleanResult(image=image, valid_mask=valid, bad_fraction=bad_fraction,
                           status=status, reject_reason=reason, qc=qc)

    # ---- extractors (duck typing over ExposureF / wrapper / ndarray) ----
    def _image_array(self, c):
        # np.array(...) forces an owned copy so CleanResult.image never aliases
        # the caller's backing buffer — satisfying the copy-semantics contract.
        if hasattr(c, "image") and hasattr(c.image, "array"):
            return np.array(c.image.array)
        if hasattr(c, "array"):
            return np.array(c.array)
        if hasattr(c, "data"):
            return np.array(c.data)
        return np.array(c)

    def _mask_array(self, c):
        if hasattr(c, "mask") and hasattr(c.mask, "array"):
            return np.asarray(c.mask.array)
        return None

    def _variance_array(self, c):
        if hasattr(c, "variance") and hasattr(c.variance, "array"):
            return np.asarray(c.variance.array)
        return None

    def _bad_bitmask(self, c):
        names = list(getattr(self.config, "bad_mask_planes",
                             ("NO_DATA", "BAD", "SAT", "UNMASKEDNAN", "SENSOR_EDGE", "EDGE")))
        getmask = c.getMask() if hasattr(c, "getMask") else c.mask
        present = set(getmask.getMaskPlaneDict().keys()) if hasattr(getmask, "getMaskPlaneDict") else None
        if present is not None:
            names = [n for n in names if n in present]
        return getmask.getPlaneBitMask(names) if names else 0

    def _core_hit(self, c, mask_arr):
        """True if a hard plane (NO_DATA/EDGE/SENSOR_EDGE) lands in the central quarter."""
        getmask = c.getMask() if hasattr(c, "getMask") else c.mask
        present = set(getmask.getMaskPlaneDict().keys()) if hasattr(getmask, "getMaskPlaneDict") else set()
        hard = [n for n in ("NO_DATA", "EDGE", "SENSOR_EDGE") if n in present]
        if not hard:
            return False
        bit = getmask.getPlaneBitMask(hard)
        h, w = mask_arr.shape
        cy0, cy1 = h // 4, 3 * h // 4
        cx0, cx1 = w // 4, 3 * w // 4
        core = mask_arr[cy0:cy1, cx0:cx1]
        return bool(((core & bit) != 0).any())

    def _verdict(self, bad_fraction, core_hit):
        if core_hit:
            return "rejected", "hard: bad plane in core"
        max_bad = getattr(self.config, "max_bad_fraction", 0.05)
        if bad_fraction > max_bad:
            return "rejected", f"soft: bad_fraction {bad_fraction:.3f} > {max_bad}"
        return "accepted", None

    def _snr(self, image, valid):
        vals = image[valid]
        if vals.size == 0:
            return 0.0
        med = float(np.median(vals))
        mad = float(np.median(np.abs(vals - med)))
        sigma = 1.4826 * mad if mad > 0 else float(vals.std())
        return float(med / sigma) if sigma > 0 else 0.0
