# ripple/preprocessing/image_normalizer.py
"""Release-independent image normalization for ML cutouts.

Default method is ``asinh`` (handles signed nJy fluxes); ``minmax`` (with robust
percentile clipping) and ``zscore`` are first-class options. Statistics are computed
over valid pixels only; inputs are never mutated.
"""
import numpy as np

from ripple.preprocessing.exceptions import NormalizationError


class ImageNormalizer:
    def __init__(self, config=None):
        self.config = config
        self.last_softening_estimator = None

    def _extract_array(self, array):
        if hasattr(array, "image") and hasattr(array.image, "array"):
            return array.image.array
        if hasattr(array, "array"):
            return np.asarray(array.array)
        if hasattr(array, "data"):
            return np.asarray(array.data)
        return np.asarray(array)

    def _resolve(self, method, params):
        cfg = self.config
        if method is None:
            method = getattr(cfg, "norm_method", "asinh") if cfg else "asinh"
        params = dict(params or {})
        if "clip_percentiles" not in params and cfg is not None:
            params["clip_percentiles"] = getattr(cfg, "clip_percentiles", (1.0, 99.0))
        params.setdefault("clip_percentiles", (1.0, 99.0))
        return method, params

    def normalize_image(self, array, method=None, params=None, valid_mask=None):
        raw = self._extract_array(array)
        data = np.array(raw, dtype=np.float32, copy=True)  # copy + float32
        method, params = self._resolve(method, params)
        if valid_mask is None:
            valid = np.isfinite(data)
        else:
            valid = np.asarray(valid_mask, dtype=bool) & np.isfinite(data)
        if not valid.any():
            return np.zeros_like(data)
        if method == "minmax":
            return self._minmax(data, valid, params)
        if method == "zscore":
            return self._zscore(data, valid)
        if method == "asinh":
            return self._asinh(data, valid, params)
        raise NormalizationError(f"Unknown normalization method: {method!r}")

    def _minmax(self, data, valid, params):
        lo_p, hi_p = params["clip_percentiles"]
        vals = data[valid]
        # Use method='lower' so a single extreme outlier cannot pull the
        # percentile boundary toward itself via interpolation (robust clipping).
        lo = np.percentile(vals, lo_p, method="lower")
        hi = np.percentile(vals, hi_p, method="lower")
        if hi <= lo:
            hi = lo + 1.0  # degenerate range guard
        out = (np.clip(data, lo, hi) - lo) / (hi - lo)
        return out.astype(np.float32, copy=False)

    def _zscore(self, data, valid):
        vals = data[valid]
        mean = float(vals.mean())
        std = float(vals.std())
        if std == 0.0:
            std = 1.0
        return ((data - mean) / std).astype(np.float32, copy=False)

    def _asinh(self, data, valid, params):
        soft = params.get("asinh_softening")
        if soft is None and self.config is not None:
            soft = getattr(self.config, "asinh_softening", None)
        if soft is None:
            soft = self._softening_fallback(data, valid, params)
        soft = max(float(soft), 1e-6)
        return np.arcsinh(data / soft).astype(np.float32, copy=False)

    def _softening_fallback(self, data, valid, params):
        """variance -> sqrt(median(var)) -> 1.4826*MAD -> sigma-clipped std -> constant."""
        var = params.get("variance")
        if var is not None:
            v = np.asarray(var)[valid]
            v = v[v > 0]
            if v.size:
                self.last_softening_estimator = "variance"
                return float(np.sqrt(np.median(v)))
        vals = data[valid]
        med = np.median(vals)
        mad = np.median(np.abs(vals - med))
        if mad > 0:
            self.last_softening_estimator = "mad"
            return float(1.4826 * mad)
        std = float(vals.std())
        if std > 0:
            self.last_softening_estimator = "sigma_clipped_std"
            return std
        self.last_softening_estimator = "config_constant"
        return 1.0
