"""Typed configuration for the RIPPLe preprocessing pipeline."""
import warnings
from dataclasses import dataclass, fields
from typing import Optional, Tuple

_NORM_METHODS = {"asinh", "minmax", "zscore"}
_RESIZE_MODES = {"pad_crop", "interpolate"}
_PARTIAL_POLICIES = {"drop", "zero_fill_band", "raise"}
_PSF_MODES = {"gaussian", "measured"}


@dataclass(frozen=True)
class PreprocessingConfig:
    """Immutable preprocessing settings. See the Phase-2 design spec.

    Attributes:
        cutout_size: Output side length in pixels (square).
        bands: Ordered bands to fetch/stack.
        size_arcsec: Angular cutout size; with ``pixel_scale`` implies ``cutout_size``.
        pixel_scale: Arcsec per pixel (DP1 ~ 0.2).
        channels: Output channel count (1 replicated to 3, or band subset selected).
        norm_method: ``'asinh'`` (default) | ``'minmax'`` | ``'zscore'``.
        clip_percentiles: Robust percentile clip used by ``minmax``.
        asinh_softening: Fixed softening; ``None`` triggers the noise-aware fallback chain.
        norm_scope: ``'per_image'`` (default) or ``'fixed'`` (survey-level ``norm_params``).
        norm_params: Fixed survey-level normalization parameters.
        bad_mask_planes: LSST mask plane *names* (never bit numbers) treated as bad.
        resize_mode: ``'pad_crop'`` (default) or ``'interpolate'``.
        partial_band_policy: Behavior when a requested band is missing.
        max_bad_fraction: Soft-reject threshold on bad-pixel fraction.
        psf_match_enabled: Enable PSF homogenization (default off).
        psf_match_mode: ``'gaussian'`` (default) or ``'measured'``.
    """
    cutout_size: int = 64
    bands: Tuple[str, ...] = ("g", "r", "i")
    size_arcsec: float = 12.8
    pixel_scale: float = 0.2
    channels: int = 3
    norm_method: str = "asinh"
    clip_percentiles: Tuple[float, float] = (1.0, 99.0)
    asinh_softening: Optional[float] = None
    norm_scope: str = "per_image"
    norm_params: Optional[dict] = None
    bad_mask_planes: Tuple[str, ...] = ("NO_DATA", "BAD", "SAT", "UNMASKEDNAN", "SENSOR_EDGE", "EDGE")
    resize_mode: str = "pad_crop"
    partial_band_policy: str = "drop"
    max_bad_fraction: float = 0.05
    psf_match_enabled: bool = False
    psf_match_mode: str = "gaussian"

    def __post_init__(self):
        if self.norm_method not in _NORM_METHODS:
            raise ValueError(f"norm_method must be one of {_NORM_METHODS}, got {self.norm_method!r}")
        if self.resize_mode not in _RESIZE_MODES:
            raise ValueError(f"resize_mode must be one of {_RESIZE_MODES}, got {self.resize_mode!r}")
        if self.partial_band_policy not in _PARTIAL_POLICIES:
            raise ValueError(f"partial_band_policy must be one of {_PARTIAL_POLICIES}")
        if self.psf_match_mode not in _PSF_MODES:
            raise ValueError(f"psf_match_mode must be one of {_PSF_MODES}")
        if self.cutout_size <= 0:
            raise ValueError("cutout_size must be positive")
        implied = round(self.size_arcsec / self.pixel_scale)
        if implied != self.cutout_size:
            warnings.warn(
                f"cutout_size={self.cutout_size} != round(size_arcsec/pixel_scale)={implied}; "
                "they will diverge — set size_arcsec/pixel_scale to match.",
                stacklevel=2,
            )

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "PreprocessingConfig":
        """Build from a dict, ignoring unknown keys (tuples coerced for list inputs)."""
        if not d:
            return cls()
        known = {f.name for f in fields(cls)}
        kwargs = {}
        for k, v in d.items():
            if k not in known:
                continue
            if k in ("bands", "bad_mask_planes", "clip_percentiles") and isinstance(v, list):
                v = tuple(v)
            kwargs[k] = v
        return cls(**kwargs)
