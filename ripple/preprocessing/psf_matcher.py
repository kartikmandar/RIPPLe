# ripple/preprocessing/psf_matcher.py
"""Cross-band PSF homogenization.

Default (gaussian) mode matches every band to the broadest-seeing band by convolving
with a Gaussian of sigma = sqrt(sigma_target^2 - sigma_band^2). A measured-kernel mode
uses photutils when measured PSF stamps are supplied. Runs only when per-band PSF/FWHM
information is available; otherwise warns and returns inputs unchanged. Default OFF.
"""
import warnings

import numpy as np

from ripple.preprocessing.exceptions import PSFMatchError


class PSFMatcher:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def _fwhm_to_sigma(fwhm):
        return float(fwhm) / 2.3548200450309493

    def match(self, band_images, band_fwhm=None, band_psfs=None):
        if not getattr(self.config, "psf_match_enabled", False):
            return band_images
        mode = getattr(self.config, "psf_match_mode", "gaussian")
        if mode == "measured" and band_psfs:
            return self._match_measured(band_images, band_psfs)
        if not band_fwhm:
            warnings.warn("PSF matching requested but no per-band PSF/FWHM available; "
                          "returning images unchanged.", stacklevel=2)
            return band_images
        return self._match_gaussian(band_images, band_fwhm)

    def _match_gaussian(self, band_images, band_fwhm):
        from scipy.ndimage import gaussian_filter
        bands = [b for b in band_images if b in band_fwhm]
        if not bands:
            warnings.warn("No overlapping bands between images and FWHM map; no-op.", stacklevel=2)
            return band_images
        target_fwhm = max(band_fwhm[b] for b in bands)
        sigma_t = self._fwhm_to_sigma(target_fwhm)
        out = {}
        for b, img in band_images.items():
            if b not in band_fwhm:
                out[b] = img
                continue
            sigma_b = self._fwhm_to_sigma(band_fwhm[b])
            delta2 = sigma_t ** 2 - sigma_b ** 2
            if delta2 <= 1e-6:
                out[b] = img  # already at/above target seeing
            else:
                out[b] = gaussian_filter(np.asarray(img, dtype=np.float32),
                                         sigma=np.sqrt(delta2)).astype(np.float32)
        return out

    def _match_measured(self, band_images, band_psfs):
        try:
            from photutils.psf.matching import create_matching_kernel, TopHatWindow
            from scipy.signal import fftconvolve
        except ImportError as exc:  # pragma: no cover
            raise PSFMatchError(f"measured PSF matching needs photutils/scipy: {exc}")
        target_band = max(band_psfs, key=lambda b: np.asarray(band_psfs[b]).sum())
        target = np.asarray(band_psfs[target_band], dtype=np.float32)
        out = {}
        for b, img in band_images.items():
            if b not in band_psfs or b == target_band:
                out[b] = img
                continue
            kernel = create_matching_kernel(np.asarray(band_psfs[b], dtype=np.float32),
                                            target, window=TopHatWindow(0.42))
            out[b] = fftconvolve(np.asarray(img, dtype=np.float32), kernel, mode="same").astype(np.float32)
        return out
