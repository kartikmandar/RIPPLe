# ripple/tests/preprocessing/test_psf_matcher.py
import numpy as np
import pytest
from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.psf_matcher import PSFMatcher


def _gaussian_point(size=33, sigma=2.0):
    y, x = np.mgrid[0:size, 0:size]
    c = size // 2
    g = np.exp(-(((x - c) ** 2 + (y - c) ** 2) / (2 * sigma ** 2)))
    return (g / g.sum()).astype(np.float32)


@pytest.fixture
def matcher():
    return PSFMatcher(PreprocessingConfig(psf_match_enabled=True, psf_match_mode="gaussian"))


def test_fwhm_to_sigma():
    assert PSFMatcher._fwhm_to_sigma(2.3548200450309493) == pytest.approx(1.0, rel=1e-4)


def test_disabled_returns_inputs_unchanged():
    m = PSFMatcher(PreprocessingConfig(psf_match_enabled=False))
    imgs = {"g": np.ones((8, 8), np.float32)}
    out = m.match(imgs, band_fwhm={"g": 3.0})
    assert out["g"] is imgs["g"]


def test_no_fwhm_info_warns_and_noops(matcher, recwarn):
    imgs = {"g": np.ones((8, 8), np.float32), "r": np.ones((8, 8), np.float32)}
    out = matcher.match(imgs, band_fwhm=None)
    assert set(out) == {"g", "r"}
    assert any("PSF" in str(w.message) for w in recwarn.list)


def test_gaussian_broadens_sharp_band_to_target(matcher):
    # sharp band (sigma 1.5) should be broadened toward broad band (sigma 3.0)
    sharp = _gaussian_point(sigma=1.5)
    broad = _gaussian_point(sigma=3.0)
    out = matcher.match({"g": sharp, "r": broad}, band_fwhm={"g": 1.5 * 2.3548, "r": 3.0 * 2.3548})
    # broadest band unchanged; sharp band's peak reduced (energy spread out)
    assert out["r"].max() == pytest.approx(broad.max(), rel=1e-5)
    assert out["g"].max() < sharp.max()
    assert out["g"].sum() == pytest.approx(sharp.sum(), rel=1e-2)  # flux conserved


def test_target_band_is_unchanged_shape(matcher):
    imgs = {"g": _gaussian_point(sigma=1.5), "r": _gaussian_point(sigma=3.0)}
    out = matcher.match(imgs, band_fwhm={"g": 3.53, "r": 7.06})
    assert out["g"].shape == imgs["g"].shape
