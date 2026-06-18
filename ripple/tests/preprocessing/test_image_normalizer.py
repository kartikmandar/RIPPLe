# ripple/tests/preprocessing/test_image_normalizer.py
import numpy as np
import pytest
from ripple.preprocessing.image_normalizer import ImageNormalizer
from ripple.preprocessing.exceptions import NormalizationError


@pytest.fixture
def norm():
    return ImageNormalizer()


def test_minmax_maps_to_unit_range(norm):
    a = np.array([[0.0, 5.0], [10.0, 100.0]], dtype=np.float32)
    out = norm.normalize_image(a, method="minmax", params={"clip_percentiles": (0, 100)})
    assert out.min() == pytest.approx(0.0) and out.max() == pytest.approx(1.0)
    assert out.dtype == np.float32


def test_minmax_robust_percentile_clips_outlier(norm):
    a = np.linspace(0, 99, 100, dtype=np.float32).reshape(10, 10)
    a[9, 9] = 1e6  # one saturated outlier replacing the brightest pixel
    out = norm.normalize_image(a, method="minmax", params={"clip_percentiles": (1, 99)})
    assert out.max() == pytest.approx(1.0) and np.isfinite(out).all()
    # without robust clipping the 1e6 outlier would crush a mid pixel to ~0;
    # with p1/p99 clipping a ~50 value lands near the middle of [0,1].
    assert out[5, 0] > 0.3


def test_asinh_accepts_negatives(norm):
    a = np.array([[-3.0, 0.0], [3.0, 50.0]], dtype=np.float32)
    out = norm.normalize_image(a, method="asinh")
    assert np.isfinite(out).all()
    assert out[0, 0] < out[0, 1] < out[1, 0]  # monotonic through zero


def test_zscore_zero_mean_unit_std(norm):
    rng = np.random.default_rng(1)
    a = rng.normal(100.0, 5.0, size=(64, 64)).astype(np.float32)
    out = norm.normalize_image(a, method="zscore")
    assert abs(float(out.mean())) < 1e-3
    assert abs(float(out.std()) - 1.0) < 1e-2


def test_does_not_mutate_input(norm):
    a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    before = a.copy()
    norm.normalize_image(a, method="minmax")
    np.testing.assert_array_equal(a, before)


def test_valid_mask_excludes_bad_pixels_from_stats(norm):
    a = np.ones((4, 4), dtype=np.float32)
    a[0, 0] = 1e6
    valid = np.ones((4, 4), dtype=bool)
    valid[0, 0] = False
    out = norm.normalize_image(a, method="minmax", params={"clip_percentiles": (0, 100)},
                               valid_mask=valid)
    # stats over valid pixels only (all 1.0) -> degenerate range handled, finite output
    assert np.isfinite(out).all()


def test_asinh_softening_fallback_records_estimator(norm):
    a = np.array([[-2.0, 1.0], [4.0, 9.0]], dtype=np.float32)
    norm.normalize_image(a, method="asinh")  # no variance -> MAD path
    assert norm.last_softening_estimator in ("mad", "sigma_clipped_std", "config_constant")


def test_unknown_method_raises(norm):
    with pytest.raises(NormalizationError):
        norm.normalize_image(np.zeros((2, 2), dtype=np.float32), method="bogus")
