import numpy as np
import pytest
from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.data_cleaner import DataCleaner, CleanResult
from ripple.tests.preprocessing.conftest import make_fake_exposure


@pytest.fixture
def cleaner():
    return DataCleaner(PreprocessingConfig(max_bad_fraction=0.05))


def test_clean_bare_ndarray_finite_only(cleaner):
    a = np.ones((8, 8), dtype=np.float32)
    a[0, 0] = np.nan
    res = cleaner.clean(a)
    assert isinstance(res, CleanResult)
    assert res.valid_mask[0, 0] == False  # noqa: E712
    assert res.valid_mask.sum() == 63
    assert res.status == "accepted"


def test_clean_exposure_flags_mask_by_name(cleaner):
    exp = make_fake_exposure(bad_stripe=True, size=16)  # column 0 = NO_DATA
    res = cleaner.clean(exp)
    assert res.valid_mask[:, 0].sum() == 0  # whole NO_DATA column invalid
    assert res.bad_fraction > 0


def test_clean_excludes_informational_planes(cleaner):
    # DETECTED must NOT be treated as bad
    exp = make_fake_exposure(size=16)
    exp.mask.array[5, 5] |= exp.getMask().getPlaneBitMask(["DETECTED"])
    res = cleaner.clean(exp)
    assert res.valid_mask[5, 5] == True  # noqa: E712


def test_clean_combines_variance_nonpositive(cleaner):
    exp = make_fake_exposure(size=16)
    exp.variance.array[2, 2] = 0.0
    res = cleaner.clean(exp)
    assert res.valid_mask[2, 2] == False  # noqa: E712


def test_soft_reject_on_high_bad_fraction():
    cleaner = DataCleaner(PreprocessingConfig(max_bad_fraction=0.01))
    exp = make_fake_exposure(bad_stripe=True, size=16)  # ~6% bad
    res = cleaner.clean(exp)
    assert res.status == "rejected" and "bad_fraction" in res.reject_reason


def test_hard_reject_no_data_in_core():
    cleaner = DataCleaner(PreprocessingConfig(max_bad_fraction=1.0))
    img = np.ones((16, 16), dtype="<f4")
    exp = make_fake_exposure(image=img, size=16)
    c = 8
    exp.mask.array[c, c] |= exp.getMask().getPlaneBitMask(["NO_DATA"])
    res = cleaner.clean(exp)
    assert res.status == "rejected" and "core" in res.reject_reason


def test_qc_stats_present(cleaner):
    res = cleaner.clean(np.ones((8, 8), dtype=np.float32))
    assert {"bad_fraction", "snr", "shape"} <= set(res.qc)
