"""Stack-free test doubles for LSST ExposureF objects."""
import numpy as np
import pytest

# Canonical-ish mask plane bits (names are what matters; values are arbitrary but stable).
_MASK_PLANES = {
    "BAD": 0, "SAT": 1, "INTRP": 2, "CR": 3, "EDGE": 4, "DETECTED": 5,
    "DETECTED_NEGATIVE": 6, "SUSPECT": 7, "NO_DATA": 8, "UNMASKEDNAN": 9,
    "SENSOR_EDGE": 10, "CLIPPED": 11, "VIGNETTED": 12,
}


class _Plane:
    def __init__(self, array):
        self.array = array


class _Mask:
    def __init__(self, array):
        self.array = array

    def getPlaneBitMask(self, names):
        if isinstance(names, str):
            names = [names]
        bitmask = 0
        for n in names:
            bitmask |= (1 << _MASK_PLANES[n])
        return bitmask

    def getMaskPlaneDict(self):
        return dict(_MASK_PLANES)


class _Psf:
    def __init__(self, fwhm):
        self._fwhm = fwhm

    def computeShape(self, *args, **kwargs):
        # afw returns a Quadrupole; expose getTraceRadius() -> sigma in pixels.
        sigma = self._fwhm / 2.3548200450309493

        class _Shape:
            def getTraceRadius(_self):
                return sigma
        return _Shape()


class FakeExposure:
    """Duck-typed stand-in for lsst.afw.image.ExposureF (numpy-backed)."""
    def __init__(self, image, mask, variance, fwhm):
        self.image = _Plane(image)
        self._mask = _Mask(mask)
        self.mask = self._mask
        self.variance = _Plane(variance)
        self._psf = _Psf(fwhm)

    def getMask(self):
        return self._mask

    def getWcs(self):
        return None  # WCS not needed by the cleaner/normalizer paths under test

    def getPsf(self):
        return self._psf


def make_fake_exposure(image=None, bad_stripe=False, sat_core=False, nan=False,
                       fwhm=3.0, dtype="<f4", size=64):
    if image is None:
        rng = np.random.default_rng(0)
        image = rng.normal(10.0, 1.0, size=(size, size)).astype(dtype)
    else:
        image = np.asarray(image, dtype=dtype)
    h, w = image.shape
    mask = np.zeros((h, w), dtype=np.int32)
    variance = np.ones((h, w), dtype=np.float32)
    if bad_stripe:
        mask[:, 0] |= (1 << _MASK_PLANES["NO_DATA"])
    if sat_core:
        c = h // 2
        mask[c - 1:c + 1, c - 1:c + 1] |= (1 << _MASK_PLANES["SAT"])
        image = image.copy()
        image[c - 1:c + 1, c - 1:c + 1] = 1e6
    if nan:
        image = image.copy()
        image[0, 0] = np.nan
    return FakeExposure(image, mask, variance, fwhm)


@pytest.fixture
def fake_exposure():
    return make_fake_exposure()


# ---- smoke test that the double behaves like afw expects ----
def test_fake_exposure_smoke():
    exp = make_fake_exposure(bad_stripe=True, fwhm=3.0)
    bit = exp.getMask().getPlaneBitMask(["NO_DATA"])
    assert (exp.mask.array & bit).any()
    assert "SENSOR_EDGE" in exp.getMask().getMaskPlaneDict()
    assert exp.getPsf().computeShape().getTraceRadius() > 0
    assert exp.image.array.dtype.byteorder in ("<", "=", "|") or exp.image.array.dtype.str == "<f4"
