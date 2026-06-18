import pytest
from ripple.preprocessing.config import PreprocessingConfig


def test_defaults_match_contract():
    c = PreprocessingConfig()
    assert c.cutout_size == 64
    assert c.bands == ("g", "r", "i")
    assert c.channels == 3
    assert c.norm_method == "asinh"
    assert c.resize_mode == "pad_crop"
    assert c.psf_match_enabled is False
    assert "NO_DATA" in c.bad_mask_planes


def test_frozen_is_immutable():
    c = PreprocessingConfig()
    with pytest.raises(AttributeError):
        c.cutout_size = 128


def test_from_dict_overrides_and_ignores_unknown():
    c = PreprocessingConfig.from_dict({"norm_method": "minmax", "cutout_size": 128, "junk": 1})
    assert c.norm_method == "minmax" and c.cutout_size == 128


def test_from_dict_none_returns_defaults():
    assert PreprocessingConfig.from_dict(None) == PreprocessingConfig()


def test_invalid_norm_method_raises():
    with pytest.raises(ValueError):
        PreprocessingConfig(norm_method="bogus")


def test_invalid_resize_mode_raises():
    with pytest.raises(ValueError):
        PreprocessingConfig(resize_mode="warp")


def test_size_arcsec_pixel_scale_consistency_warns(recwarn):
    # 12.8 / 0.2 == 64 -> no warning; mismatch -> warning, not error
    PreprocessingConfig(size_arcsec=10.0, pixel_scale=0.2, cutout_size=64)
    assert any("cutout_size" in str(w.message) for w in recwarn.list)


def test_new_exceptions_subclass_preprocessing_error():
    from ripple.preprocessing.exceptions import (
        PreprocessingError, CleaningError, PSFMatchError, ManifestError)
    for exc in (CleaningError, PSFMatchError, ManifestError):
        assert issubclass(exc, PreprocessingError)
