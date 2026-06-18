from unittest.mock import MagicMock
import numpy as np
import pytest
from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.cutout_creator import CutoutCreator


@pytest.fixture
def fetcher():
    f = MagicMock()
    f.get_multi_band_cutout.return_value = {
        "g": np.ones((64, 64), np.float32),
        "r": np.ones((64, 64), np.float32),
        "i": None,
    }
    return f


def test_create_passes_config_params(fetcher):
    cc = CutoutCreator(fetcher, PreprocessingConfig(size_arcsec=12.8, bands=("g", "r", "i")))
    out = cc.create(10.0, -30.0)
    fetcher.get_multi_band_cutout.assert_called_once()
    kwargs = fetcher.get_multi_band_cutout.call_args.kwargs
    assert kwargs["size_arcsec"] == 12.8
    assert list(kwargs["bands"]) == ["g", "r", "i"]
    assert set(out) == {"g", "r", "i"}


def test_create_many_returns_one_dict_per_coord(fetcher):
    cc = CutoutCreator(fetcher, PreprocessingConfig())
    out = cc.create_many([(1.0, 2.0), (3.0, 4.0)])
    assert len(out) == 2 and isinstance(out[0], dict)
