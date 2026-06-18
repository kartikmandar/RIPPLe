# ripple/tests/preprocessing/test_performance.py
import time

import numpy as np
import pytest

from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.preprocessor import Preprocessor


@pytest.mark.perf
def test_3band_64_default_path_under_budget():
    pp = Preprocessor(PreprocessingConfig(cutout_size=64, norm_method="asinh"))
    bands = {b: np.random.default_rng(0).normal(10, 1, (64, 64)).astype(np.float32)
             for b in ("g", "r", "i")}
    # warm up
    pp.process_band_dict(bands, meta={"ra": 1.0, "dec": 2.0})
    t0 = time.perf_counter()
    for _ in range(20):
        pp.process_band_dict(bands, meta={"ra": 1.0, "dec": 2.0})
    per = (time.perf_counter() - t0) / 20 * 1000.0  # ms
    print(f"\n3-band 64x64 default path: {per:.1f} ms/cutout (budget 60 ms)")
    assert per < 60.0, f"{per:.1f} ms exceeds the 60 ms/3-band budget"
