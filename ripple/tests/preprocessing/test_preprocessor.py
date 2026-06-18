# ripple/tests/preprocessing/test_preprocessor.py
import numpy as np
import pytest
from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.preprocessor import Preprocessor, PreprocessResult


def _bands(value=1.0, size=64, dtype=np.float32):
    return {b: np.full((size, size), value, dtype=dtype) for b in ("g", "r", "i")}


def test_process_band_dict_returns_chw_and_row():
    pp = Preprocessor(PreprocessingConfig(cutout_size=64))
    res = pp.process_band_dict(_bands(), meta={"ra": 1.0, "dec": 2.0, "label": 1})
    assert res["row"]["status"] == "accepted"
    assert res["chw"].shape == (3, 64, 64) and res["chw"].dtype == np.float32


def test_partial_band_drop_policy_rejects():
    pp = Preprocessor(PreprocessingConfig(partial_band_policy="drop"))
    bands = _bands()
    bands["i"] = None
    res = pp.process_band_dict(bands, meta={"ra": 1.0, "dec": 2.0})
    assert res["row"]["status"] == "rejected"
    assert "band" in res["row"]["reject_reason"]
    assert res["chw"] is None


def test_ragged_and_big_endian_inputs_normalized():
    pp = Preprocessor(PreprocessingConfig(cutout_size=64))
    bands = {"g": np.ones((58, 61), ">f4"), "r": np.ones((64, 64), ">f4"),
             "i": np.ones((70, 70), ">f4")}
    res = pp.process_band_dict(bands, meta={"ra": 1.0, "dec": 2.0})
    assert res["chw"].shape == (3, 64, 64)
    assert res["chw"].dtype == np.float32


@pytest.mark.torch
def test_run_builds_dense_batch_from_accepted_only(tmp_path):
    pytest.importorskip("torch")
    pp = Preprocessor(PreprocessingConfig(cutout_size=16, partial_band_policy="drop"))
    good = {"bands": _bands(size=16), "meta": {"ra": 1.0, "dec": 2.0, "label": 1}}
    bad = {"bands": {**_bands(size=16), "i": None}, "meta": {"ra": 3.0, "dec": 4.0, "label": 0}}
    result = pp.run([good, bad, good], out_dir=tmp_path)
    assert isinstance(result, PreprocessResult)
    assert result.tensor.shape == (2, 3, 16, 16)            # only 2 accepted
    assert result.accepted_indices == [0, 2]
    assert len(result.manifest) == 3                         # all coords recorded
    assert result.manifest[1]["status"] == "rejected"
    # manifest index aligns with original item order
    assert [r["index"] for r in result.manifest] == [0, 1, 2]


def test_run_writes_manifest_and_npy(tmp_path):
    pp = Preprocessor(PreprocessingConfig(cutout_size=16))
    pp.run([{"bands": _bands(size=16), "meta": {"ra": 1.0, "dec": 2.0}}], out_dir=tmp_path)
    assert (tmp_path / "manifest.csv").exists()
    assert list(tmp_path.glob("*.npy"))
