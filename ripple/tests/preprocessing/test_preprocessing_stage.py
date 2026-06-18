"""Tests for the rewired PreprocessingStage adapter.

These exercise three structural goals of the Phase-2 rewire:
  1. The RGB channel mapping is canonical (i->Red, r->Green, g->Blue).
  2. The local-Butler path is reachable (not gated solely on rsp_tap_client).
  3. The stage delegates cutout->tensor work to Preprocessor with a SINGLE
     extraction, returning the tensor/manifest in the data dict.
"""
from unittest.mock import MagicMock

import numpy as np
import pytest

from ripple.pipeline.stages.preprocessing_stage import PreprocessingStage


def _band_dict(size=16):
    return {b: np.ones((size, size), np.float32) for b in ("g", "r", "i")}


def test_rgb_canonical_mapping():
    # i -> Red(0), r -> Green(1), g -> Blue(2): conventional astronomical gri.
    from ripple.utils.cutout_saver import CutoutSaver

    assert CutoutSaver.RGB_BAND_ORDER == ("i", "r", "g")


def test_rgb_composite_uses_canonical_band_order():
    # Build distinct per-band PATTERNS so each output channel can be traced
    # back to its source band after per-band [0,1] normalization; verify
    # Red<-i, Green<-r, Blue<-g.
    from ripple.utils.cutout_saver import CutoutSaver

    saver = CutoutSaver.__new__(CutoutSaver)  # avoid touching the filesystem
    ramp = np.arange(16, dtype=np.float32).reshape(4, 4)
    cutouts = {
        "i": ramp,              # ascending ramp -> normalizes to itself
        "r": ramp[::-1, ::-1],  # reversed ramp  -> distinct pattern
        "g": (ramp % 5),        # third distinct pattern
    }
    # Per-band [0,1] normalization the saver applies, computed independently.
    norm = {}
    for band, arr in cutouts.items():
        a = arr.astype(float)
        a = np.clip(a, np.percentile(a, 1), np.percentile(a, 99))
        norm[band] = (a - a.min()) / (a.max() - a.min())

    rgb = saver._create_rgb_composite(cutouts)
    assert rgb is not None and rgb.shape == (4, 4, 3)

    # Red=i, Green=r, Blue=g per RGB_BAND_ORDER.
    assert np.allclose(rgb[:, :, 0], norm["i"])
    assert np.allclose(rgb[:, :, 1], norm["r"])
    assert np.allclose(rgb[:, :, 2], norm["g"])


def test_stage_runs_preprocessor_on_local_butler(monkeypatch, tmp_path):
    # Local-Butler path must be reachable (not gated on rsp_tap_client) and
    # must reach Preprocessor with exactly ONE extraction per coordinate.
    stage = PreprocessingStage(
        {
            "processing": {"steps": ["cutout_creation"], "params": {}},
            "output": {"save_cutouts": False},
        }
    )

    fake_fetcher = MagicMock()
    fake_fetcher.get_multi_band_cutout.return_value = _band_dict()
    monkeypatch.setattr(stage, "_build_data_fetcher", lambda data: fake_fetcher)

    data = {
        "data_source_config": {"type": "butler_repo"},
        "coordinates": [{"ra": 10.0, "dec": -30.0}],
    }
    out = stage.execute(data)

    assert out is not None
    assert isinstance(out, dict)
    assert "preprocess_manifest" in out or "tensor" in out
    # Exactly one extraction for the single coordinate (no double-extraction).
    assert fake_fetcher.get_multi_band_cutout.call_count == 1


def test_stage_reads_coordinates_from_extraction_config(monkeypatch, tmp_path):
    # Coordinates may arrive nested under data_source_config.extraction
    # (the DataSourceStage contract); both shapes must reach Preprocessor.
    stage = PreprocessingStage(
        {
            "processing": {"steps": ["cutout_creation"], "params": {}},
            "output": {"save_cutouts": False},
        }
    )

    fake_fetcher = MagicMock()
    fake_fetcher.get_multi_band_cutout.return_value = _band_dict()
    monkeypatch.setattr(stage, "_build_data_fetcher", lambda data: fake_fetcher)

    data = {
        "data_source_config": {
            "type": "butler_repo",
            "extraction": {
                "coordinates": [
                    {"ra": 10.0, "dec": -30.0},
                    {"ra": 11.0, "dec": -31.0},
                ]
            },
        }
    }
    out = stage.execute(data)

    assert isinstance(out, dict)
    assert "preprocess_manifest" in out
    # Two coordinates => exactly two extractions, no duplication.
    assert fake_fetcher.get_multi_band_cutout.call_count == 2
    assert len(out["preprocess_manifest"]) == 2


def test_process_rgb_composites_uses_canonical_default_mapping():
    # The in-memory RGB path (rgb_composite step, save_cutouts: False) must
    # default to the canonical mapping i->R, r->G, g->B -- never the old
    # inverted r->R, g->G, i->B default. Use distinct per-band patterns and
    # trace which band's pattern lands in the Red channel.
    from ripple.utils.cutout_saver import CutoutSaver

    stage = PreprocessingStage(
        {
            "processing": {"steps": ["rgb_composite"], "params": {}},
            "output": {"save_cutouts": False},
        }
    )

    # Wrap arrays so they hit the ``hasattr(cutout, 'array')`` branch of
    # _create_single_rgb_composite (the exposure-like contract), keeping this
    # test focused on the mapping default rather than ndarray extraction.
    class _Arr:
        def __init__(self, array):
            self.array = array

    ramp = np.arange(16, dtype=np.float32).reshape(4, 4)
    bands = {
        "i": ramp,              # ascending ramp -> Red channel
        "r": ramp[::-1, ::-1],  # reversed ramp  -> Green channel
        "g": (ramp % 5),        # third pattern   -> Blue channel
    }
    cutout = {band: _Arr(arr) for band, arr in bands.items()}
    data = {
        "extraction_results": [
            {"ra": 1.0, "dec": 2.0, "label": "x", "status": "success",
             "cutout": cutout},
        ]
    }

    out = stage._process_rgb_composites(data)

    rgb = out["extraction_results"][0]["rgb_composite"]
    assert rgb is not None and rgb.shape == (4, 4, 3)

    # Reproduce the per-channel stretch+clip the stage applies (asinh + [1,99]).
    def _stretched(arr):
        a = np.arcsinh(arr.astype(np.float32))
        vmin, vmax = np.percentile(a[~np.isnan(a)], [1, 99])
        return np.clip((a - vmin) / (vmax - vmin), 0, 1)

    assert np.allclose(rgb[:, :, 0], _stretched(bands["i"]))  # Red <- i
    assert np.allclose(rgb[:, :, 1], _stretched(bands["r"]))  # Green <- r
    assert np.allclose(rgb[:, :, 2], _stretched(bands["g"]))  # Blue <- g

    # And the default must match CutoutSaver.RGB_BAND_ORDER.
    assert CutoutSaver.RGB_BAND_ORDER == ("i", "r", "g")
