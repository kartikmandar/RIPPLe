"""PreprocessingStage must surface accepted_indices in its return dict."""
from unittest.mock import MagicMock

import numpy as np

from ripple.pipeline.stages.preprocessing_stage import PreprocessingStage


def _band_dict(size=16):
    return {b: np.ones((size, size), np.float32) for b in ("g", "r", "i")}


def test_create_cutouts_returns_accepted_indices(monkeypatch):
    stage = PreprocessingStage.__new__(PreprocessingStage)
    stage.config = {}
    stage.preprocessing_steps = ["cutout_creation"]
    stage.preprocessing_params = {}
    stage.cutout_saver = None

    # Data fetcher and CutoutCreator are stubbed: every coordinate "succeeds".
    stage.data_fetcher = MagicMock()

    import ripple.pipeline.stages.preprocessing_stage as ps_mod

    class _FakeCreator:
        def __init__(self, fetcher, config):
            pass

        def create(self, ra, dec):
            return _band_dict()

    monkeypatch.setattr(ps_mod, "CutoutCreator", _FakeCreator)

    data = {
        "coordinates": [
            {"ra": 1.0, "dec": 2.0, "label": "a"},
            {"ra": 3.0, "dec": 4.0, "label": "b"},
        ]
    }
    out = stage._create_cutouts(data, {})

    assert "accepted_indices" in out
    assert out["accepted_indices"] == [0, 1]
    # The manifest still has exactly one row per requested coordinate.
    assert len(out["preprocess_manifest"]) == 2
