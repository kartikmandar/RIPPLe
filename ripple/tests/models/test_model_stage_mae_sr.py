"""Regression test: mae_vit_sr routes through ModelStage's image-out writer.

Verifies that a factory-built MAEViTSR model (OUTPUT_KIND="image") is
dispatched to _run_super_resolution, which writes enhanced_*.npy cutouts
and enhanced_manifest.csv under out_dir, and sets data["enhanced_manifest"].

ModelStage.execute() reads from the data dict:
    data["tensor"]               — (N, C, H, W) array/tensor of accepted cutouts
    data["preprocess_manifest"]  — list of row dicts; rows with status="accepted"
                                   are joined to the SR outputs in order
    data["out_dir"]              — directory where artifacts are written

It calls:  model.predict(data={"tensor": tensor}, return_image=True)
So MAEViTSR.predict must accept a dict input and the return_image kwarg.
"""
import os
import pytest

pytest.importorskip("torch")
import numpy as np

pytestmark = pytest.mark.torch


def test_mae_sr_writes_enhanced_cutouts(tmp_path, monkeypatch):
    import ripple.pipeline.stages.model_stage as ms_mod
    from ripple.models.model_factory import ModelFactory

    # Build the real mae_vit_sr model; monkeypatch the factory on the stage
    # module so the stage picks up our instance instead of calling create().
    model = ModelFactory.create(
        "mae_vit_sr",
        {"encoder": "mae_vit_tiny", "task": "super_res"},
    )

    # Patch the ModelFactory name imported inside model_stage so the stage's
    # `model = ModelFactory.create(...)` call returns our pre-built instance.
    class _FixedFactory:
        @classmethod
        def create(cls, model_type, config=None):
            return model

    monkeypatch.setattr(ms_mod, "ModelFactory", _FixedFactory)

    stage = ms_mod.ModelStage(
        config={"model": {"type": "mae_vit_sr", "operation": "prediction"}}
    )

    # data dict matches the keys the stage reads (see _run_prediction /
    # _run_super_resolution / _resolve_out_dir / _accepted_rows).
    data = {
        "tensor": np.random.randn(3, 3, 64, 64).astype("float32"),
        "preprocess_manifest": [
            {"status": "accepted", "index": i} for i in range(3)
        ],
        "out_dir": str(tmp_path),
    }

    out = stage.execute(data)

    # Stage must set the enhanced_manifest key on the returned dict.
    assert "enhanced_manifest" in out, (
        "ModelStage did not set data['enhanced_manifest']; "
        "mae_vit_sr may not be routing through _run_super_resolution"
    )

    # enhanced_manifest.csv must be written to out_dir.
    csv_path = os.path.join(str(tmp_path), "enhanced_manifest.csv")
    assert os.path.exists(csv_path), (
        f"enhanced_manifest.csv not found at {csv_path}"
    )

    # One enhanced .npy file per accepted cutout.
    for i in range(3):
        npy_path = os.path.join(str(tmp_path), f"enhanced_{i:06d}.npy")
        assert os.path.exists(npy_path), f"Missing enhanced cutout: {npy_path}"

    # SR branch must NOT produce a catalog predictions.csv.
    assert not os.path.exists(os.path.join(str(tmp_path), "predictions.csv"))
