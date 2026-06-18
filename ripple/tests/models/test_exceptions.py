"""Tests for the RIPPLe models exception hierarchy."""
import pytest

import ripple.models.exceptions as exc


def test_existing_exceptions_present():
    assert issubclass(exc.ModelConfigError, exc.ModelError)
    assert issubclass(exc.ModelTrainingError, exc.ModelError)
    assert issubclass(exc.ModelError, Exception)


@pytest.mark.parametrize(
    "name",
    ["ModelLoadError", "ModelPredictionError", "ModelInferenceError", "CheckpointError"],
)
def test_new_exceptions_subclass_model_error(name):
    cls = getattr(exc, name)
    assert issubclass(cls, exc.ModelError)
    assert issubclass(cls, Exception)


def test_new_exceptions_raisable_and_catchable_as_model_error():
    for cls in (
        exc.ModelLoadError,
        exc.ModelPredictionError,
        exc.ModelInferenceError,
        exc.CheckpointError,
    ):
        with pytest.raises(exc.ModelError):
            raise cls("boom")


def test_distinct_classes():
    names = [
        exc.ModelLoadError,
        exc.ModelPredictionError,
        exc.ModelInferenceError,
        exc.CheckpointError,
    ]
    assert len(set(names)) == 4
