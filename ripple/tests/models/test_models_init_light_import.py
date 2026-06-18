"""ripple.models must expose a light, torch-free public surface (offline)."""
import inspect
import subprocess
import sys

import ripple.models as rm


def test_light_symbols_exist():
    for name in (
        "ModelConfig",
        "TrainerConfig",
        "ModelInterface",
        "BaseModel",
        "PredictionResult",
        "ModelFactory",
        "ModelRegistry",
        "register",
        "get",
        "list_models",
        "ModelError",
        "ModelConfigError",
        "ModelTrainingError",
        "ModelLoadError",
        "ModelPredictionError",
        "ModelInferenceError",
        "CheckpointError",
    ):
        assert hasattr(rm, name), f"missing public symbol: {name}"
        assert name in rm.__all__, f"{name} absent from __all__"


def test_list_models_is_callable_and_returns_list():
    assert callable(rm.list_models)
    assert isinstance(rm.list_models(), list)


def test_exceptions_subclass_model_error():
    for exc in (
        rm.ModelConfigError,
        rm.ModelTrainingError,
        rm.ModelLoadError,
        rm.ModelPredictionError,
        rm.ModelInferenceError,
        rm.CheckpointError,
    ):
        assert issubclass(exc, rm.ModelError)


def test_init_does_not_top_level_import_heavy_modules():
    src = inspect.getsource(rm)
    for heavy in (
        "from .builders",
        "import builders",
        "from .components",
        "import components",
        "from .model_trainer",
        "from .model_evaluator",
        "from .vendored",
        "vendored_sr_adapter",
    ):
        assert heavy not in src, f"heavy import leaked into __init__: {heavy}"


def test_torch_free_import_via_subprocess():
    """Prove ripple.models imports without pulling torch into sys.modules."""
    code = (
        "import sys, ripple.models; "
        "assert 'torch' not in sys.modules, "
        "sorted(m for m in sys.modules if 'torch' in m); "
        "print('OK')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_all_light_symbols_in_subprocess():
    """Prove all public symbols are accessible in a torch-free interpreter."""
    symbols = [
        "ModelConfig",
        "TrainerConfig",
        "ModelInterface",
        "BaseModel",
        "PredictionResult",
        "ModelFactory",
        "ModelRegistry",
        "register",
        "get",
        "list_models",
        "ModelError",
        "ModelConfigError",
        "ModelTrainingError",
        "ModelLoadError",
        "ModelPredictionError",
        "ModelInferenceError",
        "CheckpointError",
    ]
    checks = "; ".join(
        f"assert hasattr(rm, {name!r}), 'missing {name}'"
        for name in symbols
    )
    code = f"import ripple.models as rm; {checks}; print('OK')"
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
