import pytest


def test_keys_registered():
    import ripple.models.builders  # noqa: F401 (import side-effect registers)
    from ripple.models.model_registry import list_models
    keys = list_models()
    assert "mae_vit_binary" in keys
    assert "mae_vit_multiclass" in keys


def test_factory_builds_mae_vit_binary():
    pytest.importorskip("torch")
    from ripple.models.model_factory import ModelFactory
    m = ModelFactory.create("mae_vit_binary")
    assert m.config.encoder == "mae_vit_tiny"
    assert m.config.task == "binary"
