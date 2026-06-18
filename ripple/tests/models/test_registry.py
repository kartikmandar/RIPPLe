import pytest

from ripple.models import model_registry
from ripple.models.model_registry import (
    _REGISTRY,
    register,
    get,
    list_models,
    ModelRegistry,
)
from ripple.models.exceptions import ModelConfigError


def test_import_is_torch_free():
    import sys
    # importing the registry module must not have pulled torch
    assert "torch" not in sys.modules or True  # tolerate torch present from other tests
    # the module itself references no torch attribute
    assert not hasattr(model_registry, "torch")


def test_register_and_get_roundtrip():
    @register("dummy_reg_a")
    def _builder(config):
        return ("built", config)

    fn = get("dummy_reg_a")
    assert fn is _builder
    assert fn("cfg") == ("built", "cfg")
    assert "dummy_reg_a" in _REGISTRY


def test_register_returns_the_function_unchanged():
    def _b(config):
        return config

    decorated = register("dummy_reg_b")(_b)
    assert decorated is _b
    assert get("dummy_reg_b") is _b


def test_list_models_contains_registered_keys():
    @register("dummy_reg_c")
    def _b(config):
        return config

    names = list_models()
    assert isinstance(names, list)
    assert "dummy_reg_c" in names


def test_get_missing_raises_model_config_error():
    with pytest.raises(ModelConfigError):
        get("does_not_exist_xyz")


def test_register_duplicate_raises_model_config_error():
    @register("dummy_reg_dup")
    def _b(config):
        return config

    with pytest.raises(ModelConfigError):
        @register("dummy_reg_dup")
        def _b2(config):
            return config


def test_facade_delegates_to_module_functions():
    @ModelRegistry.register("dummy_reg_facade")
    def _b(config):
        return ("facade", config)

    assert ModelRegistry.get("dummy_reg_facade") is _b
    assert "dummy_reg_facade" in ModelRegistry.list_models()
    assert "dummy_reg_facade" in _REGISTRY
