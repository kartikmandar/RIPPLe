import pytest

from ripple.models.config import ModelConfig
from ripple.models.model_factory import ModelFactory
from ripple.models import model_registry
from ripple.models.exceptions import ModelConfigError


class _FakeModel:
    """Stand-in for BaseModel so the factory can be tested torch-free."""
    def __init__(self, config):
        self.config = config
        self.loaded_from = None

    def load_weights(self, path):
        self.loaded_from = path


@pytest.fixture
def fake_builder():
    """Register a fake builder, yield its key, then clean up the registry."""
    key = "fake_factory_model"
    def _builder(config):
        return _FakeModel(config)
    model_registry._REGISTRY[key] = _builder
    try:
        yield key
    finally:
        model_registry._REGISTRY.pop(key, None)


def test_create_returns_builder_output_and_sets_model_type(fake_builder):
    obj = ModelFactory.create(fake_builder)
    assert isinstance(obj, _FakeModel)
    assert obj.config.model_type == fake_builder


def test_create_accepts_dict_config_via_from_dict(fake_builder):
    obj = ModelFactory.create(fake_builder, config={"num_classes": 3, "bogus": 1})
    assert isinstance(obj.config, ModelConfig)
    assert obj.config.num_classes == 3
    assert obj.config.model_type == fake_builder


def test_create_accepts_modelconfig_instance(fake_builder):
    cfg = ModelConfig(num_classes=2)
    obj = ModelFactory.create(fake_builder, config=cfg)
    assert obj.config.num_classes == 2
    assert obj.config.model_type == fake_builder


def test_create_default_config_when_none(fake_builder):
    obj = ModelFactory.create(fake_builder, config=None)
    assert isinstance(obj.config, ModelConfig)
    assert obj.config.model_type == fake_builder


def test_create_loads_weights_when_weights_path_set(fake_builder, tmp_path):
    ckpt = tmp_path / "w.pt"
    ckpt.write_bytes(b"not-real")
    obj = ModelFactory.create(fake_builder, config={"weights_path": str(ckpt)})
    assert obj.loaded_from == str(ckpt)


def test_create_no_load_when_weights_path_absent(fake_builder):
    obj = ModelFactory.create(fake_builder)
    assert obj.loaded_from is None


def test_create_unknown_model_type_raises(monkeypatch):
    # Builders import must not silently provide the key.
    with pytest.raises(ModelConfigError):
        ModelFactory.create("definitely_not_registered_zzz")


def test_from_dict_dispatches_on_model_type(fake_builder):
    obj = ModelFactory.from_dict({"model_type": fake_builder, "num_classes": 5})
    assert isinstance(obj, _FakeModel)
    assert obj.config.num_classes == 5
    assert obj.config.model_type == fake_builder
