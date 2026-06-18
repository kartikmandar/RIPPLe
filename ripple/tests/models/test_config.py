"""Tests for ripple.models.config (ModelConfig + TrainerConfig)."""
import dataclasses

import pytest

from ripple.models.config import ModelConfig, TrainerConfig
from ripple.models.exceptions import ModelConfigError


# --------------------------- ModelConfig ---------------------------

def test_modelconfig_defaults():
    c = ModelConfig()
    assert c.model_type == "resnet_binary"
    assert c.task == "binary"
    assert c.encoder == "resnet18"
    assert c.in_channels == 3
    assert c.num_classes == 2
    assert c.input_size == 64
    assert c.class_names == ("non_lens", "lens")
    assert c.weights_path is None
    assert c.device == "auto"
    assert c.dropout == 0.0
    assert c.pretrained is False
    assert c.apply_imagenet_norm is False
    assert c.vit_variant == "b16_finetune"


def test_modelconfig_is_frozen():
    c = ModelConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        c.task = "multiclass"


def test_modelconfig_head_logits_binary_is_one():
    assert ModelConfig(task="binary", num_classes=2).head_logits == 1


def test_modelconfig_head_logits_multiclass_is_num_classes():
    assert ModelConfig(task="multiclass", num_classes=3).head_logits == 3


def test_modelconfig_head_logits_super_res_is_num_classes():
    assert ModelConfig(task="super_res", num_classes=2).head_logits == 2


def test_modelconfig_post_init_rejects_bad_task():
    with pytest.raises(ModelConfigError):
        ModelConfig(task="regression")


def test_modelconfig_post_init_rejects_bad_encoder():
    with pytest.raises(ModelConfigError):
        ModelConfig(encoder="alexnet")


def test_modelconfig_post_init_accepts_all_valid_encoders():
    for enc in ("resnet18", "resnet34", "vit_b_16", "vit_small"):
        assert ModelConfig(encoder=enc).encoder == enc


def test_modelconfig_from_dict_none_returns_defaults():
    c = ModelConfig.from_dict(None)
    assert c == ModelConfig()


def test_modelconfig_from_dict_empty_returns_defaults():
    assert ModelConfig.from_dict({}) == ModelConfig()


def test_modelconfig_from_dict_ignores_unknown_keys():
    c = ModelConfig.from_dict({"task": "multiclass", "bogus": 123, "nope": "x"})
    assert c.task == "multiclass"


def test_modelconfig_from_dict_aliases_model_path_to_weights_path():
    c = ModelConfig.from_dict({"model_path": "/tmp/w.pt"})
    assert c.weights_path == "/tmp/w.pt"


def test_modelconfig_from_dict_aliases_architecture_to_encoder():
    c = ModelConfig.from_dict({"architecture": "resnet34"})
    assert c.encoder == "resnet34"


def test_modelconfig_from_dict_explicit_wins_over_alias():
    c = ModelConfig.from_dict({"architecture": "resnet34", "encoder": "vit_b_16"})
    assert c.encoder == "vit_b_16"


def test_modelconfig_from_dict_coerces_class_names_list_to_tuple():
    c = ModelConfig.from_dict({"class_names": ["a", "b", "c"]})
    assert c.class_names == ("a", "b", "c")
    assert isinstance(c.class_names, tuple)


def test_modelconfig_resolve_device_explicit_passthrough():
    assert ModelConfig(device="cpu").resolve_device() == "cpu"


def test_modelconfig_resolve_device_auto_prefers_cuda(monkeypatch):
    import sys
    import types

    fake = types.ModuleType("torch")
    fake.cuda = types.SimpleNamespace(is_available=lambda: True)
    fake.backends = types.SimpleNamespace(
        mps=types.SimpleNamespace(is_available=lambda: True)
    )
    monkeypatch.setitem(sys.modules, "torch", fake)
    assert ModelConfig(device="auto").resolve_device() == "cuda"


def test_modelconfig_resolve_device_auto_mps_when_no_cuda(monkeypatch):
    import sys
    import types

    fake = types.ModuleType("torch")
    fake.cuda = types.SimpleNamespace(is_available=lambda: False)
    fake.backends = types.SimpleNamespace(
        mps=types.SimpleNamespace(is_available=lambda: True)
    )
    monkeypatch.setitem(sys.modules, "torch", fake)
    assert ModelConfig(device="auto").resolve_device() == "mps"


def test_modelconfig_resolve_device_auto_cpu_fallback(monkeypatch):
    import sys
    import types

    fake = types.ModuleType("torch")
    fake.cuda = types.SimpleNamespace(is_available=lambda: False)
    fake.backends = types.SimpleNamespace(
        mps=types.SimpleNamespace(is_available=lambda: False)
    )
    monkeypatch.setitem(sys.modules, "torch", fake)
    assert ModelConfig(device="auto").resolve_device() == "cpu"


def test_modelconfig_resolve_device_auto_cpu_when_torch_absent(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "torch":
            raise ImportError("no torch")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert ModelConfig(device="auto").resolve_device() == "cpu"
