"""Tests for ripple.models.builders.

Torch-free checks verify registry population and config correctness.
Torch-gated checks exercise the full builder -> BaseModel -> components.build_net
path with a forward pass.
"""
import pytest

from ripple.models.config import ModelConfig
from ripple.models.base_model import BaseModel


# ---------------------------------------------------------------------------
# Torch-free: registry and config shape
# ---------------------------------------------------------------------------

def test_builders_register_four_canonical_keys():
    """Importing builders must populate the registry with the four classifier keys."""
    import ripple.models.builders  # noqa: F401  (import side effect: registration)
    from ripple.models.model_registry import list_models
    keys = set(list_models())
    assert {"resnet_binary", "vit_binary", "resnet_multiclass", "vit_multiclass"} <= keys


def test_resnet_binary_builder_returns_basemodel_with_binary_config():
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    obj = get("resnet_binary")(ModelConfig())
    assert isinstance(obj, BaseModel)
    assert obj.config.task == "binary"
    assert obj.config.encoder == "resnet18"
    assert obj.config.model_type == "resnet_binary"
    assert obj.config.num_classes == 2
    assert obj.config.class_names == ("non_lens", "lens")


def test_resnet_multiclass_builder_sets_three_classes():
    """resnet_multiclass must pin num_classes=3 and class_names regardless of input config."""
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    # Pass a bare ModelConfig (num_classes defaults to 2); builder must override to 3.
    obj = get("resnet_multiclass")(ModelConfig())
    assert isinstance(obj, BaseModel)
    assert obj.config.task == "multiclass"
    assert obj.config.num_classes == 3
    assert obj.config.class_names == ("no_sub", "cdm", "axion")
    assert obj.config.encoder == "resnet18"
    assert obj.config.model_type == "resnet_multiclass"


def test_vit_binary_builder_uses_vit_encoder():
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    obj = get("vit_binary")(ModelConfig())
    assert isinstance(obj, BaseModel)
    assert obj.config.task == "binary"
    assert obj.config.encoder == "vit_small"
    assert obj.config.num_classes == 2
    assert obj.config.class_names == ("non_lens", "lens")
    assert obj.config.model_type == "vit_binary"


def test_vit_multiclass_builder_sets_three_classes():
    """vit_multiclass must pin num_classes=3 and class_names regardless of input config."""
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    obj = get("vit_multiclass")(ModelConfig())
    assert isinstance(obj, BaseModel)
    assert obj.config.task == "multiclass"
    assert obj.config.num_classes == 3
    assert obj.config.class_names == ("no_sub", "cdm", "axion")
    assert obj.config.encoder == "vit_small"
    assert obj.config.model_type == "vit_multiclass"


def test_factory_create_resnet_multiclass_config():
    """ModelFactory.create should produce num_classes=3 for resnet_multiclass."""
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_factory import ModelFactory
    obj = ModelFactory.create("resnet_multiclass")
    assert obj.config.num_classes == 3
    assert obj.config.encoder == "resnet18"
    assert obj.config.task == "multiclass"
    assert obj.config.class_names == ("no_sub", "cdm", "axion")


def test_factory_create_vit_binary_config():
    """ModelFactory.create should produce correct config for vit_binary."""
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_factory import ModelFactory
    obj = ModelFactory.create("vit_binary")
    assert obj.config.encoder == "vit_small"
    assert obj.config.task == "binary"
    assert obj.config.num_classes == 2


# ---------------------------------------------------------------------------
# Torch-gated: forward shape
# ---------------------------------------------------------------------------

@pytest.mark.torch
def test_resnet_binary_forward_shape():
    """resnet_binary forward on (2,3,64,64) must yield (2,1) logits."""
    torch = pytest.importorskip("torch")
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    obj = get("resnet_binary")(ModelConfig(pretrained=False))
    logits = obj.predict_logits(torch.zeros(2, 3, 64, 64))
    assert tuple(logits.shape) == (2, 1)


@pytest.mark.torch
def test_resnet_multiclass_forward_shape():
    """resnet_multiclass forward on (2,3,64,64) must yield (2,3) logits."""
    torch = pytest.importorskip("torch")
    import ripple.models.builders  # noqa: F401
    from ripple.models.model_registry import get
    obj = get("resnet_multiclass")(ModelConfig(pretrained=False))
    logits = obj.predict_logits(torch.zeros(2, 3, 64, 64))
    assert tuple(logits.shape) == (2, 3)
