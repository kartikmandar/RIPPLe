import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_build_net_mae_vit_binary_one_logit():
    from ripple.models.config import ModelConfig
    from ripple.models.components import build_net, MaskedViTEncoder
    net = build_net(ModelConfig(encoder="mae_vit_tiny", task="binary", patch_size=4))
    assert isinstance(net.encoder, MaskedViTEncoder)
    out = net(torch.randn(2, 3, 64, 64))
    assert out.shape == (2, 1)


def test_build_net_mae_vit_multiclass_num_classes_logits():
    from ripple.models.config import ModelConfig
    from ripple.models.components import build_net
    net = build_net(ModelConfig(encoder="mae_vit_tiny", task="multiclass",
                                num_classes=3))
    out = net(torch.randn(2, 3, 64, 64))
    assert out.shape == (2, 3)
