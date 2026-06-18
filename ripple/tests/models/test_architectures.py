# ripple/tests/models/test_architectures.py
import pytest

torch = pytest.importorskip("torch")

from ripple.models.config import ModelConfig


@pytest.mark.torch
def test_resnet_encoder_feature_dim_and_3ch_conv1():
    from ripple.models.components import ResNetEncoder, Encoder
    enc = ResNetEncoder(arch="resnet18", pretrained=False, in_channels=3)
    # Genuine module-scope subclass: isinstance/issubclass both hold.
    assert isinstance(enc, ResNetEncoder)
    assert isinstance(enc, Encoder)
    assert issubclass(ResNetEncoder, Encoder)
    assert enc.feature_dim == 512
    # No conv1 surgery for the 3-channel path: stock conv1 stays 3ch.
    assert enc.backbone.conv1.in_channels == 3
    x = torch.zeros(2, 3, 64, 64)
    feat = enc(x)
    assert tuple(feat.shape) == (2, 512)


@pytest.mark.torch
def test_resnet_encoder_conv1_surgery_when_not_3ch():
    from ripple.models.components import ResNetEncoder
    enc = ResNetEncoder(arch="resnet18", pretrained=False, in_channels=1)
    assert enc.backbone.conv1.in_channels == 1
    feat = enc(torch.zeros(2, 1, 64, 64))
    assert tuple(feat.shape) == (2, 512)


@pytest.mark.torch
def test_vit_small_scratch_feature_dim_and_3ch_conv_proj():
    from ripple.models.components import ViTEncoder, Encoder
    enc = ViTEncoder(variant="small_scratch", pretrained=False, in_channels=3, input_size=64)
    # Genuine module-scope subclass: isinstance/issubclass both hold.
    assert isinstance(enc, ViTEncoder)
    assert isinstance(enc, Encoder)
    assert issubclass(ViTEncoder, Encoder)
    assert enc.backbone.conv_proj.in_channels == 3
    feat = enc(torch.zeros(2, 3, 64, 64))
    assert feat.ndim == 2 and feat.shape[0] == 2
    assert enc.feature_dim == feat.shape[1]


@pytest.mark.torch
def test_classifier_head_shape():
    from ripple.models.components import ClassifierHead
    head = ClassifierHead(feature_dim=8, num_logits=1, dropout=0.0)
    out = head(torch.zeros(2, 8))
    assert tuple(out.shape) == (2, 1)
    head3 = ClassifierHead(feature_dim=8, num_logits=3, dropout=0.5)
    assert tuple(head3(torch.zeros(2, 8)).shape) == (2, 3)


@pytest.mark.torch
def test_encoder_head_net_freeze_encoder():
    from ripple.models.components import EncoderHeadNet, ClassifierHead

    class _Enc(torch.nn.Module):
        feature_dim = 4

        def __init__(self):
            super().__init__()
            self.lin = torch.nn.Linear(12, 4)

        def forward(self, x):
            return self.lin(x.flatten(1))

    net = EncoderHeadNet(_Enc(), ClassifierHead(4, 1))
    net.freeze_encoder()
    assert all(not p.requires_grad for p in net.encoder.parameters())
    assert all(p.requires_grad for p in net.head.parameters())
    out = net(torch.zeros(2, 3, 2, 2))
    assert tuple(out.shape) == (2, 1)


@pytest.mark.torch
def test_build_net_resnet18_binary_shape():
    from ripple.models.components import build_net, EncoderHeadNet
    cfg = ModelConfig(model_type="resnet_binary", task="binary", encoder="resnet18",
                      num_classes=2, input_size=64, pretrained=False)
    net = build_net(cfg)
    assert isinstance(net, EncoderHeadNet)
    out = net(torch.zeros(2, 3, 64, 64))
    assert tuple(out.shape) == (2, 1)


@pytest.mark.torch
def test_build_net_resnet18_multiclass_shape():
    from ripple.models.components import build_net
    cfg = ModelConfig(model_type="resnet_multiclass", task="multiclass", encoder="resnet18",
                      num_classes=3, input_size=64, pretrained=False)
    out = build_net(cfg)(torch.zeros(2, 3, 64, 64))
    assert tuple(out.shape) == (2, 3)


@pytest.mark.torch
def test_build_net_vit_small_scratch_binary_and_multiclass_shape():
    from ripple.models.components import build_net
    cfg_b = ModelConfig(model_type="vit_binary", task="binary", encoder="vit_small",
                        vit_variant="small_scratch", num_classes=2, input_size=64, pretrained=False)
    assert tuple(build_net(cfg_b)(torch.zeros(2, 3, 64, 64)).shape) == (2, 1)
    cfg_m = ModelConfig(model_type="vit_multiclass", task="multiclass", encoder="vit_small",
                        vit_variant="small_scratch", num_classes=3, input_size=64, pretrained=False)
    assert tuple(build_net(cfg_m)(torch.zeros(2, 3, 64, 64)).shape) == (2, 3)
