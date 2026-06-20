import pytest
pytest.importorskip("torch")
import torch

pytestmark = pytest.mark.torch


def test_optimizer_excludes_frozen_encoder_params():
    from ripple.models.config import ModelConfig, TrainerConfig
    from ripple.models.components import build_net
    from ripple.models.model_trainer import ModelTrainer
    net = build_net(ModelConfig(encoder="mae_vit_tiny", task="binary"))
    net.freeze_encoder()
    trainer = ModelTrainer(TrainerConfig(task="binary"))
    opt = trainer._make_optimizer(net)
    n_opt_params = sum(p.numel() for g in opt.param_groups for p in g["params"])
    n_head_params = sum(p.numel() for p in net.head.parameters())
    assert n_opt_params == n_head_params      # only the head is trainable
