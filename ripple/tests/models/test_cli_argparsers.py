"""Argument-parser surface of the model CLIs (offline, torch-free)."""
import argparse

from ripple.models.cli import train as train_cli
from ripple.models.cli import evaluate as eval_cli


def test_train_parser_defaults():
    parser = train_cli.build_parser()
    assert isinstance(parser, argparse.ArgumentParser)
    args = parser.parse_args(["--manifest", "m.csv"])
    assert args.manifest == "m.csv"
    assert args.model_type == "resnet_binary"
    assert args.out == "models_cache"
    assert args.epochs == 30
    assert args.batch_size == 64


def test_train_parser_overrides():
    parser = train_cli.build_parser()
    args = parser.parse_args(
        ["--manifest", "m.csv", "--model-type", "vit_binary",
         "--epochs", "3", "--batch-size", "8", "--out", "/tmp/run"]
    )
    assert args.model_type == "vit_binary"
    assert args.epochs == 3
    assert args.batch_size == 8
    assert args.out == "/tmp/run"


def test_evaluate_parser_defaults():
    parser = eval_cli.build_parser()
    assert isinstance(parser, argparse.ArgumentParser)
    args = parser.parse_args(["--manifest", "m.csv", "--checkpoint", "ckpt.pt"])
    assert args.manifest == "m.csv"
    assert args.checkpoint == "ckpt.pt"
    assert args.split == "test"
    assert args.threshold == 0.5


def test_clis_have_main_and_run():
    for mod in (train_cli, eval_cli):
        assert callable(mod.main)
        assert callable(mod.run)
        assert callable(mod.build_parser)
