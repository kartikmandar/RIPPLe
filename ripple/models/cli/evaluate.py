"""Evaluate a trained RIPPLe checkpoint on an ingested manifest split.

Out-of-suite: the offline test suite only checks ``build_parser()``; the
evaluation body (``run``) imports torch and the evaluator lazily.

Example::

    python -m ripple.models.cli.evaluate \\
        --manifest data/deeplense/testII_ingested/manifest.csv \\
        --checkpoint models_cache/resnet_binary.pt --split test
"""
import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ripple.models.cli.evaluate",
        description="Evaluate a trained RIPPLe checkpoint on a manifest split.",
    )
    parser.add_argument(
        "--manifest", required=True,
        help="Path to the ingested manifest.csv.",
    )
    parser.add_argument(
        "--checkpoint", required=True,
        help="Path to the trained checkpoint (.pt) to evaluate.",
    )
    parser.add_argument(
        "--model-type", default="resnet_binary", dest="model_type",
        help="Registry key to instantiate (overridden by checkpoint metadata "
             "when present).",
    )
    parser.add_argument(
        "--task", default="binary", help="Task: binary|multiclass "
             "(overridden by checkpoint metadata when present).",
    )
    parser.add_argument(
        "--num-classes", type=int, default=2, dest="num_classes",
        help="Number of classes (overridden by checkpoint metadata when present).",
    )
    parser.add_argument(
        "--encoder", default=None,
        help="Encoder backbone override (resnet18|resnet34|vit_b_16|vit_small); "
             "default uses the model_type's built-in encoder.",
    )
    parser.add_argument(
        "--split", default="test",
        help="Manifest split to evaluate (train|val|test).",
    )
    parser.add_argument(
        "--batch-size", type=int, default=64, dest="batch_size",
        help="Mini-batch size.",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Decision threshold for the binary task.",
    )
    parser.add_argument(
        "--num-workers", type=int, default=0, dest="num_workers",
        help="DataLoader workers.",
    )
    parser.add_argument(
        "--device", default=None,
        help="Force device (cuda|mps|cpu); default auto-resolves.",
    )
    return parser


def run(args) -> int:
    # Heavy imports are deferred so module import + build_parser() stay torch-free.
    import torch

    from ripple.models.config import ModelConfig
    from ripple.models.model_factory import ModelFactory
    from ripple.models.model_evaluator import ModelEvaluator
    from ripple.preprocessing.dataset import RippleCutoutDataset, make_dataloader

    # 1) Read the self-describing checkpoint metadata FIRST so the model is
    #    rebuilt with the architecture it was trained with (model_type, task,
    #    num_classes, input_size). We read the raw dict here (weights_only=False
    #    is safe for our own checkpoints: only plain types + tensors are stored)
    #    BEFORE building a model, because the architecture-defining metadata only
    #    becomes known once the checkpoint has been read. Loading exactly once
    #    here (and reusing ``ckpt["state_dict"]`` below) avoids a redundant
    #    second read of the same file.
    ckpt = torch.load(args.checkpoint, map_location="cpu", weights_only=False)
    if not isinstance(ckpt, dict) or "state_dict" not in ckpt:
        raise ValueError(
            f"checkpoint {args.checkpoint!r} is not a RIPPLe checkpoint dict"
        )
    model_type = ckpt.get("model_type") or args.model_type
    task = ckpt.get("task") or args.task
    classes = list(ckpt.get("classes") or ())
    num_classes = len(classes) if classes else args.num_classes
    input_size = int(ckpt.get("input_size") or 64)

    # 2) Build the model from the checkpoint's architecture. ``--encoder`` lets
    #    the user override the backbone; otherwise the model_type's builder
    #    default is used. NO weights_path here — that would trigger a redundant
    #    load; weights are restored exactly once below.
    config_dict = {
        "model_type": model_type,
        "task": task,
        "num_classes": num_classes,
        "input_size": input_size,
    }
    if classes:
        config_dict["class_names"] = classes
    if args.encoder is not None:
        config_dict["encoder"] = args.encoder
    model_config = ModelConfig.from_dict(config_dict)
    model = ModelFactory.create(model_type, model_config)

    # 3) Restore the trained weights EXACTLY ONCE (strict shape/key match).
    model._build()
    model._net.load_state_dict(ckpt["state_dict"], strict=True)

    dataset = RippleCutoutDataset(args.manifest, split=args.split)
    loader = make_dataloader(
        dataset, batch_size=args.batch_size, shuffle=False,
        num_workers=args.num_workers, seed=0,
    )

    evaluator = ModelEvaluator(task)
    metrics = evaluator.evaluate(model, loader, threshold=args.threshold)

    print(f"[evaluate] split={args.split} threshold={args.threshold}")
    for key in ("auc", "accuracy", "precision", "recall", "f1"):
        if key in metrics:
            print(f"[evaluate] {key}={metrics[key]}")
    return 0


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
