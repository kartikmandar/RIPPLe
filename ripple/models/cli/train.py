"""Train a RIPPLe lens classifier on an ingested DeepLense manifest.

Out-of-suite: the offline test suite only checks ``build_parser()``; the
training body (``run``) imports torch and the trainer lazily.

Example::

    python -m ripple.models.cli.train \\
        --manifest data/deeplense/testII_ingested/manifest.csv \\
        --model-type resnet_binary --epochs 30 --out models_cache
"""
import argparse
import os
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ripple.models.cli.train",
        description="Train a RIPPLe lens classifier on an ingested manifest.",
    )
    parser.add_argument(
        "--manifest", required=True,
        help="Path to the ingested manifest.csv (from ripple-ingest-deeplense).",
    )
    parser.add_argument(
        "--model-type", default="resnet_binary", dest="model_type",
        help="Registry key (resnet_binary|vit_binary|resnet_multiclass|vit_multiclass).",
    )
    parser.add_argument(
        "--task", default="binary",
        help="Task: binary|multiclass.",
    )
    parser.add_argument(
        "--num-classes", type=int, default=2, dest="num_classes",
        help="Number of classes (multiclass only).",
    )
    parser.add_argument(
        "--encoder", default="resnet18",
        help="Encoder backbone (resnet18|resnet34|vit_b_16|vit_small).",
    )
    parser.add_argument(
        "--epochs", type=int, default=30, help="Training epochs.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=64, dest="batch_size",
        help="Mini-batch size.",
    )
    parser.add_argument(
        "--lr", type=float, default=3e-4, help="Learning rate.",
    )
    parser.add_argument(
        "--num-workers", type=int, default=0, dest="num_workers",
        help="DataLoader workers.",
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="Random seed.",
    )
    parser.add_argument(
        "--device", default=None,
        help="Force device (cuda|mps|cpu); default auto-resolves.",
    )
    parser.add_argument(
        "--out", default="models_cache",
        help="Output directory for the checkpoint (gitignored cache).",
    )
    return parser


def run(args) -> int:
    # Heavy imports are deferred so module import + build_parser() stay torch-free.
    from ripple.models.config import ModelConfig, TrainerConfig
    from ripple.models.model_factory import ModelFactory
    from ripple.models.model_trainer import ModelTrainer
    from ripple.preprocessing.dataset import RippleCutoutDataset, make_dataloader

    model_config = ModelConfig.from_dict({
        "model_type": args.model_type,
        "task": args.task,
        "num_classes": args.num_classes,
        "encoder": args.encoder,
    })
    model = ModelFactory.create(args.model_type, model_config)
    # Build the underlying nn.Module now so the trainer's ``_unwrap`` finds a
    # real ``model._net`` (it is None until first build) to optimize/checkpoint.
    model._build()

    trainer_config = TrainerConfig.from_dict({
        "task": args.task,
        "num_classes": args.num_classes,
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "num_workers": args.num_workers,
        "seed": args.seed,
        "device": args.device,
    })

    train_ds = RippleCutoutDataset(args.manifest, split="train")
    train_loader = make_dataloader(
        train_ds, batch_size=args.batch_size, shuffle=True,
        num_workers=args.num_workers, seed=args.seed,
    )
    # Use a validation loader only when the manifest actually carries a 'val'
    # split; an empty val set would otherwise make the trainer's validation
    # metrics undefined (empty y_true/y_pred).
    val_loader = None
    try:
        val_ds = RippleCutoutDataset(args.manifest, split="val")
        if len(val_ds) > 0:
            val_loader = make_dataloader(
                val_ds, batch_size=args.batch_size, shuffle=False,
                num_workers=args.num_workers, seed=args.seed,
            )
    except Exception:
        val_loader = None

    trainer = ModelTrainer(trainer_config)
    history = trainer.fit(model, train_loader, val_loader)

    # ``--out`` is a directory (checkpoint named ``<model_type>.pt``) unless it
    # already names a ``.pt`` file, in which case it is the exact path to write.
    if args.out.endswith(".pt"):
        ckpt_path = args.out
        parent = os.path.dirname(ckpt_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    else:
        os.makedirs(args.out, exist_ok=True)
        ckpt_path = os.path.join(args.out, f"{args.model_type}.pt")
    trainer.save_checkpoint(ckpt_path, model, history=history)

    print(f"[train] model_type={args.model_type} "
          f"best_epoch={history.best_epoch} best_metric={history.best_metric}")
    print(f"[train] checkpoint -> {ckpt_path}")
    return 0


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
