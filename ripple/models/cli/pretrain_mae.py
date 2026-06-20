"""Out-of-suite MAE pretraining CLI.

Example:
    python -m ripple.models.cli.pretrain_mae \
        --manifest data/deeplense/manifest.csv \
        --out models_cache/mae_vit_tiny.pt --epochs 100 --mask-ratio 0.75
"""
from __future__ import annotations

import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(description="MAE pretraining (out-of-suite)")
    parser.add_argument("--manifest", required=True, help="RIPPLe manifest.csv of cutouts")
    parser.add_argument("--out", required=True, help="output encoder checkpoint path")
    parser.add_argument("--epochs", type=int, default=100)
    parser.add_argument("--mask-ratio", type=float, default=0.75)
    parser.add_argument("--patch-size", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--device", default=None)
    args = parser.parse_args(argv)

    from ripple.preprocessing.dataset import RippleCutoutDataset, make_dataloader
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.mae_trainer import MAETrainer
    from ripple.models.config import MAEConfig

    ds = RippleCutoutDataset(args.manifest, split="train")
    loader = make_dataloader(ds, batch_size=args.batch_size, shuffle=True)
    mae = MAE(MaskedViTEncoder(patch_size=args.patch_size))
    cfg = MAEConfig(epochs=args.epochs, mask_ratio=args.mask_ratio,
                    patch_size=args.patch_size, batch_size=args.batch_size,
                    lr=args.lr, device=args.device)
    trainer = MAETrainer(cfg)
    history = trainer.fit(mae, loader)
    trainer.save_encoder_checkpoint(args.out, mae, epoch=args.epochs)
    print("final train_loss:", history[-1]["train_loss"])
    print("encoder checkpoint:", args.out)


if __name__ == "__main__":
    main()
