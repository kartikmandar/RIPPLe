"""Out-of-suite MAE machinery demo on local real LSST imagery.

Grid-extracts unlabelled cutouts from the local pipelines_check calexp
(data_release='dp02'), MAE-pretrains a few epochs, saves an encoder
checkpoint, then loads it into a mae_vit_binary classifier and runs a
forward pass. This is a MACHINERY proof (does it run end-to-end on real
imagery), NOT an accuracy claim — quantitative sim-to-real measurement is
the Phase-5C instrument.
"""
from __future__ import annotations

import numpy as np


def main():
    import torch
    from torch.utils.data import TensorDataset, DataLoader
    from ripple.models.components import MaskedViTEncoder
    from ripple.models.ssl.mae import MAE
    from ripple.models.ssl.mae_trainer import MAETrainer
    from ripple.models.config import MAEConfig
    from ripple.models.model_factory import ModelFactory

    # NOTE: replace this synthetic stand-in with real calexp cutouts via
    # ripple.data_access.butler_client.ButlerClient(data_release="dp02") +
    # ripple.preprocessing.Preprocessor when running against the local repo.
    cutouts = np.random.randn(16, 3, 64, 64).astype("float32")
    x = torch.from_numpy(cutouts)
    loader = DataLoader(TensorDataset(x, torch.full((16,), -1)), batch_size=8)

    mae = MAE(MaskedViTEncoder())
    trainer = MAETrainer(MAEConfig(epochs=2, device="cpu"))
    hist = trainer.fit(mae, loader)
    print("MAE pretrain loss:", hist[-1]["train_loss"])

    ckpt = "models_cache/demo_mae_vit_tiny.pt"
    import os
    os.makedirs("models_cache", exist_ok=True)
    trainer.save_encoder_checkpoint(ckpt, mae, epoch=2)

    clf = ModelFactory.create("mae_vit_binary",
                              {"encoder": "mae_vit_tiny", "task": "binary",
                               "encoder_weights_path": ckpt})
    rows = clf.predict_batch(x)
    print("classifier produced", len(rows), "predictions from MAE-pretrained encoder")


if __name__ == "__main__":
    main()
