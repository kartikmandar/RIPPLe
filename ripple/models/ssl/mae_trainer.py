"""MAE pretraining loop (pure torch)."""
from __future__ import annotations


def _resolve_device(name):
    import torch
    if name:
        return name
    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


class MAETrainer:
    def __init__(self, config):
        self.config = config

    def _make_optimizer(self, mae):
        import torch
        params = [p for p in mae.parameters() if p.requires_grad]
        if self.config.optimizer == "adam":
            return torch.optim.Adam(params, lr=self.config.lr,
                                    weight_decay=self.config.weight_decay)
        if self.config.optimizer == "sgd":
            return torch.optim.SGD(params, lr=self.config.lr,
                                   weight_decay=self.config.weight_decay)
        return torch.optim.AdamW(params, lr=self.config.lr,
                                 weight_decay=self.config.weight_decay)

    def fit(self, mae, loader):
        import torch
        torch.manual_seed(self.config.seed)
        device = _resolve_device(self.config.device)
        mae.to(device)
        mae.train()
        opt = self._make_optimizer(mae)
        history = []
        for epoch in range(self.config.epochs):
            total, count = 0.0, 0
            for batch in loader:
                x = batch[0].to(device)
                opt.zero_grad()
                loss, _, _ = mae(x, mask_ratio=self.config.mask_ratio)
                loss.backward()
                if self.config.grad_clip_norm:
                    torch.nn.utils.clip_grad_norm_(mae.parameters(),
                                                   self.config.grad_clip_norm)
                opt.step()
                total += float(loss.item()) * x.shape[0]
                count += x.shape[0]
            history.append({"epoch": epoch, "train_loss": total / max(count, 1)})
        return history

    def save_encoder_checkpoint(self, path, mae, *, epoch=0,
                                norm_mean=None, norm_std=None):
        from ripple.models.ssl.checkpoint import save_encoder_checkpoint
        save_encoder_checkpoint(path, mae, epoch=epoch,
                                norm_mean=norm_mean, norm_std=norm_std)
