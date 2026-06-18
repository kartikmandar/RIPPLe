"""Training loop, checkpointing, and History for RIPPLe classifiers.

Heavy imports (torch, ripple.models.model_evaluator) are imported lazily inside
methods so that `import ripple.models.model_trainer` succeeds with torch absent.
The trainer operates on a raw ``nn.Module`` (``model._net`` if a BaseModel-like
wrapper is passed, else the object itself), masks unlabelled rows (``y == -1``),
and delegates validation metrics to ModelEvaluator.
"""
from dataclasses import dataclass


@dataclass
class History:
    """Per-epoch training record plus best-checkpoint bookkeeping."""
    epochs: list
    best_epoch: int
    best_metric: float
    best_state_dict: object = None


class ModelTrainer:
    """Trains a raw ``nn.Module`` on the canonical RIPPLe cutout contract.

    Masks unlabelled rows (``y == -1``), computes ``pos_weight = N_neg/N_pos``
    for the binary BCE loss, tracks the best ``monitor`` metric, and supports
    early stopping. Validation metrics are delegated to ModelEvaluator.
    """

    _MAXIMIZE = {"val_auc", "auc", "val_accuracy", "accuracy", "val_f1", "f1"}

    def __init__(self, config):
        from ripple.models.config import TrainerConfig
        if config is None:
            config = TrainerConfig()
        elif isinstance(config, dict):
            config = TrainerConfig.from_dict(config)
        self.config = config

    @staticmethod
    def _unwrap(model):
        net = getattr(model, "_net", None)
        return net if net is not None else model

    def _resolve_device(self):
        import torch
        dev = self.config.device
        if dev in (None, "auto"):
            dev = self.config.resolve_device()
        return torch.device(dev)

    @staticmethod
    def _seed_all(seed):
        import random

        import numpy as np
        import torch
        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)

    def _compute_pos_weight(self, loader):
        import torch
        n_pos = 0
        n_neg = 0
        for _, y in loader:
            y = y.view(-1)
            mask = y != -1
            yv = y[mask]
            n_pos += int((yv == 1).sum().item())
            n_neg += int((yv == 0).sum().item())
        if n_pos == 0:
            return torch.tensor(1.0)
        return torch.tensor(float(n_neg) / float(n_pos))

    def _make_loss(self, loader, device, class_weights):
        import torch
        import torch.nn as nn
        if self.config.task == "binary":
            pos_weight = None
            if self.config.imbalance == "pos_weight":
                pos_weight = self._compute_pos_weight(loader).to(device)
            return nn.BCEWithLogitsLoss(pos_weight=pos_weight)
        weight = None
        if class_weights is not None:
            weight = torch.as_tensor(class_weights, dtype=torch.float32, device=device)
        return nn.CrossEntropyLoss(weight=weight)

    def _make_optimizer(self, net):
        import torch
        if self.config.optimizer == "adamw":
            return torch.optim.AdamW(net.parameters(), lr=self.config.lr,
                                     weight_decay=self.config.weight_decay)
        if self.config.optimizer == "adam":
            return torch.optim.Adam(net.parameters(), lr=self.config.lr,
                                    weight_decay=self.config.weight_decay)
        if self.config.optimizer == "sgd":
            return torch.optim.SGD(net.parameters(), lr=self.config.lr,
                                   weight_decay=self.config.weight_decay)
        raise ValueError(f"unknown optimizer: {self.config.optimizer}")

    def _batch_loss(self, net, loss_fn, x, y):
        mask = y.view(-1) != -1
        if not bool(mask.any()):
            return None
        x = x[mask]
        y = y.view(-1)[mask]
        logits = net(x)
        if self.config.task == "binary":
            return loss_fn(logits.view(-1), y.float())
        return loss_fn(logits, y.long())

    def _train_epoch(self, net, loader, loss_fn, optimizer, device):
        import torch
        net.train()
        total = 0.0
        count = 0
        for x, y in loader:
            x = x.to(device).float()
            y = y.to(device)
            loss = self._batch_loss(net, loss_fn, x, y)
            if loss is None:
                continue
            optimizer.zero_grad()
            loss.backward()
            if self.config.grad_clip_norm is not None:
                torch.nn.utils.clip_grad_norm_(net.parameters(),
                                               self.config.grad_clip_norm)
            optimizer.step()
            total += float(loss.item())
            count += 1
        if count == 0:
            return 0.0
        return total / count

    def _validate(self, model, net, val_loader):
        from ripple.models.model_evaluator import ModelEvaluator
        return ModelEvaluator(self.config).evaluate(model, val_loader)

    @staticmethod
    def _is_val_metric(monitor):
        """True if ``monitor`` requires validation data to be computed."""
        return monitor.startswith("val") or monitor in {
            "val_auc", "val_accuracy", "val_f1"}

    def _is_better(self, current, best, maximize):
        if maximize:
            return current > best + self.config.min_delta
        return current < best - self.config.min_delta

    def fit(self, model, train_loader, val_loader=None, *, class_weights=None):
        import copy

        import torch  # noqa: F401 — ensures torch is importable
        self._seed_all(self.config.seed)
        device = self._resolve_device()
        net = self._unwrap(model)
        net.to(device)
        loss_fn = self._make_loss(train_loader, device, class_weights)
        optimizer = self._make_optimizer(net)

        # When no val_loader is given but the configured monitor is a validation
        # metric (cannot be computed without validation), fall back to monitoring
        # train_loss in the MINIMIZE direction. Explicitly non-val monitors and
        # the with-val_loader behavior are left unchanged.
        monitor = self.config.monitor
        if val_loader is None and self._is_val_metric(monitor):
            monitor = "train_loss"
        maximize = monitor in self._MAXIMIZE
        best_metric = float("-inf") if maximize else float("inf")
        best_epoch = -1
        best_state = None
        epochs = []
        since_improved = 0

        for epoch in range(self.config.epochs):
            train_loss = self._train_epoch(net, train_loader, loss_fn,
                                           optimizer, device)
            record = {"epoch": epoch, "train_loss": train_loss}
            if val_loader is not None:
                metrics = self._validate(model, net, val_loader)
                record.update({f"val_{k}": v for k, v in metrics.items()
                               if isinstance(v, (int, float))})
                monitored = record.get(monitor,
                                       metrics.get(monitor.replace("val_", "")))
            else:
                monitored = record.get(monitor, train_loss)
            if monitored is None:
                monitored = train_loss
            record["monitored"] = float(monitored)
            epochs.append(record)

            if self._is_better(monitored, best_metric, maximize):
                best_metric = float(monitored)
                best_epoch = epoch
                best_state = copy.deepcopy(
                    {k: v.detach().cpu() for k, v in net.state_dict().items()})
                since_improved = 0
            else:
                since_improved += 1

            if self.config.early_stopping and since_improved >= self.config.patience:
                break

        if best_state is None:
            best_state = copy.deepcopy(
                {k: v.detach().cpu() for k, v in net.state_dict().items()})
            best_epoch = len(epochs) - 1
            best_metric = epochs[-1]["monitored"] if epochs else float("nan")

        return History(epochs=epochs, best_epoch=best_epoch,
                       best_metric=best_metric, best_state_dict=best_state)

