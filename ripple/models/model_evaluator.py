"""Metric computation for RIPPLe classifiers.

ROC-AUC primary; accuracy/precision/recall/F1/confusion. Binary models emit
ONE logit (sigmoid); multiclass emit ``num_classes`` logits (softmax).
Heavy imports (torch / numpy / sklearn) are lazy so importing this module is
cheap and torch-free.
"""


class ModelEvaluator:
    """Compute classification metrics for a model over a dataloader.

    ``config`` may be a plain task string ("binary"|"multiclass"), an object
    exposing a ``.task`` attribute (e.g. ``TrainerConfig``/``ModelConfig``), or
    ``None`` (defaults to "binary").
    """

    def __init__(self, config=None):
        self.config = config
        if config is None:
            self.task = "binary"
        elif isinstance(config, str):
            self.task = config
        else:
            self.task = getattr(config, "task", "binary")

    def _iter_logits(self, model, loader):
        """Yield (logits_np, y_np) per batch over a no-grad eval pass."""
        import numpy as np
        import torch

        if hasattr(model, "eval"):
            model.eval()
        with torch.no_grad():
            for batch in loader:
                x, y = batch[0], batch[1]
                if hasattr(model, "predict_logits"):
                    logits = model.predict_logits(x)
                elif hasattr(model, "forward_logits"):
                    logits = model.forward_logits(x)
                else:
                    logits = model(x)
                yield (
                    logits.detach().cpu().numpy(),
                    y.detach().cpu().numpy().astype(np.int64),
                )

    def predict_proba(self, model, loader):
        """Return ``(y_true, y_score)`` with ``y == -1`` rows masked out.

        Binary: ``y_score`` is shape ``(N,)`` sigmoid of the single logit.
        Multiclass: ``y_score`` is shape ``(N, num_classes)`` softmax.
        """
        import numpy as np

        y_true_chunks = []
        score_chunks = []
        for logits, y in self._iter_logits(model, loader):
            if self.task == "binary":
                logits = logits.reshape(logits.shape[0], -1)[:, 0]
                score = 1.0 / (1.0 + np.exp(-logits))
            else:
                z = logits - logits.max(axis=1, keepdims=True)
                ez = np.exp(z)
                score = ez / ez.sum(axis=1, keepdims=True)
            y_true_chunks.append(y)
            score_chunks.append(score)

        if not y_true_chunks:
            empty_score = (
                np.empty((0,), dtype=np.float64)
                if self.task == "binary"
                else np.empty((0, 0), dtype=np.float64)
            )
            return np.empty((0,), dtype=np.int64), empty_score

        y_true = np.concatenate(y_true_chunks, axis=0)
        y_score = np.concatenate(score_chunks, axis=0)
        mask = y_true != -1
        return y_true[mask], y_score[mask]

    def evaluate(self, model, loader, *, threshold=0.5):
        """Compute metrics. Returns a dict keyed by metric name."""
        y_true, y_score = self.predict_proba(model, loader)
        if self.task == "binary":
            return self._evaluate_binary(y_true, y_score, threshold)
        return self._evaluate_multiclass(y_true, y_score, threshold)

    def _evaluate_binary(self, y_true, y_score, threshold):
        import numpy as np
        from sklearn.metrics import (
            accuracy_score,
            confusion_matrix,
            f1_score,
            precision_score,
            recall_score,
            roc_auc_score,
        )

        y_pred = (y_score >= threshold).astype(np.int64)
        try:
            auc = float(roc_auc_score(y_true, y_score))
        except ValueError:
            auc = float("nan")
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
        return {
            "auc": auc,
            "accuracy": float(accuracy_score(y_true, y_pred)),
            "precision": float(
                precision_score(y_true, y_pred, zero_division=0)
            ),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "f1": float(f1_score(y_true, y_pred, zero_division=0)),
            "confusion_matrix": cm.tolist(),
            "threshold": float(threshold),
        }

    def _evaluate_multiclass(self, y_true, y_score, threshold):
        raise NotImplementedError(
            "Multiclass evaluation is handled by Task 14."
        )
