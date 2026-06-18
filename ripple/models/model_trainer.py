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
    """
    A placeholder for the Model Trainer.
    This class will be implemented in a later phase.
    """
    def __init__(self, config=None):
        self.config = config

