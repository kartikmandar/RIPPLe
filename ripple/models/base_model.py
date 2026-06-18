"""Model interface layer for RIPPLe.

Top-level imports are stdlib only so `import ripple.models.base_model`
succeeds with torch absent. torch is imported lazily inside the methods
of BaseModel that actually need it.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class PredictionResult:
    """Structured per-cutout prediction.

    `to_dict` flattens to keys compatible with predictions.PREDICTION_FIELDS:
    probabilities are spread into ``prob_<name>`` columns, ``class_name`` is
    emitted as ``pred_class_name``, and ``extra`` (e.g. model_name, ra/dec)
    is merged last. Binary results additionally expose ``prob_lens`` /
    ``prob_non_lens`` aliases for catalog triage.
    """
    task: str
    pred_class: Optional[int]
    class_name: Optional[str]
    score: Optional[float]
    probabilities: dict
    confidence: Optional[float]
    image: object = None
    extra: Optional[dict] = None

    def to_dict(self) -> dict:
        row = {
            "task": self.task,
            "pred_class": self.pred_class,
            "pred_class_name": self.class_name,
            "score": self.score,
            "confidence": self.confidence,
        }
        probs = self.probabilities or {}
        for name, prob in probs.items():
            row["prob_" + str(name)] = prob
        if self.task == "binary":
            if "lens" in probs:
                row["prob_lens"] = probs["lens"]
            if "non_lens" in probs:
                row["prob_non_lens"] = probs["non_lens"]
        if self.extra:
            row.update(self.extra)
        return row


class ModelInterface(ABC):
    """
    Abstract base class for all models in RIPPLe pipeline.
    
    This interface ensures consistent API across different model types
    (DeepLense, custom models, future integrations).
    """
    
    @abstractmethod
    def predict(self, data):
        """Abstract method for model prediction."""
        pass

class BaseModel(ModelInterface):
    """
    A placeholder for the Base Model.
    This class will be implemented in a later phase.
    """
    def __init__(self, config=None):
        self.config = config
    
    def predict(self, data):
        """Placeholder implementation of predict method."""
        raise NotImplementedError("Predict method not implemented yet")