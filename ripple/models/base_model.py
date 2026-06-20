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
    """Concrete torch wrapper around an EncoderHeadNet built via components.build_net.

    torch is imported lazily inside the methods that need it so the module
    imports cleanly with torch absent. The underlying nn.Module is built on
    first use (_build); the resolved device string is cached lazily.
    """

    def __init__(self, config):
        self.config = config
        self._net = None
        self._device = None

    # -- lazy build / device -------------------------------------------------

    def _build(self):
        if self._net is not None:
            return
        from ripple.models.components import build_net
        self._net = build_net(self.config)

    @property
    def _resolved_device(self):
        if self._device is None:
            self._device = self.config.resolve_device()
        return self._device

    # -- tensor path ---------------------------------------------------------

    def forward_logits(self, x):
        self._build()
        return self._net(x)

    def predict_logits(self, x):
        import torch

        self._build()
        self._net.eval()
        with torch.no_grad():
            return self._net(x)

    # -- structured path -----------------------------------------------------

    def postprocess(self, logits):
        import torch

        task = self.config.task
        class_names = list(self.config.class_names)
        results = []
        if task == "binary":
            # head emits ONE logit per row; sigmoid -> P(lens).
            logits = logits.reshape(logits.shape[0], -1)
            p_lens = torch.sigmoid(logits[:, 0])
            neg_name = class_names[0] if len(class_names) > 0 else "non_lens"
            pos_name = class_names[1] if len(class_names) > 1 else "lens"
            for i in range(p_lens.shape[0]):
                pl = float(p_lens[i].item())
                pn = 1.0 - pl
                pred = 1 if pl >= 0.5 else 0
                conf = pl if pred == 1 else pn
                results.append(PredictionResult(
                    task="binary",
                    pred_class=pred,
                    class_name=pos_name if pred == 1 else neg_name,
                    score=pl,
                    probabilities={neg_name: pn, pos_name: pl},
                    confidence=conf,
                ))
        else:
            probs = torch.softmax(logits, dim=1)
            n = probs.shape[1]
            names = class_names if len(class_names) == n else [str(j) for j in range(n)]
            for i in range(probs.shape[0]):
                row = probs[i]
                top = int(torch.argmax(row).item())
                conf = float(row[top].item())
                results.append(PredictionResult(
                    task=task,
                    pred_class=top,
                    class_name=names[top],
                    score=conf,
                    probabilities={names[j]: float(row[j].item()) for j in range(n)},
                    confidence=conf,
                ))
        return results

    def predict(self, data):
        import torch

        if not torch.is_tensor(data):
            data = torch.as_tensor(data, dtype=torch.float32)
        if data.dim() == 3:
            logits = self.predict_logits(data.unsqueeze(0))
            return self.postprocess(logits)[0]
        logits = self.predict_logits(data)
        return self.postprocess(logits)

    def predict_batch(self, tensor, *, batch_size=32, device=None):
        import torch

        self._build()
        if not torch.is_tensor(tensor):
            tensor = torch.as_tensor(tensor, dtype=torch.float32)
        dev = device or self._resolved_device
        self._net.to(dev)
        self._net.eval()
        rows = []
        n = tensor.shape[0]
        with torch.no_grad():
            for start in range(0, n, batch_size):
                chunk = tensor[start:start + batch_size].to(dev)
                logits = self._net(chunk).cpu()
                for result in self.postprocess(logits):
                    rows.append(result.to_dict())
        return rows

    # -- weights / device / introspection ------------------------------------

    def load_weights(self, path):
        import torch
        from ripple.models.exceptions import ModelLoadError

        self._build()
        try:
            state = torch.load(path, map_location="cpu", weights_only=True)
            if isinstance(state, dict) and "state_dict" in state:
                state = state["state_dict"]
            self._net.load_state_dict(state, strict=True)
        except Exception as exc:
            raise ModelLoadError(
                "failed to load weights from {!r}: {}".format(path, exc)
            ) from exc

    def load_encoder_weights(self, path):
        from ripple.models.exceptions import ModelLoadError

        self._build()
        try:
            from ripple.models.ssl.checkpoint import load_encoder_state
            ckpt = load_encoder_state(path)
            sd = ckpt["encoder_state_dict"] if isinstance(ckpt, dict) and \
                "encoder_state_dict" in ckpt else ckpt
            if isinstance(ckpt, dict) and "state_dict" in ckpt and \
                    "encoder_state_dict" not in ckpt:
                sd = ckpt["state_dict"]
            result = self._net.encoder.load_state_dict(sd, strict=False)
            missing = [k for k in result.missing_keys
                       if not k.endswith("num_batches_tracked")]
            unexpected = [k for k in result.unexpected_keys
                          if not k.endswith("num_batches_tracked")]
            if missing or unexpected:
                raise ModelLoadError(
                    "encoder weight mismatch loading {!r}: missing={}, unexpected={}"
                    .format(path, missing, unexpected))
        except ModelLoadError:
            raise
        except Exception as exc:
            raise ModelLoadError(
                "failed to load encoder weights from {!r}: {}".format(path, exc)
            ) from exc

    def to(self, device):
        self._build()
        self._net.to(device)
        self._device = device
        return self

    def eval(self):
        self._build()
        self._net.eval()
        return self

    @property
    def model_info(self):
        return {
            "type": getattr(self.config, "model_type", None),
            "task": getattr(self.config, "task", None),
            "encoder": getattr(self.config, "encoder", None),
            "num_classes": getattr(self.config, "num_classes", None),
            "device": self._device,
            "loaded": self._net is not None,
        }
