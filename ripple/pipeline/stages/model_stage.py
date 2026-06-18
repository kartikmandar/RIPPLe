"""Model stage: builds a model via the registry/factory and runs prediction,
evaluation, or super-resolution against the canonical Phase-2 tensor.

Importing this module is torch-free: ``ripple.models`` exposes ModelFactory and
the inference/predictions helpers without importing torch at top level; torch is
only pulled inside ModelFactory.create (lazy builders). The stage is defensive:
any model failure logs and returns the input data unchanged, mirroring
PreprocessingStage, so a model error never crashes the pipeline run.
"""
import os
from typing import Any, Dict, List, Optional

import numpy as np

from ripple.utils.logger import Logger
from ripple.pipeline.pipeline_stage import PipelineStage
from ripple.models import ModelFactory
from ripple.models.inference import predict_batch
from ripple.models.predictions import write_predictions


class ModelStage(PipelineStage):
    """Pipeline stage for model operations (prediction / evaluation / SR)."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the ModelStage.

        Self-heals BOTH call shapes:
          * the full pipeline config dict (a top-level ``model`` key), as passed
            by ``pipeline_builder``; and
          * a bare model sub-dict (no ``model`` key), the legacy shape.
        Resolution is ``model_cfg = cfg.get('model', cfg)`` so a sub-dict that
        was already extracted is used as-is instead of re-indexing to ``{}``.
        """
        super().__init__(config)
        cfg = self.config if hasattr(self.config, "get") else {}
        model_cfg = cfg.get("model", cfg)
        if not hasattr(model_cfg, "get"):
            model_cfg = {}
        self.model_operation = model_cfg.get("operation", "prediction")
        self.model_type = model_cfg.get("type", "resnet_binary")
        self.model_params = model_cfg.get("params", {}) or {}

    def execute(self, data: Any = None) -> Any:
        """Execute the model stage, dispatching on the configured operation."""
        Logger.info(
            f"Executing Model Stage (Operation: {self.model_operation}, "
            f"Type: {self.model_type})"
        )

        if not isinstance(data, dict):
            Logger.warning("Model Stage received no data dict. Skipping.")
            return data

        try:
            if self.model_operation == "training":
                Logger.info("Training is out-of-suite (python -m ripple.models.cli.train).")
                return data
            if self.model_operation == "evaluation":
                return self._run_evaluation(data)
            if self.model_operation == "prediction":
                return self._run_prediction(data)
            Logger.warning(f"Unknown model operation: {self.model_operation}. Skipping.")
            return data
        except Exception as exc:  # never crash the run on a model failure
            Logger.error(f"Model Stage failed: {exc}")
            return data

    def _resolve_out_dir(self, data: Dict[str, Any]) -> Optional[str]:
        """Resolve the directory to write predictions.csv / enhanced cutouts.

        Prefers an explicit ``data['out_dir']``, then the output config block
        (``data['output']['directory']``), then the stage's own config.
        """
        out_dir = data.get("out_dir")
        if not out_dir:
            output_cfg = data.get("output", {})
            if hasattr(output_cfg, "get"):
                out_dir = output_cfg.get("directory")
        if not out_dir and hasattr(self.config, "get"):
            output_cfg = self.config.get("output", {})
            if hasattr(output_cfg, "get"):
                out_dir = output_cfg.get("directory")
        if out_dir:
            os.makedirs(str(out_dir), exist_ok=True)
        return str(out_dir) if out_dir else None

    def _accepted_rows(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Manifest rows for accepted cutouts, in tensor row order.

        Recomputes ``accepted = [r for r in manifest if status == 'accepted']``
        so tensor row j <-> accepted_rows[j], honouring the load-bearing
        index/accepted_indices/manifest/predictions join invariant.
        """
        manifest = data.get("preprocess_manifest", []) or []
        return [r for r in manifest if r.get("status") == "accepted"]

    def _run_prediction(self, data: Dict[str, Any], model: Any = None) -> Dict[str, Any]:
        """Build the model, score the accepted tensor rows, write predictions.csv.

        ``model`` may be supplied by ``_run_evaluation`` so the same built model
        is reused for both the prediction write and the metric computation.
        """
        tensor = data.get("tensor")
        if tensor is None or len(tensor) == 0:
            Logger.warning("No tensor available for prediction. Skipping model stage.")
            return data

        if model is None:
            model = ModelFactory.create(self.model_type, self.model_params)

        # Super-resolution models are image producers, not catalog producers.
        if getattr(model, "OUTPUT_KIND", "catalog") == "image":
            return self._run_super_resolution(data, model)

        preds = predict_batch(model, tensor)
        accepted = self._accepted_rows(data)
        if len(accepted) != len(preds):
            Logger.warning(
                f"Prediction count ({len(preds)}) != accepted manifest rows "
                f"({len(accepted)}); joining on the shorter length."
            )

        join_keys = ("index", "ra", "dec", "tract", "patch", "label")
        rows: List[Dict[str, Any]] = []
        for row, pred in zip(accepted, preds):
            merged = {k: row.get(k) for k in join_keys}
            merged.update(pred)
            merged["model_name"] = self.model_type
            merged["model_type"] = self.model_type
            rows.append(merged)

        out_dir = self._resolve_out_dir(data)
        if out_dir is not None:
            csv_path = os.path.join(out_dir, "predictions.csv")
            write_predictions(rows, csv_path)
            Logger.success(f"Wrote {len(rows)} predictions to {csv_path}")
        else:
            Logger.warning("No out_dir resolved; predictions not written to disk.")

        data["predictions"] = rows
        Logger.success("Model Stage (prediction) completed")
        return data

    def _run_evaluation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Run prediction (CSV + scored rows) AND compute classifier metrics.

        Builds the model once, runs the prediction path (so ``predictions.csv``
        and ``data['predictions']`` are produced), then constructs a loader from
        the canonical tensor + accepted-row labels and computes metrics via the
        canonical ``ModelEvaluator``. torch + evaluator are imported lazily.
        If there are no labeled rows (all ``label == -1``), ``data['metrics']``
        is set to ``{}`` and a warning logged instead of crashing.
        """
        tensor = data.get("tensor")
        if tensor is None or len(tensor) == 0:
            Logger.warning("No tensor available for evaluation. Skipping model stage.")
            return data

        model = ModelFactory.create(self.model_type, self.model_params)

        # SR models have no classifier metrics; route to the image branch.
        if getattr(model, "OUTPUT_KIND", "catalog") == "image":
            return self._run_super_resolution(data, model)

        # Prediction path first (predictions.csv + data['predictions']).
        self._run_prediction(data, model=model)

        import torch
        from torch.utils.data import TensorDataset, DataLoader

        from ripple.models.model_evaluator import ModelEvaluator

        accepted = self._accepted_rows(data)
        labels = [
            int(r["label"]) if r.get("label") is not None else -1
            for r in accepted
        ]
        if not labels or all(lbl == -1 for lbl in labels):
            Logger.warning("No labeled rows for evaluation; metrics empty.")
            data["metrics"] = {}
            Logger.success("Model Stage (evaluation) completed")
            return data

        y = torch.tensor(labels, dtype=torch.long)
        batch_size = self.model_params.get("batch_size", 32)
        loader = DataLoader(
            TensorDataset(tensor, y), batch_size=batch_size, shuffle=False
        )
        task = getattr(getattr(model, "config", None), "task", "binary")
        metrics = ModelEvaluator(task).evaluate(model, loader)
        data["metrics"] = metrics
        Logger.success("Model Stage (evaluation) completed")
        return data

    def _run_super_resolution(self, data: Dict[str, Any], model: Any) -> Dict[str, Any]:
        """Save enhanced .npy cutouts + enhanced_manifest.csv (image-out model)."""
        tensor = data.get("tensor")
        result = model.predict(data={"tensor": tensor}, return_image=True)
        images = result.get("output_image")
        if images is None:
            Logger.warning("SR model produced no output_image. Skipping write.")
            return data

        accepted = self._accepted_rows(data)
        out_dir = self._resolve_out_dir(data)
        enhanced_rows: List[Dict[str, Any]] = []
        for j, row in enumerate(accepted):
            idx = row.get("index", j)
            path = ""
            if out_dir is not None:
                path = os.path.join(out_dir, f"enhanced_{idx:06d}.npy")
                arr = images[j]
                arr = arr.detach().cpu().numpy() if hasattr(arr, "detach") else np.asarray(arr)
                np.save(path, arr)
            enhanced_rows.append({
                "index": idx, "ra": row.get("ra"), "dec": row.get("dec"),
                "tract": row.get("tract"), "patch": row.get("patch"),
                "label": row.get("label"), "path": path,
                "scale": result.get("scale"), "model_type": self.model_type,
            })

        if out_dir is not None and enhanced_rows:
            import csv as _csv
            manifest_path = os.path.join(out_dir, "enhanced_manifest.csv")
            with open(manifest_path, "w", newline="") as fh:
                writer = _csv.DictWriter(fh, fieldnames=list(enhanced_rows[0].keys()))
                writer.writeheader()
                writer.writerows(enhanced_rows)
            Logger.success(f"Wrote {len(enhanced_rows)} enhanced cutouts to {out_dir}")

        data["enhanced_manifest"] = enhanced_rows
        Logger.success("Model Stage (super-resolution) completed")
        return data
