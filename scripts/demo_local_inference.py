"""End-to-end real-data inference demo (OUT-OF-SUITE, never run by pytest).

Pulls a real LSST-pipeline ``calexp`` from the local Butler repo, slices it
into a grid of (cutout_size x cutout_size) patches, runs the canonical Phase-2
preprocessing, builds a RIPPLe ``resnet_binary`` classifier and writes a
lens-candidate ``predictions.csv``.

This proves the full Phase-1 -> Phase-2 -> Phase-3 path on genuine local
imagery; the model weights are random-init unless ``--checkpoint`` is supplied,
so scores are plumbing-correct but NOT meaningful predictions without a trained
checkpoint.

Requires the activated LSST stack and the local demo repo::

    source ~/lsst_stack/loadLSST.zsh && setup lsst_distrib
    python scripts/demo_local_inference.py \\
        --repo data/pipelines_check-29.1.1/DATA_REPO \\
        --collection demo_collection --data-release dp02 \\
        --out demo_out
"""
import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python scripts/demo_local_inference.py",
        description=(
            "Real local-Butler -> calexp -> grid patches -> preprocess "
            "-> resnet_binary -> predictions.csv."
        ),
    )
    parser.add_argument(
        "--repo",
        default="data/pipelines_check-29.1.1/DATA_REPO",
        help="Local Butler repo path (legacy HSC dataset naming).",
    )
    parser.add_argument(
        "--collection",
        default="demo_collection",
        help="Butler collection holding the calexp.",
    )
    parser.add_argument(
        "--data-release",
        default="dp02",
        dest="data_release",
        help="Release tag for dataset-name resolution (dp02 = legacy HSC).",
    )
    parser.add_argument(
        "--visit",
        type=int,
        default=None,
        help="calexp visit id (defaults to the first available).",
    )
    parser.add_argument(
        "--detector",
        type=int,
        default=None,
        help="calexp detector id (defaults to the first available).",
    )
    parser.add_argument(
        "--checkpoint",
        default=None,
        help=(
            "Optional trained checkpoint (.pt) to load. "
            "If omitted the model is random-init (plumbing demo only; "
            "scores are NOT meaningful without a trained checkpoint)."
        ),
    )
    parser.add_argument(
        "--model-type",
        default="resnet_binary",
        dest="model_type",
        help="Registry key to build for inference.",
    )
    parser.add_argument(
        "--n-cutouts",
        type=int,
        default=8,
        dest="n_cutouts",
        help="Number of grid patches to extract from the calexp.",
    )
    parser.add_argument(
        "--cutout-size",
        type=int,
        default=64,
        dest="cutout_size",
        help="Side length of each square patch in pixels (default 64).",
    )
    parser.add_argument(
        "--out",
        default="demo_out",
        help="Output directory for manifest.csv + predictions.csv.",
    )
    return parser


def _discover_calexp(butler, collection):
    """Return the first (visit, detector) pair found in the Butler collection."""
    ref = next(iter(butler.registry.queryDatasets("calexp", collections=[collection])))
    required = dict(ref.dataId.required)
    return int(required["visit"]), int(required["detector"])


def _calexp_to_items(exposure, n_cutouts, cutout_size, bands=("g", "r", "i")):
    """Slice the calexp image array into a grid of ``n_cutouts`` patches.

    A single calexp is one-band imagery.  We broadcast that band across the
    three RIPPLe channels (g, r, i) so the Preprocessor can build a 3-channel
    tensor.  The patches are drawn from a regular grid across the image centre,
    skipping border regions to avoid edge effects.

    Returns a list of items in the format expected by ``Preprocessor.run()``:
        [{"bands": {band: np.ndarray, ...}, "meta": {...}}, ...]
    """
    import numpy as np

    arr = exposure.getMaskedImage().getImage().getArray().astype(np.float32)
    h, w = arr.shape

    # Collect equally-spaced grid centres, staying at least cutout_size/2
    # away from every edge.
    margin = cutout_size // 2 + 1
    usable_h = h - 2 * margin
    usable_w = w - 2 * margin
    if usable_h <= 0 or usable_w <= 0:
        raise ValueError(
            f"calexp ({h}x{w}) is too small for {cutout_size}px patches with "
            f"margin {margin}px."
        )

    cols = max(1, int(n_cutouts ** 0.5))
    rows = max(1, (n_cutouts + cols - 1) // cols)
    total = rows * cols

    cy_list = [margin + int(usable_h * (r + 0.5) / rows) for r in range(rows)]
    cx_list = [margin + int(usable_w * (c + 0.5) / cols) for c in range(cols)]

    items = []
    for idx, (cy, cx) in enumerate(
        (cy, cx) for cy in cy_list for cx in cx_list
    ):
        if idx >= n_cutouts:
            break
        patch = arr[
            cy - cutout_size // 2: cy + cutout_size // 2,
            cx - cutout_size // 2: cx + cutout_size // 2,
        ]
        # Broadcast single band to all requested channels.
        band_dict = {b: patch.copy() for b in bands}
        items.append({
            "bands": band_dict,
            "meta": {"index": idx, "ra": None, "dec": None},
        })

    return items


def run(args) -> int:
    # All heavy / LSST-stack imports are deferred so the module is torch-free
    # at import time and the offline test suite can load it without the stack.
    import os

    from lsst.daf.butler import Butler

    from ripple.preprocessing.config import PreprocessingConfig
    from ripple.preprocessing.preprocessor import Preprocessor
    from ripple.models.config import ModelConfig
    from ripple.models.model_factory import ModelFactory
    from ripple.models.inference import predict_batch
    from ripple.models.predictions import write_predictions

    os.makedirs(args.out, exist_ok=True)

    # 1. Open the local Butler and resolve a (visit, detector) if not supplied.
    print(f"[demo] opening Butler at {args.repo!r} collection={args.collection!r} "
          f"data_release={args.data_release!r}")
    butler = Butler(args.repo, collections=[args.collection])

    if args.visit is None or args.detector is None:
        visit, detector = _discover_calexp(butler, args.collection)
        print(f"[demo] auto-discovered visit={visit} detector={detector}")
    else:
        visit, detector = args.visit, args.detector

    # 2. Retrieve the real calexp.
    from ripple.data_access.config_examples import ButlerConfig
    from ripple.data_access.butler_client import ButlerClient

    config = ButlerConfig(
        repo_path=args.repo,
        collections=[args.collection],
        data_release=args.data_release,
    )
    client = ButlerClient(config=config)
    exposure = client.get_calexp(visit=visit, detector=detector)
    if exposure is None:
        print(
            f"[demo] ERROR: could not retrieve calexp for "
            f"visit={visit} detector={detector}",
            file=sys.stderr,
        )
        return 1

    dims = exposure.getDimensions()
    print(f"[demo] calexp retrieved: {dims.getX()}x{dims.getY()} pixels")

    # 3. Slice the calexp into a grid of patches.
    bands = ("g", "r", "i")
    items = _calexp_to_items(
        exposure,
        n_cutouts=args.n_cutouts,
        cutout_size=args.cutout_size,
        bands=bands,
    )
    print(f"[demo] extracted {len(items)} grid patches ({args.cutout_size}px)")

    # 4. Run canonical Phase-2 preprocessing -> (N, 3, 64, 64) float32 tensor.
    preproc_config = PreprocessingConfig(
        bands=bands,
        cutout_size=args.cutout_size,
        channels=3,
        partial_band_policy="zero_fill_band",
    )
    result = Preprocessor(preproc_config).run(items, out_dir=args.out)
    n_accepted = len(result.accepted_indices)
    print(f"[demo] preprocessing: {n_accepted}/{len(items)} patches accepted")

    if n_accepted == 0 or result.tensor is None:
        print("[demo] ERROR: no patches passed preprocessing", file=sys.stderr)
        return 1

    print(f"[demo] tensor shape: {tuple(result.tensor.shape)}")

    # 5. Build the classifier.
    if args.checkpoint is not None:
        # Load architecture from the checkpoint's own metadata and restore weights.
        import torch

        ckpt = torch.load(args.checkpoint, map_location="cpu", weights_only=False)
        if not isinstance(ckpt, dict) or "state_dict" not in ckpt:
            print(
                f"[demo] ERROR: {args.checkpoint!r} is not a RIPPLe checkpoint dict",
                file=sys.stderr,
            )
            return 1
        model_type = ckpt.get("model_type") or args.model_type
        task = ckpt.get("task") or "binary"
        classes = list(ckpt.get("classes") or ())
        num_classes = len(classes) if classes else 2
        input_size = int(ckpt.get("input_size") or args.cutout_size)
        cfg_dict = {
            "model_type": model_type,
            "task": task,
            "num_classes": num_classes,
            "input_size": input_size,
        }
        if classes:
            cfg_dict["class_names"] = classes
        model_config = ModelConfig.from_dict(cfg_dict)
        model = ModelFactory.create(model_type, model_config)
        model._build()
        model._net.load_state_dict(ckpt["state_dict"], strict=True)
        weights_desc = f"loaded from {args.checkpoint!r}"
    else:
        model_config = ModelConfig.from_dict({
            "model_type": args.model_type,
            "task": "binary",
            "input_size": args.cutout_size,
        })
        model = ModelFactory.create(args.model_type, model_config)
        weights_desc = "RANDOM-INIT (scores are NOT meaningful without a trained checkpoint)"

    print(f"[demo] model: {args.model_type}  weights: {weights_desc}")

    # 6. Run batched inference.
    predictions = predict_batch(model, result.tensor, batch_size=32)

    # 7. Join prediction rows to manifest by accepted_index and write CSV.
    manifest_by_index = {row["index"]: row for row in result.manifest}
    rows = []
    for pred, accepted_index in zip(predictions, result.accepted_indices):
        manifest_row = manifest_by_index.get(accepted_index, {})
        row = dict(pred)
        row["index"] = accepted_index
        for key in ("ra", "dec", "tract", "patch", "label"):
            if key in manifest_row:
                row[key] = manifest_row[key]
        row.setdefault("model_name", args.model_type)
        row.setdefault("model_type", args.model_type)
        rows.append(row)

    out_csv = os.path.join(args.out, "predictions.csv")
    write_predictions(rows, out_csv)

    # 8. Summary.
    print(f"[demo] wrote {len(rows)} predictions -> {out_csv}")
    print(f"[demo] manifest  -> {os.path.join(args.out, 'manifest.csv')}")
    if args.checkpoint is None:
        print(
            "[demo] NOTE: model is random-init; scores above are NOT meaningful "
            "predictions. Provide --checkpoint to score with a trained model."
        )
    return 0


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
