"""Dense, order-preserving inference helpers.

``predict_batch`` runs a model over an in-memory ``(N, C, H, W)`` tensor and
returns one dict per row in input order. ``predict_dataset`` does the same from
an on-disk ``RippleCutoutDataset`` via ``make_dataloader(shuffle=False)`` so the
returned order matches dataset (and therefore manifest) order. torch is imported
lazily so ``import ripple.models`` succeeds without torch.
"""


def predict_batch(model, tensor, *, batch_size=32, device=None):
    """Run ``model`` over ``tensor`` -> list[dict], one row per input, in order.

    Applies its own ``eval`` + ``no_grad`` guard, then delegates to the
    model's contract ``predict_batch`` (chunking lives in ``BaseModel``),
    keeping a single inference codepath. The nested ``no_grad`` with
    ``BaseModel``'s own is harmless.
    """
    import torch  # local lazy import keeps the module torch-free at import

    model.eval()
    with torch.no_grad():
        return model.predict_batch(tensor, batch_size=batch_size, device=device)


def predict_dataset(model, dataset, *, batch_size=32, device=None):
    """Run ``model`` over an on-disk dataset -> list[dict] in dataset order.

    Loads with ``make_dataloader(shuffle=False)`` so the i-th returned row
    corresponds to the i-th dataset item (and its manifest ``index``).
    """
    import torch

    from ripple.preprocessing.dataset import make_dataloader

    loader = make_dataloader(
        dataset, batch_size=batch_size, shuffle=False, num_workers=0
    )
    batches = []
    for x, _y in loader:
        batches.append(x)
    if not batches:
        return []
    tensor = torch.cat(batches, dim=0)
    return predict_batch(model, tensor, batch_size=batch_size, device=device)
