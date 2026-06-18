"""Offline, CPU-only tests for ripple.models.model_trainer."""
import dataclasses

from ripple.models.model_trainer import History


def test_history_fields_and_default():
    h = History(epochs=[], best_epoch=-1, best_metric=float("-inf"))
    assert h.epochs == []
    assert h.best_epoch == -1
    assert h.best_metric == float("-inf")
    assert h.best_state_dict is None
    names = [f.name for f in dataclasses.fields(History)]
    assert names == ["epochs", "best_epoch", "best_metric", "best_state_dict"]


def test_history_records_epochs():
    h = History(epochs=[{"epoch": 0, "train_loss": 1.0}], best_epoch=0, best_metric=0.5)
    assert h.epochs[0]["train_loss"] == 1.0
    assert h.best_metric == 0.5
