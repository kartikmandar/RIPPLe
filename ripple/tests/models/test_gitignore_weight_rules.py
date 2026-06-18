"""Guard that model-weight cache artifacts are gitignored (offline, stdlib-only)."""
import pathlib


def _gitignore_lines():
    root = pathlib.Path(__file__).resolve().parents[3]
    text = (root / ".gitignore").read_text(encoding="utf-8")
    return {line.strip() for line in text.splitlines()}


def test_models_cache_ignored():
    assert "models_cache/" in _gitignore_lines()


def test_torch_checkpoints_ignored():
    assert "*.pt" in _gitignore_lines()
