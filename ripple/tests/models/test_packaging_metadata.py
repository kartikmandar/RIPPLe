"""Packaging metadata for the Phase-3 model layer (offline, stdlib-only)."""
import pathlib
import tomllib


def _load_pyproject():
    root = pathlib.Path(__file__).resolve().parents[3]
    with open(root / "pyproject.toml", "rb") as fh:
        return tomllib.load(fh)


def test_runtime_dependencies_stay_empty():
    data = _load_pyproject()
    assert data["project"]["dependencies"] == []


def test_data_extra_declares_gdown():
    data = _load_pyproject()
    extras = data["project"]["optional-dependencies"]
    assert "data" in extras
    assert extras["data"] == ["gdown>=5"]
    # The pre-existing rsp extra must survive untouched.
    assert "rsp" in extras


def test_console_scripts_present():
    data = _load_pyproject()
    scripts = data["project"]["scripts"]
    assert scripts["ripple-fetch-data"] == "ripple.data.fetch:main"
    assert scripts["ripple-ingest-deeplense"] == "ripple.data.ingest_deeplense:main"
