"""Offline tests for the ripple.data acquisition package (no network, no gdown)."""
import builtins

import pytest


def test_data_package_imports_without_gdown():
    import ripple.data  # must succeed even though gdown is not installed
    assert ripple.data is not None


def test_datafetcherror_is_exception():
    from ripple.data.exceptions import DataFetchError
    assert issubclass(DataFetchError, Exception)
    err = DataFetchError("boom")
    assert str(err) == "boom"


def test_datasets_have_expected_ids():
    from ripple.data.fetch import DATASETS
    assert DATASETS["testII"]["gdrive_id"] == "1doUhVoq1-c9pamZVLpvjW1YRDMkKO1Q5"
    assert DATASETS["testVI_3class"]["gdrive_id"] == "1znqUeFzYz-DeAE3dYXD17qoMPK82Whji"
    for spec in DATASETS.values():
        assert "filename" in spec and "expected_dirs" in spec


def test_fetch_unknown_name_raises(tmp_path):
    from ripple.data.fetch import fetch
    from ripple.data.exceptions import DataFetchError
    with pytest.raises(DataFetchError):
        fetch("does_not_exist", dest=str(tmp_path))


def test_fetch_raises_when_gdown_absent(tmp_path, monkeypatch):
    from ripple.data.fetch import fetch
    from ripple.data.exceptions import DataFetchError

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "gdown" or name.startswith("gdown."):
            raise ImportError("No module named 'gdown'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(DataFetchError):
        fetch("testII", dest=str(tmp_path), force=True)


def test_main_help_exits_zero():
    """main(["--help"]) should raise SystemExit(0) (argparse)."""
    from ripple.data.fetch import main
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0


def test_main_unknown_dataset_exits_nonzero():
    """main with an unknown dataset name should exit with non-zero status."""
    from ripple.data.fetch import main
    # argparse will exit(2) for invalid choices
    with pytest.raises(SystemExit) as exc_info:
        main(["not_a_real_dataset"])
    assert exc_info.value.code != 0
