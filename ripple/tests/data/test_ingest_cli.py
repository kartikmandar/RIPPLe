"""Offline test for the ripple-ingest-deeplense CLI wrapper."""
import os

import numpy as np
import pytest


def _make_fixture(root):
    for cls in ("train_lenses", "train_nonlenses", "test_lenses", "test_nonlenses"):
        d = os.path.join(root, cls)
        os.makedirs(d, exist_ok=True)
        for k in range(4):
            np.save(os.path.join(d, f"{cls}_{k}.npy"),
                    np.random.rand(3, 8, 8).astype(np.float32))


def test_ingest_cli_writes_manifest(tmp_path, capsys):
    from ripple.data.ingest_deeplense import main
    from ripple.preprocessing import read_manifest

    src = tmp_path / "src"
    out = tmp_path / "out"
    _make_fixture(str(src))

    rc = main(["--src", str(src), "--out", str(out),
               "--val-fraction", "0.5", "--seed", "0"])
    assert rc == 0

    manifest_path = os.path.join(str(out), "manifest.csv")
    assert os.path.exists(manifest_path)
    rows = read_manifest(manifest_path)
    assert rows and all(r["status"] == "accepted" for r in rows)
    printed = capsys.readouterr().out
    assert "manifest.csv" in printed


def test_help_exits_zero():
    from ripple.data.ingest_deeplense import main

    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_bad_src_errors_cleanly(tmp_path):
    from ripple.data.ingest_deeplense import main

    src = tmp_path / "does_not_exist"
    out = tmp_path / "out"

    # A nonexistent source root surfaces the converter's FileNotFoundError
    # (from os.listdir) rather than silently writing a broken manifest.
    with pytest.raises(FileNotFoundError):
        main(["--src", str(src), "--out", str(out)])

    assert not os.path.exists(os.path.join(str(out), "manifest.csv"))
