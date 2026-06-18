import pytest
from ripple.preprocessing.manifest import MANIFEST_FIELDS, write_manifest, read_manifest


def test_roundtrip_preserves_rows(tmp_path):
    rows = [
        {"path": "a.npy", "label": 1, "ra": 10.0, "dec": -30.0, "status": "accepted",
         "group_key": "t1_p2", "norm_method": "asinh", "bad_fraction": 0.0},
        {"path": "", "label": 0, "ra": 11.0, "dec": -31.0, "status": "rejected",
         "reject_reason": "soft: bad_fraction", "group_key": "t1_p2"},
    ]
    p = tmp_path / "manifest.csv"
    write_manifest(rows, p)
    back = read_manifest(p)
    assert len(back) == 2
    assert back[0]["status"] == "accepted"
    assert back[0]["ra"] == 10.0 and isinstance(back[0]["ra"], float)
    assert back[0]["label"] == 1 and isinstance(back[0]["label"], int)


def test_all_fields_written_even_if_missing(tmp_path):
    p = tmp_path / "m.csv"
    write_manifest([{"path": "x.npy", "status": "accepted"}], p)
    back = read_manifest(p)
    assert set(MANIFEST_FIELDS) <= set(back[0].keys())


def test_read_missing_file_raises(tmp_path):
    from ripple.preprocessing.exceptions import ManifestError
    with pytest.raises(ManifestError):
        read_manifest(tmp_path / "nope.csv")
