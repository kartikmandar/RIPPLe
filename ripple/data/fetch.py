"""Download DeepLense GSoC datasets from Google Drive (gdown lazy, stdlib unzip).

``gdown`` is an optional dependency (``pip install 'ripple[data]'``); it is imported
inside ``fetch()`` so this module and the DATASETS table import without it. Downloads
land in ``data/deeplense`` (already gitignored). Idempotent unless ``force=True``.
"""
import argparse
import os
import zipfile

from ripple.data.exceptions import DataFetchError

DATASETS = {
    "testII": {
        "gdrive_id": "1doUhVoq1-c9pamZVLpvjW1YRDMkKO1Q5",
        "filename": "testII.zip",
        "expected_dirs": ("train_lenses", "train_nonlenses", "test_lenses", "test_nonlenses"),
        "description": "GSoC Test II binary lens-finding, (3,64,64) g,r,i, imbalanced.",
    },
    "testVI_3class": {
        "gdrive_id": "1znqUeFzYz-DeAE3dYXD17qoMPK82Whji",
        "filename": "testVI_3class.zip",
        "expected_dirs": ("no_sub", "cdm", "axion"),
        "description": "Test VI foundation corpus, 3-class substructure, single-channel.",
    },
}


def fetch(name, dest="data/deeplense", force=False, unzip=True):
    """Download dataset ``name`` into ``dest`` and return the dataset directory path.

    Raises DataFetchError on unknown name or when gdown is not installed.
    """
    if name not in DATASETS:
        raise DataFetchError(
            f"Unknown dataset {name!r}; known: {sorted(DATASETS)}"
        )
    spec = DATASETS[name]
    os.makedirs(dest, exist_ok=True)
    out_dir = os.path.join(dest, name)
    if os.path.isdir(out_dir) and not force:
        return out_dir

    try:
        import gdown
    except ImportError as exc:
        raise DataFetchError(
            "gdown is required to fetch datasets; install with: pip install 'ripple[data]' "
            f"(or download {spec['filename']} manually from Google Drive id "
            f"{spec['gdrive_id']} into {dest})"
        ) from exc

    zip_path = os.path.join(dest, spec["filename"])
    url = f"https://drive.google.com/uc?id={spec['gdrive_id']}"
    try:
        gdown.download(url, zip_path, quiet=False)
    except Exception as exc:  # gdown raises a variety of errors on failure
        raise DataFetchError(f"Download of {name} failed: {exc}") from exc

    if not os.path.exists(zip_path):
        raise DataFetchError(f"Download did not produce {zip_path}")

    if unzip:
        os.makedirs(out_dir, exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(out_dir)
        except zipfile.BadZipFile as exc:
            raise DataFetchError(f"{zip_path} is not a valid zip: {exc}") from exc
    return out_dir


def main(argv=None):
    """Console-script entry point (ripple-fetch-data)."""
    parser = argparse.ArgumentParser(prog="ripple-fetch-data", description=__doc__)
    parser.add_argument("name", choices=sorted(DATASETS), help="dataset key to fetch")
    parser.add_argument("--dest", default="data/deeplense", help="download destination root")
    parser.add_argument("--force", action="store_true", help="re-download even if present")
    parser.add_argument("--no-unzip", dest="unzip", action="store_false", help="skip unzip")
    args = parser.parse_args(argv)
    try:
        out_dir = fetch(args.name, dest=args.dest, force=args.force, unzip=args.unzip)
    except DataFetchError as exc:
        parser.exit(status=2, message=f"error: {exc}\n")
    print(out_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
