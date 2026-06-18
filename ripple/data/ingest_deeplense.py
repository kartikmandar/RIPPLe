"""Console-script wrapper (ripple-ingest-deeplense) around ingest_deeplense_dataset.

Converts a downloaded DeepLense class-folder dataset into a RIPPLe manifest + cutouts
that RippleCutoutDataset/make_dataloader consume directly. No heavy imports at module
top; the converter itself imports numpy and (lazily) torch only when used.
"""
import argparse

from ripple.preprocessing import ingest_deeplense_dataset


def main(argv=None):
    """Console-script entry point (ripple-ingest-deeplense)."""
    parser = argparse.ArgumentParser(prog="ripple-ingest-deeplense", description=__doc__)
    parser.add_argument("--src", required=True, help="dataset root (class folders of .npy)")
    parser.add_argument("--out", required=True, help="output dir for manifest.csv + cutouts")
    parser.add_argument("--val-fraction", type=float, default=0.1,
                        help="fraction of train groups carved into val")
    parser.add_argument("--band-order", default="g,r,i", help="channel band order label")
    parser.add_argument("--no-copy-arrays", dest="copy_arrays", action="store_false",
                        help="reference source .npy in place instead of copying")
    parser.add_argument("--seed", type=int, default=0, help="split seed")
    args = parser.parse_args(argv)

    manifest_path = ingest_deeplense_dataset(
        args.src, args.out,
        val_fraction=args.val_fraction,
        copy_arrays=args.copy_arrays,
        band_order=args.band_order,
        seed=args.seed,
    )
    print(manifest_path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
