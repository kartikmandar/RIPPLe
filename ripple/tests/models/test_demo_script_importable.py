"""The out-of-suite real-Butler demo must import torch-free and expose entry points."""
import importlib.util
import pathlib


def _load_demo_module():
    root = pathlib.Path(__file__).resolve().parents[3]
    path = root / "scripts" / "demo_local_inference.py"
    spec = importlib.util.spec_from_file_location("demo_local_inference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_demo_imports_and_exposes_entry_points():
    module = _load_demo_module()
    assert callable(module.main)
    assert callable(module.run)
    assert callable(module.build_parser)


def test_demo_parser_defaults():
    module = _load_demo_module()
    parser = module.build_parser()
    args = parser.parse_args([])
    assert args.repo.endswith("DATA_REPO")
    assert args.collection == "demo_collection"
    assert args.data_release == "dp02"
    assert args.model_type == "resnet_binary"
