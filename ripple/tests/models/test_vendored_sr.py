import pytest

torch = pytest.importorskip("torch")


@pytest.mark.torch
def test_sisr_random_init_forward_shape():
    from ripple.models.vendored.anirudh_sr import SISR

    net = SISR(magnification=2, n_mag=1, residual_depth=3,
               in_channels=1, latent_channel_count=64)
    net.eval()
    x = torch.rand(2, 1, 64, 64)
    with torch.no_grad():
        out = net(x)
    assert tuple(out.shape) == (2, 2, 128, 128)
    assert out.dtype == torch.float32


@pytest.mark.torch
def test_rcan_random_init_forward_shape():
    from ripple.models.vendored.anirudh_sr import RCAN

    net = RCAN(scale=2, latent_dim=128, num_rg=2, num_rcab=2,
               reduction=2, in_channels=1, out_channels=2)
    net.eval()
    x = torch.rand(2, 1, 64, 64)
    with torch.no_grad():
        out = net(x)
    assert tuple(out.shape) == (2, 2, 128, 128)


@pytest.mark.torch
def test_sersic_profiler_reconstruction_methods_exist():
    from ripple.models.vendored.anirudh_sr import SersicProfiler

    prof = SersicProfiler(resolution=0.05, device="cpu", sersic_args=[1, 1, 0.25])
    for name in ("create_lensing", "get_sample", "sersic_law",
                 "approximate_center", "origin_shift", "create_source",
                 "create_sersic"):
        assert hasattr(prof, name)
    # get_sample reconstructs an image from a deflection field on CPU.
    alpha = torch.rand(1, 2, 128, 128) * 0.5
    lr = torch.rand(1, 1, 64, 64)
    sersic_profile, lr_out, source = prof.get_sample(alpha, lr, plot=False)
    assert sersic_profile.shape[-1] == 128
    assert tuple(lr_out.shape) == (1, 64, 64)


@pytest.mark.torch
def test_vendored_module_imports_no_pil_matplotlib():
    import ripple.models.vendored.anirudh_sr as mod

    src = open(mod.__file__).read()
    assert "matplotlib" not in src
    assert "from PIL" not in src and "import PIL" not in src


# ---------------------------------------------------------------------------
# Task 19: VendoredSRAdapter tests
# ---------------------------------------------------------------------------

@pytest.mark.torch
def test_adapter_preprocess_collapses_3ch_to_1_and_scales():
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter

    cfg = ModelConfig(model_type="anirudh_sr", task="super_res",
                      in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)
    x = torch.rand(2, 3, 64, 64) * 10.0 + 5.0
    out = adapter.preprocess(x)
    assert tuple(out.shape) == (2, 1, 64, 64)
    assert float(out.min()) >= 0.0 and float(out.max()) <= 1.0


@pytest.mark.torch
def test_adapter_predict_random_init_returns_alpha_and_image():
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter

    cfg = ModelConfig(model_type="anirudh_sr", task="super_res",
                      in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)
    x = torch.rand(2, 3, 64, 64)
    result = adapter.predict({"tensor": x}, return_image=True)
    assert tuple(result["alpha"].shape) == (2, 2, 128, 128)
    assert result["scale"] == 2
    assert tuple(result["output_image"].shape) == (2, 1, 128, 128)
    assert result["alpha"].device.type == "cpu"


@pytest.mark.torch
def test_adapter_predict_accepts_bare_tensor():
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter

    cfg = ModelConfig(model_type="anirudh_sr", task="super_res", in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)
    result = adapter.predict(torch.rand(1, 3, 64, 64))
    assert tuple(result["alpha"].shape) == (1, 2, 128, 128)


@pytest.mark.torch
def test_rcan_adapter_builds_rcan_net():
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter
    from ripple.models.vendored.anirudh_sr import RCAN

    cfg = ModelConfig(model_type="anirudh_sr_rcan", task="super_res", in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)
    adapter._build()
    assert isinstance(adapter._net, RCAN)


def test_sr_keys_registered_torch_free():
    # Registry listing must work without torch (light-import rule).
    import ripple.models.vendored_sr_adapter  # noqa: F401  populates registry
    from ripple.models.model_registry import list_models, get

    keys = list_models()
    assert "anirudh_sr" in keys
    assert "anirudh_sr_rcan" in keys
    assert callable(get("anirudh_sr"))


@pytest.mark.torch
def test_factory_creates_sr_adapter():
    from ripple.models.model_factory import ModelFactory
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter

    model = ModelFactory.create("anirudh_sr",
                                {"model_type": "anirudh_sr", "task": "super_res",
                                 "in_channels": 1, "device": "cpu"})
    assert isinstance(model, VendoredSRAdapter)
    assert model.OUTPUT_KIND == "image"


@pytest.mark.torch
def test_adapter_load_weights_bad_path_raises_model_load_error(tmp_path):
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter
    from ripple.models.exceptions import ModelLoadError

    cfg = ModelConfig(model_type="anirudh_sr", task="super_res", in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)
    bad = tmp_path / "not_a_checkpoint.pt"
    bad.write_bytes(b"garbage-not-a-torch-file")
    with pytest.raises(ModelLoadError):
        adapter.load_weights(str(bad))


@pytest.mark.torch
def test_adapter_load_weights_wrong_shape_raises_model_load_error(tmp_path):
    # A SISR-shaped adapter cannot strict-load an RCAN state dict.
    from ripple.models.config import ModelConfig
    from ripple.models.vendored_sr_adapter import VendoredSRAdapter
    from ripple.models.vendored.anirudh_sr import RCAN
    from ripple.models.exceptions import ModelLoadError

    rcan = RCAN(scale=2, latent_dim=16, num_rg=1, num_rcab=1, reduction=2,
                in_channels=1, out_channels=2)
    ckpt = tmp_path / "rcan_sd.pt"
    torch.save(rcan.state_dict(), str(ckpt))

    cfg = ModelConfig(model_type="anirudh_sr", task="super_res", in_channels=1, device="cpu")
    adapter = VendoredSRAdapter(cfg)  # builds a SISR
    with pytest.raises(ModelLoadError):
        adapter.load_weights(str(ckpt))
