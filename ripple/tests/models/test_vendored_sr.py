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
