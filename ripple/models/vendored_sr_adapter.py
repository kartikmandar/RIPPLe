"""Adapter wrapping Anirudh Shankar's vendored super-resolution nets behind the RIPPLe ModelInterface.

Proves RIPPLe is a model-agnostic bridge: a DeepLense model plugs into the same
``BaseModel`` surface (``predict``) as RIPPLe's own classifiers. Torch and the
vendored architectures are imported lazily (light-import rule).

Registry keys:
    anirudh_sr       -> SISR  (vanilla/Weights_*.pt, 1->2 channel deflection field)
    anirudh_sr_rcan  -> RCAN  (RCAN/Weights_*.pt,  1->2 channel deflection field)

Both nets emit a 2-channel deflection field alpha at (N, 2, 128, 128) from a
(N, 1, 64, 64) input; the reconstructed image is produced by the vendored
SersicProfiler.
"""
from .base_model import BaseModel
from .config import ModelConfig
from .exceptions import ModelError, ModelLoadError
from .model_registry import register


class VendoredSRAdapter(BaseModel):
    """Wraps SISR / RCAN as a RIPPLe BaseModel producing super-resolved cutouts."""

    task = "super_res"
    model_type = "super_resolution"
    OUTPUT_KIND = "image"

    def __init__(self, config: ModelConfig):
        super().__init__(config)
        # Architecture selected by the registry key stored in config.model_type.
        self._arch = getattr(config, "model_type", "anirudh_sr")

    def _build(self):
        """Lazily construct the vendored net for this adapter's registry key."""
        if self._net is not None:
            return
        import torch  # noqa: F401  (ensures torch present; nets need it)
        from .vendored.anirudh_sr import SISR, RCAN

        if self._arch == "anirudh_sr_rcan":
            self._net = RCAN(scale=2, latent_dim=128, num_rg=2, num_rcab=2,
                             reduction=2, in_channels=1, out_channels=2)
        else:
            self._net = SISR(magnification=2, n_mag=1, residual_depth=3,
                             in_channels=1, latent_channel_count=64)
        self._net.eval()

    def load_weights(self, path):
        """Strict-load real DeepLense weights (saved on CUDA -> map to CPU).

        weights_only=True is safe (verified: bare OrderedDict). RuntimeError from
        a strict-load mismatch and any load failure are wrapped as ModelLoadError.
        """
        import torch

        self._build()
        try:
            state_dict = torch.load(path, weights_only=True, map_location="cpu")
            self._net.load_state_dict(state_dict, strict=True)
        except Exception as exc:  # noqa: BLE001  wrap all load failures
            raise ModelLoadError(
                f"Failed to load SR weights from {path!r}: {exc}"
            ) from exc

    def preprocess(self, x):
        """Collapse g,r,i -> 1 channel (mean) and min-max scale to [0, 1] per image.

        The vendored net was trained on single-channel min-max-normalized inputs.
        """
        import torch

        if not isinstance(x, torch.Tensor):
            x = torch.as_tensor(x)
        x = x.float()
        if x.dim() == 3:
            x = x.unsqueeze(0)
        if x.shape[1] > 1:
            x = x.mean(dim=1, keepdim=True)
        B = x.shape[0]
        flat = x.view(B, -1)
        mn = flat.min(dim=1)[0].view(B, 1, 1, 1)
        mx = flat.max(dim=1)[0].view(B, 1, 1, 1)
        x = (x - mn) / (mx - mn + 1e-8)
        return x

    def predict(self, data, *, return_image=True):
        """Run super-resolution; returns the deflection field and (optionally) a reconstructed image.

        :param data: a tensor (N,C,H,W)/(C,H,W) or a dict with key 'tensor'.
        :return: {"alpha": (N,2,128,128), "scale": 2, "output_image": (N,1,128,128)}
        """
        import torch

        if isinstance(data, dict):
            tensor = data.get("tensor")
            if tensor is None:
                raise ModelError("VendoredSRAdapter.predict: dict input missing 'tensor' key")
        else:
            tensor = data

        self._build()
        device = self.config.resolve_device() if hasattr(self.config, "resolve_device") else "cpu"
        self._net.to(device)
        self._net.eval()

        x = self.preprocess(tensor).to(device)
        with torch.no_grad():
            alpha = self._net(x)

        result = {"alpha": alpha.cpu(), "scale": 2}
        if return_image:
            from .vendored.anirudh_sr import SersicProfiler

            resolution = float(getattr(self.config, "pixel_scale", 0.05)) \
                if hasattr(self.config, "pixel_scale") else 0.05
            profiler = SersicProfiler(resolution=resolution, device=device,
                                      sersic_args=[1, 1, 0.25])
            with torch.no_grad():
                alpha_lr = torch.nn.functional.interpolate(
                    alpha.to(device), scale_factor=0.5, mode="bicubic")
                _, _, image = profiler.create_lensing(
                    x, alpha_lr, alpha.to(device), resolution, resolution / 2, 2)
            result["output_image"] = image.cpu()
        return result


@register("anirudh_sr")
def build_anirudh_sr(config):
    """Builder for the vanilla SISR super-resolution net."""
    if isinstance(config, dict):
        config = ModelConfig.from_dict({**config, "model_type": "anirudh_sr",
                                        "task": "super_res"})
    return VendoredSRAdapter(config)


@register("anirudh_sr_rcan")
def build_anirudh_sr_rcan(config):
    """Builder for the RCAN super-resolution net."""
    if isinstance(config, dict):
        config = ModelConfig.from_dict({**config, "model_type": "anirudh_sr_rcan",
                                        "task": "super_res"})
    return VendoredSRAdapter(config)
