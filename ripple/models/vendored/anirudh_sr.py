"""Vendored super-resolution architectures from Anirudh Shankar's DeepLense GSoC 2024 project.

Provenance
----------
Upstream: "Physics-Informed Unsupervised Super-Resolution of Strong Lensing Images"
          Anirudh Shankar, Google Summer of Code 2024 x ML4Sci / DeepLense.
Source files (vendored verbatim except as noted below):
    DeepLense/DeepLense_Physics_Informed_Super_Resolution_Anirudh_Shankar/vanilla/models.py
        -> SISR, SersicProfiler
    DeepLense/DeepLense_Physics_Informed_Super_Resolution_Anirudh_Shankar/RCAN/rcan.py
        -> RCAN, RG, RCAB, ChannelAttention
Attribution: https://medium.com/@anirudhshankar99/physics-informed-unsupervised-super-resolution-of-lensing-images-gsoc-2024-x-ml4sci-51cedc1cfb00

Trimming notes (RIPPLe vendoring policy: torch only)
----------------------------------------------------
- The upstream ``models.py`` top-imports the Pillow image library and
  the plotting library; both are used ONLY inside a dead ``if False:`` plotting
  branch (``create_lensing``) and the notebook-only plotting branch of
  ``get_sample``. Those imports and branches are removed. The upstream
  ``numpy`` import was used only by the removed plotting branch and is dropped.
- ``SersicProfiler`` is kept reconstruction-only: ``create_source``,
  ``create_sersic``, ``create_lensing``, ``get_sample``, ``sersic_law``,
  ``approximate_center``, ``origin_shift`` (verbatim numerics). The
  training-only ``variation_density_loss``, ``multi_scale_loss`` and
  ``forward`` are dropped (RIPPLe uses these nets for inference only).
- The four ``torch.meshgrid(...)`` calls in ``SersicProfiler`` were made
  ``indexing="ij"``-explicit (behavior-preserving: ``"ij"`` is the old default;
  this only silences the PyTorch deprecation UserWarning).
- ``SISR`` is copied verbatim. ``RCAN``/``RG``/``RCAB``/``ChannelAttention``
  are copied verbatim from ``rcan.py``.
"""
import torch


class SersicProfiler(torch.nn.Module):
    def __init__(self, resolution, device, sersic_args=[1, 1, 0.25],
                 vdl_weight=1, multi_scale_loss_scales=[1, 0.5, 0.25]) -> None:
        """
        The physics-based loss module that performs everything required in training

        :param resolution: Pixel to arcsec conversion
        :param device: Device the model is being trained on
        :param sersic_args: Definition of the sersic profile
        :param vdl_weight: Weight of the VDL module
        :param multi_scale_loss_scales: List of scales the multi_scale_loss module will operate in
        """
        super(SersicProfiler, self).__init__()
        self.sersic_args = sersic_args
        self.device = device
        self.resolution = resolution
        self.vdl_weight = vdl_weight
        self.scale_list = multi_scale_loss_scales

    def create_source(self, alpha, resolution, LR):
        """
        Uses the provided deflection angle to reconstruct the source image from the lensing image

        :param alpha: Input deflection angle
        :param resolution: Arcsec per pixel of the image
        :param LR: Image with which source is to be reconstructed
        :return: Source image
        """
        B, _, x, y = LR.shape
        alpha = torch.split(alpha, [1, 1], dim=1)
        alpha_r, alpha_t = alpha[0], alpha[1]
        arcsec_bound = resolution * x / 2  #
        pos_x = torch.linspace(-arcsec_bound, arcsec_bound, x).to(self.device)
        pos_y = torch.linspace(-arcsec_bound, arcsec_bound, y).to(self.device)
        theta_y, theta_x = torch.meshgrid(pos_x, pos_y, indexing='ij')

        # reverse lensing to get the source

        theta_r = torch.sqrt(theta_x ** 2 + theta_y ** 2)
        theta_t = torch.arctan2(input=theta_y, other=theta_x)
        beta_r = theta_r - alpha_r

        beta_x, beta_y = beta_r * torch.cos(theta_t), beta_r * torch.sin(theta_t)  # gradient flow
        beta_x, beta_y = beta_x - alpha_t * torch.cos(theta_t), beta_y - alpha_t * torch.sin(theta_t)  # gradient flow
        beta_y = torch.flip(beta_y, dims=[2])  # gradient flow

        theta_r_source = torch.sqrt(theta_x ** 2 + theta_y ** 2)  # gradient flow
        theta_t_source = torch.arctan2(input=theta_y, other=theta_x)  # gradient flow
        beta_r_source = theta_r_source + alpha_r  # gradient flow
        beta_x_source, beta_y_source = beta_r_source * torch.cos(theta_t_source), beta_r_source * torch.sin(theta_t_source)  # gradient flow
        beta_x_source, beta_y_source = beta_x_source + alpha_t * torch.cos(theta_t), beta_y_source + alpha_t * torch.sin(theta_t)  # gradient flow
        beta_x_source, beta_y_source = beta_x_source / resolution, beta_y_source / resolution  # gradient flow
        beta_x_source, beta_y_source = beta_x_source / (x // 2), beta_y_source / (y // 2)  # gradient flow

        grid = torch.stack((beta_x_source, beta_y_source), dim=-1)  # gradient flow
        grid = grid.view(B, x, y, 2)  # gradient flow
        source_profile_regenerated = torch.nn.functional.grid_sample(LR, grid, mode='bilinear', padding_mode='zeros', align_corners=True)  # gradient flow
        return source_profile_regenerated

    def create_sersic(self, source_profile_regenerated, resolution, R_sersic):
        """
        Creates and fits a Sersic profile to a source image

        :param source_profile_regenerated: Input source image
        :param resolution: Arcsec per pixel of the source image
        :param R_sersic: Half light radius of the Sersic profile
        :return: Sersic profile fit to the source image, at its resolution
        """
        B, _, x, y = source_profile_regenerated.shape
        mean_indices, _ = self.approximate_center(source_profile_regenerated.view(B, -1))

        # re-lensing
        arcsec_bound = resolution * x / 2
        pos_x_source = torch.linspace(-arcsec_bound, arcsec_bound, x).to(self.device)
        pos_y_source = torch.linspace(-arcsec_bound, arcsec_bound, y).to(self.device)
        theta_y_source, theta_x_source = torch.meshgrid(pos_x_source, pos_y_source, indexing='ij')

        y_center_LR, x_center_LR = mean_indices // y, mean_indices % x
        y_center_LR = x - y_center_LR
        x_center_LR, y_center_LR = self.origin_shift(x_center_LR, x / 2) * resolution, self.origin_shift(y_center_LR, y / 2) * resolution

        S_lens = self.sersic_law(theta_x_source.reshape(1, -1).repeat(B, 1), theta_y_source.reshape(1, -1).repeat(B, 1), x_center_LR.view(B, -1), y_center_LR.view(B, -1), R_sersic)  # no gradient
        S_lens = (S_lens - torch.min(S_lens, dim=-1)[0].view(B, 1)) / (torch.max(S_lens, dim=-1)[0].view(B, 1) - torch.min(S_lens, dim=-1)[0].view(B, 1))
        return S_lens

    def create_lensing(self, LR, alpha, alpha_interpolated, resolution, resolution_, magnification, R_sersic=None):
        """
        Performs lensing twice, to create the source image using the deflection angle, and then to recreate the lensing image at the required resolution

        :param LR: Low resolution lensing image
        :param alpha: Deflection angle extracted from the LR image, at the higher resolution
        :param alpha_interpolated: Deflection angle interpolated to the resolution of the LR image
        :param resolution: Arcsec per pixel of the LR image
        :param resolution_: Target arcsec per pixel
        :param magnification: Target magnification
        :param R_sersic: Half light radius of the Sersic profile
        :return: Source image, Sersic profile, Re-lensed image
        """
        B, _, x, y = LR.shape
        alpha = torch.split(alpha, [1, 1], dim=1)
        alpha_r, alpha_t = alpha[0], alpha[1]
        arcsec_bound = resolution * x / 2  #
        pos_x = torch.linspace(-arcsec_bound, arcsec_bound, x).to(self.device)
        pos_y = torch.linspace(-arcsec_bound, arcsec_bound, y).to(self.device)
        theta_y, theta_x = torch.meshgrid(pos_x, pos_y, indexing='ij')

        # reverse lensing to get the source

        theta_r = torch.sqrt(theta_x ** 2 + theta_y ** 2)
        theta_t = torch.arctan2(input=theta_y, other=theta_x)
        beta_r = theta_r - alpha_r

        beta_x, beta_y = beta_r * torch.cos(theta_t), beta_r * torch.sin(theta_t)  # gradient flow
        beta_x, beta_y = beta_x - alpha_t * torch.cos(theta_t), beta_y - alpha_t * torch.sin(theta_t)  # gradient flow
        beta_y = torch.flip(beta_y, dims=[2])  # gradient flow

        theta_r_source = torch.sqrt(theta_x ** 2 + theta_y ** 2)  # gradient flow
        theta_t_source = torch.arctan2(input=theta_y, other=theta_x)  # gradient flow
        beta_r_source = theta_r_source + alpha_r  # gradient flow
        beta_x_source, beta_y_source = beta_r_source * torch.cos(theta_t_source), beta_r_source * torch.sin(theta_t_source)  # gradient flow
        beta_x_source, beta_y_source = beta_x_source + alpha_t * torch.cos(theta_t), beta_y_source + alpha_t * torch.sin(theta_t)  # gradient flow
        beta_x_source, beta_y_source = beta_x_source / resolution, beta_y_source / resolution  # gradient flow
        beta_x_source, beta_y_source = beta_x_source / (x // 2), beta_y_source / (y // 2)  # gradient flow

        grid = torch.stack((beta_x_source, beta_y_source), dim=-1)  # gradient flow
        grid = grid.view(B, x, y, 2)  # gradient flow
        source_profile_regenerated = torch.nn.functional.grid_sample(LR, grid, mode='bilinear', padding_mode='zeros', align_corners=True)  # gradient flow

        # fitting the source onto a Sersic

        LR = LR.view(B, -1)
        mean_indices, _ = self.approximate_center(source_profile_regenerated.view(B, -1))
        beta_x_LR, beta_y_LR = beta_x, beta_y

        # re-lensing

        in_shape = int(x * magnification)
        alpha_interpolated = torch.split(alpha_interpolated, [1, 1], dim=1)
        alpha_r_interpolated, alpha_t_interpolated = alpha_interpolated[0], alpha_interpolated[1]
        arcsec_bound = resolution_ * in_shape / 2
        pos_x_source = torch.linspace(-arcsec_bound, arcsec_bound, in_shape).to(self.device)
        pos_y_source = torch.linspace(-arcsec_bound, arcsec_bound, in_shape).to(self.device)
        theta_y_source, theta_x_source = torch.meshgrid(pos_x_source, pos_y_source, indexing='ij')
        theta_r_source = torch.sqrt(theta_x_source ** 2 + theta_y_source ** 2)
        theta_t_source = torch.arctan2(input=theta_y_source, other=theta_x_source)
        theta_y_source = torch.flip(theta_y_source, dims=[0])  # gradient flow

        beta_r_source = theta_r_source - alpha_r_interpolated  # gradient flow
        beta_x_source, beta_y_source = beta_r_source * torch.cos(theta_t_source), beta_r_source * torch.sin(theta_t_source)  # gradient flow
        beta_x_source, beta_y_source = beta_x_source - alpha_t_interpolated * torch.cos(theta_t_source), beta_y_source - alpha_t_interpolated * torch.sin(theta_t_source)  # gradient flow
        beta_y_source = torch.flip(beta_y_source, dims=[2])  # gradient flow

        y_center_LR, x_center_LR = mean_indices // x, mean_indices % x
        y_center_LR = x - y_center_LR
        x_center_LR, y_center_LR = self.origin_shift(x_center_LR, x / 2) * resolution, self.origin_shift(y_center_LR, y / 2) * resolution

        I_lens_LR = self.sersic_law(beta_x_LR.view(B, -1), beta_y_LR.view(B, -1), x_center_LR.view(B, 1), y_center_LR.view(B, 1), R_sersic)  # gradient flow
        I_lens = self.sersic_law(beta_x_source.view(B, -1), beta_y_source.view(B, -1), x_center_LR.view(B, 1), y_center_LR.view(B, 1), R_sersic)  # gradient flow
        S_lens = self.sersic_law(theta_x_source.reshape(1, -1).repeat(B, 1), theta_y_source.reshape(1, -1).repeat(B, 1), x_center_LR.view(B, -1), y_center_LR.view(B, -1), R_sersic)  # no gradient

        I_lens_LR = (I_lens_LR - torch.min(I_lens_LR, dim=-1)[0].view(B, 1)) / (torch.max(I_lens_LR, dim=-1)[0].view(B, 1) - torch.min(I_lens_LR, dim=-1)[0].view(B, 1))
        S_lens = (S_lens - torch.min(S_lens, dim=-1)[0].view(B, 1)) / (torch.max(S_lens, dim=-1)[0].view(B, 1) - torch.min(S_lens, dim=-1)[0].view(B, 1))
        I_lens = (I_lens - torch.min(I_lens, dim=-1)[0].view(B, 1)) / (torch.max(I_lens, dim=-1)[0].view(B, 1) - torch.min(I_lens, dim=-1)[0].view(B, 1))
        source_profile_regenerated = source_profile_regenerated.view(B, -1)
        source_profile_regenerated = (source_profile_regenerated - torch.min(source_profile_regenerated, dim=-1)[0].view(B, 1)) / (torch.max(source_profile_regenerated, dim=-1)[0].view(B, 1) - torch.min(source_profile_regenerated, dim=-1)[0].view(B, 1))

        I_lens_LR = I_lens_LR.view(B, 1, x, y)
        I_lens = I_lens.view(B, 1, in_shape, in_shape)
        S_lens = S_lens.view(B, 1, in_shape, in_shape)
        source_profile_regenerated = source_profile_regenerated.view(B, 1, x, y)
        return source_profile_regenerated, S_lens, I_lens  # gradients on 1st, 3rd and 4th

    def sersic_law(self, x, y, x_center, y_center, R_sersic):
        """
        Constructs a (displaced) Sersic profile for a set of positions

        :param x: x coordinate list (in arcsec)
        :param y: y coordinate list (in arcsec)
        :param x_center, y_center: Defines the displacement of the Sersic center
        :return: Intensity values for the given coordinates
        """
        if R_sersic is None:
            amp, n_sersic, R_sersic = self.sersic_args
        else:
            amp, n_sersic = self.sersic_args[:2]
        b_n = 1.999 * n_sersic - 0.327
        R = torch.sqrt(torch.pow(x - x_center, 2) + torch.pow(y - y_center, 2))
        I = amp * torch.pow(torch.e, -b_n * (torch.pow(R / R_sersic, 1 / n_sersic) - 1))
        return I

    def get_sample(self, alpha, LR, plot, R_sersic=None):
        """
        Used in generating examples.

        The upstream plotting branch (``if plot:``) is removed; the
        ``plot`` argument is kept for call-signature compatibility.
        """
        alpha_LR = torch.nn.functional.interpolate(alpha, scale_factor=0.5, mode='bicubic')
        source_profile_regenerated, S_lens, I_lens = self.create_lensing(LR, alpha_LR, alpha, self.resolution, self.resolution / 2, 2, R_sersic=R_sersic)
        return I_lens[0], LR[0], source_profile_regenerated[0]

    def origin_shift(self, source, shift):
        """
        A coordinate shift to recenter the origin
        """
        return source - shift

    def approximate_center(self, intensity_profile):
        """
        Used to locate the center of an intensity profile
        """
        _, max_index = torch.max(intensity_profile, dim=1)
        return max_index, None


class SISR(torch.nn.Module):
    def __init__(self, magnification, n_mag, residual_depth, in_channels=1, latent_channel_count=64):
        """
        Single image super-resolution module, to upscale an image to a decided magnification

        :param magnification: Magnification value
        :param n_mag: Number of times the above magnification is applied
        :param residual_depth: Number of residual modules used
        :param in_channels: Number of channels in the image (here 1)
        :param latent_channel_count: Dimensions of the residual module layers
        """
        super(SISR, self).__init__()
        self.magnification = magnification
        self.residual_depth = residual_depth
        self.in_channels = in_channels
        self.latent_channel_count = latent_channel_count
        self.residual_layer_list = torch.nn.ModuleList()
        self.subpixel_layer_list = torch.nn.ModuleList()
        self.conv1 = torch.nn.Conv2d(in_channels=self.in_channels, out_channels=self.latent_channel_count, kernel_size=3, padding=1)
        self.bn1 = torch.nn.BatchNorm2d(num_features=self.latent_channel_count)

        self.relu1 = torch.nn.ReLU()
        for _ in range(residual_depth):
            self.residual_layer_list.append(self.make_residual_layer(latent_channel_count))
        self.conv2 = torch.nn.Conv2d(in_channels=latent_channel_count, out_channels=latent_channel_count, kernel_size=9, padding=4)
        for _ in range(n_mag):
            self.subpixel_layer_list.append(self.make_subpixel_layer(latent_channel_count))

        self.conv3 = torch.nn.Conv2d(in_channels=self.latent_channel_count, out_channels=self.in_channels * 2, kernel_size=3, padding=1)
        self.bn2 = torch.nn.BatchNorm2d(num_features=self.in_channels * 2)

        self.sigmoid = torch.nn.Sigmoid()

    def forward(self, x):
        """
        Feed-forward
        """
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu1(x)
        x_res_0 = x.clone()
        for module in self.residual_layer_list:
            x_res = x.clone()
            x = module(x)
            x = x + x_res
        x = self.conv2(x)
        x = x + x_res_0
        for module in self.subpixel_layer_list:
            x = module(x)
        x = self.conv3(x)
        x = self.bn2(x)
        x = self.sigmoid(x)
        return x

    def make_residual_layer(self, channels):
        """
        Generates and returns a single residual layer
        """
        return torch.nn.Sequential(
            torch.nn.Conv2d(in_channels=channels, out_channels=channels, kernel_size=3, padding=1),
            torch.nn.BatchNorm2d(num_features=channels),
            torch.nn.ReLU(),
            torch.nn.Conv2d(in_channels=channels, out_channels=channels, kernel_size=5, padding=2),
            torch.nn.BatchNorm2d(num_features=channels),
            torch.nn.ReLU()
        )

    def make_subpixel_layer(self, channels):
        """
        Generates and returns a single subpixel layer
        """
        return torch.nn.Sequential(
            torch.nn.Conv2d(in_channels=channels, out_channels=channels * self.magnification * self.magnification, kernel_size=3, padding=1),
            torch.nn.PixelShuffle(self.magnification),
            torch.nn.ReLU()
        )


class ChannelAttention(torch.nn.Module):
    def __init__(self, latent_dim, reduction) -> None:
        """
        A module that implements channel attention

        :param latent_dim: Latent dimension size
        :param reduction: Latent size reduction scale
        """
        super(ChannelAttention, self).__init__()
        self.attention = torch.nn.Sequential(
            torch.nn.AdaptiveAvgPool2d(1),
            torch.nn.Conv2d(latent_dim, latent_dim // reduction, kernel_size=1),
            torch.nn.ReLU(inplace=True),
            torch.nn.Conv2d(latent_dim // reduction, latent_dim, kernel_size=1),
            torch.nn.Sigmoid()
        )

    def forward(self, x):
        return x * self.attention(x)


class RCAB(torch.nn.Module):
    def __init__(self, latent_dim, reduction):
        """
        Implements the Residual Channel Attention Block module

        :param latent_dim: Latent dimension size
        :param reduction: Latent size reduction scale
        """
        super(RCAB, self).__init__()
        self.rcab = torch.nn.Sequential(
            torch.nn.Conv2d(latent_dim, latent_dim, kernel_size=3, padding=1),
            torch.nn.ReLU(inplace=True),
            torch.nn.Conv2d(latent_dim, latent_dim, kernel_size=3, padding=1),
            ChannelAttention(latent_dim, reduction)
        )

    def forward(self, x):
        return x + self.rcab(x)


class RG(torch.nn.Module):
    def __init__(self, latent_dim, num_rcab, reduction):
        """
        Implements the Residual Group module

        :param latent_dim: Latent dimension size
        :param num_rcab: Number of RCAB blocks in the Residual Group
        :param reduction: Latent size reduction scale
        """
        super(RG, self).__init__()
        self.rg = [RCAB(latent_dim, reduction) for _ in range(num_rcab)]
        self.rg.append(torch.nn.Conv2d(latent_dim, latent_dim, kernel_size=3, padding=1))
        self.rg = torch.nn.Sequential(*self.rg)

    def forward(self, x):
        return x + self.rg(x)


class RCAN(torch.nn.Module):
    def __init__(self, scale, latent_dim, num_rg, num_rcab, reduction, in_channels=1, out_channels=1):
        """
        Implements the Residual Channel Attention Network

        :param scale: Super-resolution scale
        :param latent_dim: Latent dimension size
        :param num_rg: Number of residual groups
        :param num_rcab: Number of RCAB modules
        :param reduction: Latent size reduction scale
        :param in_channels: Number of input image channels
        :param out_channels: Number of output image channels
        """
        super(RCAN, self).__init__()
        self.conv1 = torch.nn.Conv2d(in_channels, latent_dim, kernel_size=3, padding=1)
        self.rgs = torch.nn.Sequential(*[RG(latent_dim, num_rcab, reduction) for _ in range(num_rg)])
        self.conv2 = torch.nn.Conv2d(latent_dim, latent_dim, kernel_size=3, padding=1)
        self.upscale = torch.nn.Sequential(
            torch.nn.Conv2d(latent_dim, latent_dim * (scale ** 2), kernel_size=3, padding=1),
            torch.nn.PixelShuffle(scale)
        )
        self.conv3 = torch.nn.Conv2d(latent_dim, out_channels, kernel_size=3, padding=1)

    def forward(self, x):
        """
        Feed-forward
        """
        x = self.conv1(x)
        residual = x
        x = self.rgs(x)
        x = self.conv2(x)
        x += residual
        x = self.upscale(x)
        x = self.conv3(x)
        return x
