# ripple/preprocessing/data_transformer.py
"""Geometry + channel + tensor conversion (the tensor core).

Pad/crop (default) or bilinear-zoom to a fixed NxN; replicate/select channels;
stack a band list into (C,H,W); coerce byte order; convert to torch tensors.
Torch and scipy are imported lazily.
"""
import numpy as np


class DataTransformer:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def ensure_native_float32(array) -> np.ndarray:
        """Coerce to native-endian, C-contiguous float32 (fixes FITS '>f4' for torch)."""
        a = np.asarray(array)
        if a.dtype.byteorder not in ("=", "|", "<") or a.dtype.kind != "f":
            a = a.astype(a.dtype.newbyteorder("="), copy=False)
        a = np.ascontiguousarray(a, dtype=np.float32)
        return a

    def to_fixed_size(self, array, size) -> np.ndarray:
        a = self.ensure_native_float32(array)
        mode = getattr(self.config, "resize_mode", "pad_crop")
        if mode == "interpolate":
            from scipy.ndimage import zoom
            zy, zx = size / a.shape[0], size / a.shape[1]
            out = zoom(a, (zy, zx), order=1)
            return self._exact(out, size)
        return self._pad_crop(a, size)

    def _pad_crop(self, a, size):
        h, w = a.shape
        # crop (centered) if too big
        if h > size:
            top = (h - size) // 2
            a = a[top:top + size, :]
        if w > size:
            left = (w - size) // 2
            a = a[:, left:left + size]
        h, w = a.shape
        # pad (centered) with background 0
        pad_top = (size - h) // 2
        pad_bot = size - h - pad_top
        pad_left = (size - w) // 2
        pad_right = size - w - pad_left
        if pad_top or pad_bot or pad_left or pad_right:
            a = np.pad(a, ((pad_top, pad_bot), (pad_left, pad_right)),
                       mode="constant", constant_values=0.0)
        return self._exact(a, size)

    @staticmethod
    def _exact(a, size):
        return a[:size, :size] if a.shape != (size, size) else a

    def adapt_channels(self, arrays, channels) -> list:
        arrays = [self.ensure_native_float32(a) for a in arrays]
        if len(arrays) == channels:
            return arrays
        if len(arrays) == 1 and channels > 1:
            return [arrays[0].copy() for _ in range(channels)]
        if len(arrays) > channels:
            return arrays[:channels]
        # fewer than requested (and not the 1->N case): replicate last to fill
        return arrays + [arrays[-1].copy() for _ in range(channels - len(arrays))]

    def stack_to_chw(self, arrays) -> np.ndarray:
        arrays = [self.ensure_native_float32(a) for a in arrays]
        chw = np.stack(arrays, axis=0)
        return np.ascontiguousarray(chw, dtype=np.float32)

    def to_tensor(self, chw):
        import torch
        chw = self.ensure_native_float32(chw)
        return torch.from_numpy(chw).float()

    def batch(self, chw_list):
        import torch
        tensors = [self.to_tensor(c) for c in chw_list]
        return torch.stack(tensors, dim=0)
