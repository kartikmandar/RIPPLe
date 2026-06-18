# ripple/tests/preprocessing/test_data_transformer.py
import numpy as np
import pytest
from ripple.preprocessing.config import PreprocessingConfig
from ripple.preprocessing.data_transformer import DataTransformer


@pytest.fixture
def tf():
    return DataTransformer(PreprocessingConfig(cutout_size=64, resize_mode="pad_crop"))


def test_ensure_native_float32_byteswaps_big_endian(tf):
    a = np.arange(9, dtype=">f4").reshape(3, 3)
    out = tf.ensure_native_float32(a)
    assert out.dtype == np.float32
    assert out.dtype.byteorder in ("=", "<", "|")
    np.testing.assert_allclose(out, a.astype(np.float32))


def test_pad_small_to_fixed_size(tf):
    a = np.ones((58, 61), dtype=np.float32)
    out = tf.to_fixed_size(a, 64)
    assert out.shape == (64, 64)
    assert out[0, 0] == 0.0  # padded with background 0
    assert out[32, 32] == 1.0


def test_crop_large_to_fixed_size(tf):
    a = np.ones((70, 70), dtype=np.float32)
    out = tf.to_fixed_size(a, 64)
    assert out.shape == (64, 64)


def test_interpolate_mode_resizes():
    tf = DataTransformer(PreprocessingConfig(cutout_size=64, resize_mode="interpolate"))
    a = np.ones((32, 32), dtype=np.float32)
    out = tf.to_fixed_size(a, 64)
    assert out.shape == (64, 64)


def test_adapt_channels_replicate_single_to_three(tf):
    arrays = [np.ones((4, 4), dtype=np.float32)]
    out = tf.adapt_channels(arrays, channels=3)
    assert len(out) == 3
    np.testing.assert_array_equal(out[0], out[2])


def test_adapt_channels_selects_subset(tf):
    arrays = [np.full((4, 4), i, dtype=np.float32) for i in range(4)]
    out = tf.adapt_channels(arrays, channels=3)
    assert len(out) == 3


def test_stack_to_chw_order_and_dtype(tf):
    arrays = [np.full((4, 4), i, dtype=np.float32) for i in range(3)]
    chw = tf.stack_to_chw(arrays)
    assert chw.shape == (3, 4, 4)
    assert chw[0, 0, 0] == 0 and chw[2, 0, 0] == 2
    assert chw.flags["C_CONTIGUOUS"] and chw.dtype == np.float32


@pytest.mark.torch
def test_to_tensor_and_batch():
    pytest.importorskip("torch")
    import torch
    tf = DataTransformer(PreprocessingConfig())
    chw = np.zeros((3, 8, 8), dtype=np.float32)
    t = tf.to_tensor(chw)
    assert isinstance(t, torch.Tensor) and t.shape == (3, 8, 8) and t.dtype == torch.float32
    b = tf.batch([chw, chw])
    assert b.shape == (2, 3, 8, 8)


@pytest.mark.torch
def test_to_tensor_handles_big_endian():
    pytest.importorskip("torch")
    tf = DataTransformer(PreprocessingConfig())
    chw = np.zeros((1, 4, 4), dtype=">f4")
    t = tf.to_tensor(chw)  # must not raise
    assert t.shape == (1, 4, 4)
