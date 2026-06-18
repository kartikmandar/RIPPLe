"""Offline tests for the ripple.data acquisition package (no network, no gdown)."""


def test_data_package_imports_without_gdown():
    import ripple.data  # must succeed even though gdown is not installed
    assert ripple.data is not None


def test_datafetcherror_is_exception():
    from ripple.data.exceptions import DataFetchError
    assert issubclass(DataFetchError, Exception)
    err = DataFetchError("boom")
    assert str(err) == "boom"
