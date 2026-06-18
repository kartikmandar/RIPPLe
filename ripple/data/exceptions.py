"""Exceptions for the ripple.data acquisition package."""


class DataFetchError(Exception):
    """Raised when a dataset cannot be fetched (missing gdown, bad id, or absent files)."""
    pass
