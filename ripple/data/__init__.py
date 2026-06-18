"""RIPPLe dataset acquisition and ingest helpers.

Importing this package never requires torch or gdown; ``fetch`` imports gdown
lazily inside ``fetch()`` so the package and its DATASETS table stay usable offline.
"""
from .exceptions import DataFetchError
from .fetch import DATASETS, fetch, main

__all__ = ["DataFetchError", "DATASETS", "fetch", "main"]
