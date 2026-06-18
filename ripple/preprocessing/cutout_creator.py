"""Thin wrapper over LsstDataFetcher for coordinate-based cutout retrieval.

Retrieval (Butler vs RSP backend selection, getCutout, etc.) is owned by data_access;
this class only forwards config-driven parameters and normalizes the call site so the
Preprocessor and PreprocessingStage share one extraction path.
"""
from typing import Any, Dict, List, Optional, Tuple

from ripple.preprocessing.exceptions import CutoutError


class CutoutCreator:
    def __init__(self, data_fetcher, config):
        self.data_fetcher = data_fetcher
        self.config = config

    def create(self, ra: float, dec: float) -> Dict[str, Optional[Any]]:
        if self.data_fetcher is None:
            raise CutoutError("CutoutCreator requires a data_fetcher")
        return self.data_fetcher.get_multi_band_cutout(
            ra=ra, dec=dec,
            size_arcsec=getattr(self.config, "size_arcsec", 12.8),
            bands=list(getattr(self.config, "bands", ("g", "r", "i"))),
        )

    def create_many(self, coords: List[Tuple[float, float]]) -> List[Dict[str, Optional[Any]]]:
        return [self.create(ra, dec) for ra, dec in coords]