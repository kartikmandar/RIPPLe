"""
Coordinate Resolution Utilities for RIPPLe

This module provides high-performance coordinate resolution utilities
that convert RA/Dec coordinates to LSST tract/patch coordinates
with skymap caching and optimization.
"""

import logging
from typing import Optional, Tuple, Dict, Any
from functools import lru_cache

from lsst.geom import SpherePoint, degrees
from lsst.daf.butler import Butler


class CoordinateResolver:
    """
    High-performance coordinate resolver with caching and optimization.

    This class provides efficient RA/Dec to tract/patch resolution
    for LSST data access, with skymap caching and error handling.
    """

    def __init__(self, butler: Butler, cache_size: int = 100):
        """
        Initialize coordinate resolver.

        Args:
            butler (Butler): Initialized Butler instance
            cache_size (int): Maximum number of skymap objects to cache
        """
        self.butler = butler
        self.cache_size = cache_size
        self._skymap_cache = {}
        self.logger = logging.getLogger(__name__)

    @lru_cache(maxsize=50)
    def _get_skymap(self, skymap_name: str = None):
        """
        Get skymap with LRU caching.

        Args:
            skymap_name (str, optional): Name of skymap to retrieve

        Returns:
            Skymap object or None if not found
        """
        try:
            if skymap_name:
                return self.butler.get("skyMap", {"skymap": skymap_name})
            else:
                return self.butler.get("skyMap")
        except Exception as e:
            self.logger.warning(f"Failed to get skymap {skymap_name}: {e}")
            return None

    def ra_dec_to_tract_patch(self, ra: float, dec: float,
                             skymap_name: str = None) -> Optional[Tuple[int, int]]:
        """
        Convert RA/Dec coordinates to tract/patch with caching.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            skymap_name (str, optional): Skymap identifier

        Returns:
            Optional[Tuple[int, int]]: (tract_id, patch_index) or None if conversion fails
        """
        try:
            skymap = self._get_skymap(skymap_name)
            if not skymap:
                self.logger.error("Skymap not available for coordinate resolution")
                return None

            coord = SpherePoint(ra * degrees, dec * degrees)
            tract_info = skymap.findTract(coord)

            if not tract_info:
                self.logger.warning(f"Coordinates ({ra}, {dec}) outside skymap coverage")
                return None

            patch_info = tract_info.findPatch(coord)

            tract_id = tract_info.tract_id
            patch_index = patch_info.getIndex()

            self.logger.debug(f"Coordinates ({ra}, {dec}) -> tract {tract_id}, patch {patch_index}")
            return tract_id, patch_index

        except Exception as e:
            self.logger.error(f"Coordinate resolution failed: {e}")
            return None

    def coord_to_dataid(self, ra: float, dec: float, band: str,
                    skymap_name: str = None, dataset_type: str = "deepCoadd") -> Dict[str, Any]:
        """
        Convert RA/Dec coordinates to complete DataId for dataset access.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            band (str): Filter band
            skymap_name (str, optional): Skymap identifier
            dataset_type (str): Dataset type for DataId construction

        Returns:
            Dict[str, Any]: Complete DataId dictionary or None if conversion fails
        """
        result = self.ra_dec_to_tract_patch(ra, dec, skymap_name)
        if not result:
            return None

        tract_id, patch_index = result

        data_id = {
            "tract": tract_id,
            "patch": patch_index,
            "band": band
        }

        if skymap_name:
            data_id["skymap"] = skymap_name

        return data_id

    def get_tract_center(self, tract: int, skymap_name: str = None) -> Optional[Tuple[float, float]]:
        """
        Get the center coordinates of a tract.

        Args:
            tract (int): Tract identifier
            skymap_name (str, optional): Skymap identifier

        Returns:
            Optional[Tuple[float, float]]: (ra_deg, dec_deg) of tract center
        """
        try:
            skymap = self._get_skymap(skymap_name)
            if not skymap:
                return None

            tract_info = skymap[tract]
            if not tract_info:
                self.logger.warning(f"Tract {tract} not found in skymap")
                return None

            center = tract_info.getCtr()
            ra_deg = center.getRa().asDegrees()
            dec_deg = center.getDec().asDegrees()

            return ra_deg, dec_deg

        except Exception as e:
            self.logger.error(f"Failed to get tract {tract} center: {e}")
            return None

    def get_tract_bbox(self, tract: int, skymap_name: str = None) -> Optional[Dict[str, Any]]:
        """
        Get the bounding box of a tract.

        Args:
            tract (int): Tract identifier
            skymap_name (str, optional): Skymap identifier

        Returns:
            Optional[Dict[str, Any]]: Bounding box information or None if failed
        """
        try:
            skymap = self._get_skymap(skymap_name)
            if not skymap:
                return None

            tract_info = skymap[tract]
            if not tract_info:
                self.logger.warning(f"Tract {tract} not found in skymap")
                return None

            bbox = tract_info.getBBox()

            return {
                "min_x": bbox.getMinX(),
                "min_y": bbox.getMinY(),
                "max_x": bbox.getMaxX(),
                "max_y": bbox.getMaxY(),
                "width": bbox.getWidth(),
                "height": bbox.getHeight()
            }

        except Exception as e:
            self.logger.error(f"Failed to get tract {tract} bbox: {e}")
            return None

    def validate_sky_coverage(self, ra: float, dec: float,
                            radius_deg: float = 0.1, skymap_name: str = None) -> Dict[str, Any]:
        """
        Validate if coordinates are within skymap coverage.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            radius_deg (float): Search radius in degrees
            skymap_name (str, optional): Skymap identifier

        Returns:
            Dict[str, Any]: Validation result with coverage information
        """
        try:
            result = self.ra_dec_to_tract_patch(ra, dec, skymap_name)
            if not result:
                return {
                    "valid": False,
                    "error": "Coordinate conversion failed"
                }

            tract_id, patch_index = result
            skymap = self._get_skymap(skymap_name)

            # Check if coordinates are within reasonable distance of tract center
            tract_info = skymap[tract_id]
            tract_center = tract_info.getCtr()
            coord = SpherePoint(ra * degrees, dec * degrees)

            # Simple distance check (more sophisticated checks could be added)
            distance = coord.separation(tract_center).asDegrees()

            # Assume tract is roughly 1.7 degrees, so anything within 2 degrees is reasonable
            within_coverage = distance <= max(radius_deg, 2.0)

            return {
                "valid": True,
                "tract_id": tract_id,
                "patch_index": patch_index,
                "distance_deg": distance,
                "within_coverage": within_coverage,
                "tract_center_ra": tract_center.getRa().asDegrees(),
                "tract_center_dec": tract_center.getDec().asDegrees()
            }

        except Exception as e:
            self.logger.error(f"Sky coverage validation failed: {e}")
            return {
                "valid": False,
                "error": str(e)
            }

    def get_nearby_tracts(self, ra: float, dec: float,
                         radius_deg: float = 1.0, skymap_name: str = None) -> Dict[int, Dict[str, Any]]:
        """
        Get all tracts within radius of coordinates.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            radius_deg (float): Search radius in degrees
            skymap_name (str, optional): Skymap identifier

        Returns:
            Dict[int, Dict[str, Any]]: Dictionary mapping tract IDs to information
        """
        try:
            skymap = self._get_skymap(skymap_name)
            if not skymap:
                return {}

            coord = SpherePoint(ra * degrees, dec * degrees)

            # Find all tracts that might contain the coordinates
            nearby_tracts = {}
            coord = SpherePoint(ra * degrees, dec * degrees)

            # Simple approach: check tracts in expanding radius
            max_search_radius = radius_deg * 2  # Search slightly larger area

            for tract_id in range(len(skymap)):
                try:
                    tract_info = skymap[tract_id]
                    tract_center = tract_info.getCtr()
                    distance = coord.separation(tract_center).asDegrees()

                    if distance <= max_search_radius:
                        tract_bbox = tract_info.getBBox()

                        # More precise check: actually see if coordinate is in tract bounds
                        # This would require WCS conversion for precision
                        nearby_tracts[tract_id] = {
                            "tract_id": tract_id,
                            "distance_deg": distance,
                            "tract_center_ra": tract_center.getRa().asDegrees(),
                            "tract_center_dec": tract_center.getDec().asDegrees(),
                            "tract_bbox": {
                                "min_x": tract_bbox.getMinX(),
                                "min_y": tract_bbox.getMinY(),
                                "max_x": tract_bbox.getMaxX(),
                                "max_y": tract_bbox.getMaxY()
                            }
                        }
                except Exception:
                    continue

            self.logger.info(f"Found {len(nearby_tracts)} potential tracts within {radius_deg} degrees")
            return nearby_tracts

        except Exception as e:
            self.logger.error(f"Failed to find nearby tracts: {e}")
            return {}


class SpatialQueryHelper:
    """
    Helper class for spatial queries and optimizations.
    """

    def __init__(self, butler: Butler):
        """
        Initialize spatial query helper.

        Args:
            butler (Butler): Initialized Butler instance
        """
        self.butler = butler
        self.logger = logging.getLogger(__name__)
        self.coord_resolver = CoordinateResolver(butler)

    def build_tract_query(self, tracts: list, bands: list = None,
                      dataset_type: str = "deepCoadd") -> str:
        """
        Build efficient WHERE clause for tract-based queries.

        Args:
            tracts (list): List of tract numbers
            bands (list, optional): List of filter bands
            dataset_type (str): Dataset type for query

        Returns:
            str: Optimized WHERE clause
        """
        tract_list = ", ".join(map(str, tracts))
        where_clause = f"tract IN ({tract_list})"

        if bands:
            band_list = "', '".join(bands)
            where_clause += f" AND band IN ('{band_list}')"

        self.logger.debug(f"Built query for tracts {tracts}, bands {bands}: {where_clause}")
        return where_clause

    def optimize_dataid_order(self, data_ids: list) -> list:
        """
        Optimize DataId order for efficient batch processing.

        Args:
            data_ids (list): List of DataId dictionaries

        Returns:
            list: Reordered DataIds for efficient access
        """
        # Group by tract to minimize Butler context switching
        tract_groups = {}
        for data_id in data_ids:
            tract = data_id.get("tract")
            if tract not in tract_groups:
                tract_groups[tract] = []
            tract_groups[tract].append(data_id)

        # Flatten groups back to list (tract-contiguous order)
        optimized_order = []
        for tract in sorted(tract_groups.keys()):
            optimized_order.extend(tract_groups[tract])

        self.logger.info(f"Optimized DataId order for {len(data_ids)} items across {len(tract_groups)} tracts")
        return optimized_order

    def find_missing_bands(self, tract: int, patch: int,
                       required_bands: list = ["g", "r", "i"],
                       dataset_type: str = "deepCoadd") -> Dict[str, bool]:
        """
        Find which bands are missing for a tract/patch location.

        Args:
            tract (int): Tract identifier
            patch (int): Patch identifier
            required_bands (list): Required filter bands
            dataset_type (str): Dataset type to check

        Returns:
            Dict[str, bool]: Dictionary mapping band to availability
        """
        availability = {}

        for band in required_bands:
            try:
                data_id = {"tract": tract, "patch": patch, "band": band}
                ref = self.butler.find_dataset(dataset_type, data_id,
                                               collections=self.butler.collections)
                availability[band] = ref is not None
            except Exception as e:
                self.logger.warning(f"Failed to check {band} band availability: {e}")
                availability[band] = False

        return availability


# Utility functions for backward compatibility and ease of use
def create_coordinate_resolver(butler: Butler, cache_size: int = 100) -> CoordinateResolver:
    """
    Factory function to create a coordinate resolver.

    Args:
        butler (Butler): Butler instance
        cache_size (int): Cache size for skymap objects

    Returns:
        CoordinateResolver: Initialized coordinate resolver
    """
    return CoordinateResolver(butler, cache_size)


def create_spatial_helper(butler: Butler) -> SpatialQueryHelper:
    """
    Factory function to create a spatial query helper.

    Args:
        butler (Butler): Butler instance

    Returns:
        SpatialQueryHelper: Initialized spatial query helper
    """
    return SpatialQueryHelper(butler)