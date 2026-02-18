"""
LSST Data Fetcher - High-level interface for LSST data access

This module provides the LsstDataFetcher class that serves as a high-level
interface for accessing LSST data through various backends (Butler, RSP TAP, etc.).
"""

import logging
import io
from typing import Dict, Any, Optional, List, Union, Tuple
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units as u

from .butler_client import ButlerClient, ButlerConfig
from .rsp_tap_client import RSPTAPClient, create_rsp_client
from .exceptions import DataAccessError, ButlerConnectionError
from .coordinate_resolver import create_coordinate_resolver, create_spatial_helper


class LsstDataFetcher:
    """
    High-level interface for accessing LSST data from multiple sources.

    This class provides a unified API for accessing LSST data through different
    backends including local Butler repositories and remote RSP services.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize LSST Data Fetcher.

        Args:
            config: Configuration dictionary containing data source settings
        """
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

        # Initialize data clients
        self.butler_client: Optional[ButlerClient] = None
        self.rsp_tap_client: Optional[RSPTAPClient] = None

        # Initialize coordinate resolver when Butler client is available
        self.coord_resolver = None
        self.spatial_helper = None

        # Determine data source type and initialize appropriate client
        self._initialize_data_client()

    def _initialize_data_client(self) -> None:
        """Initialize the appropriate data client based on configuration."""
        # Check if config is already a data_source configuration
        # (this happens when LsstDataFetcher is created from DataSourceStage)
        if 'type' in self.config:
            # Config is already the data_source part
            data_source = self.config
        else:
            # Config is the full configuration - extract data_source
            data_source = self.config.get('data_source', {})

        source_type = data_source.get('type', 'data_folder')

        self.logger.info(f"Initializing data fetcher for source type: {source_type}")

        if source_type == 'butler_repo':
            self._initialize_butler_client(data_source)
        elif source_type == 'butler_server':
            self._initialize_rsp_client(data_source)
        elif source_type == 'data_folder':
            self.logger.info("Data folder source - no remote client needed")
        else:
            self.logger.warning(f"Unknown data source type: {source_type}")

    def _initialize_butler_client(self, data_source: Dict[str, Any]) -> None:
        """Initialize Butler client for local repository access."""
        try:
            repo_path = data_source.get('params', {}).get('path')
            if not repo_path:
                raise ValueError("Repository path required for butler_repo type")

            # Create Butler configuration
            butler_config = ButlerConfig(repo_path=repo_path)

            # Initialize Butler client
            self.butler_client = ButlerClient(config=butler_config)
            self.coord_resolver = create_coordinate_resolver(self.butler_client.butler)
            self.spatial_helper = create_spatial_helper(self.butler_client.butler)
            self.logger.info(f"✓ Butler client initialized for repository: {repo_path}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Butler client: {e}")
            self.butler_client = None

    def _initialize_rsp_client(self, data_source: Dict[str, Any]) -> None:
        """Initialize RSP TAP client for remote access."""
        try:
            # Extract RSP configuration
            server_url = data_source.get('server_url')
            sia_url = data_source.get('sia_url')
            collections = data_source.get('collections', [])

            # Initialize RSP client
            self.rsp_tap_client = create_rsp_client(sia_url=sia_url)

            # Verify we have at least one working service
            if not self.rsp_tap_client.tap_service and not self.rsp_tap_client.sia_service:
                self.logger.error("No RSP services available")
                self.rsp_tap_client = None
            else:
                self.logger.info(f"✓ RSP TAP client initialized for: {server_url}")

        except Exception as e:
            self.logger.error(f"Failed to initialize RSP TAP client: {e}")
            self.rsp_tap_client = None

    def is_available(self) -> bool:
        """
        Check if the data fetcher has any available data clients.

        Returns:
            True if at least one data client is available
        """
        return (self.butler_client is not None or
                self.rsp_tap_client is not None or
                self.config.get('data_source', {}).get('type') == 'data_folder')

    def get_available_services(self) -> Dict[str, bool]:
        """
        Get the status of available data services.

        Returns:
            Dictionary with service availability status
        """
        services = {
            'butler_client': self.butler_client is not None,
            'rsp_tap_client': self.rsp_tap_client is not None,
            'rsp_tap_service': False,
            'rsp_sia_service': False
        }

        if self.rsp_tap_client:
            services['rsp_tap_service'] = self.rsp_tap_client.tap_service is not None
            services['rsp_sia_service'] = self.rsp_tap_client.sia_service is not None

        return services

    # High-level data access methods

    def get_object_catalog(self, ra: float, dec: float, radius: float = 0.1,
                          table: str = "dp02_dc2_catalogs.Object",
                          limit: int = 1000) -> List[Dict]:
        """
        Get object catalog around a sky position.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Search radius in degrees
            table: Catalog table name
            limit: Maximum number of objects to return

        Returns:
            List of object records

        Raises:
            DataAccessError: If data retrieval fails
        """
        if not self.rsp_tap_client or not self.rsp_tap_client.tap_service:
            raise DataAccessError("TAP service not available for catalog queries")

        try:
            return self.rsp_tap_client.get_object_catalog(
                ra=ra, dec=dec, radius=radius, table=table
            )
        except Exception as e:
            raise DataAccessError(f"Failed to retrieve object catalog: {e}")

    def search_images(self, ra: float, dec: float, radius: float = 0.1,
                     bands: List[str] = None) -> List[Dict]:
        """
        Search for images around a sky position.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Search radius in degrees
            bands: Filter bands to search

        Returns:
            List of image metadata records

        Raises:
            DataAccessError: If data retrieval fails
        """
        if not self.rsp_tap_client:
            raise DataAccessError("RSP TAP client not available")

        try:
            return self.rsp_tap_client.search_images(
                ra=ra, dec=dec, size=radius, bands=bands or ['g', 'r', 'i']
            )
        except Exception as e:
            raise DataAccessError(f"Failed to search for images: {e}")

    def get_calexp(self, visit: int, detector: int) -> Optional[Any]:
        """
        Get a calibrated exposure from Butler repository.

        Args:
            visit: Visit identifier
            detector: Detector identifier

        Returns:
            Calibrated exposure object or None if not found

        Raises:
            DataAccessError: If data retrieval fails
        """
        if not self.butler_client:
            raise DataAccessError("Butler client not available")

        try:
            return self.butler_client.get_calexp(visit, detector)
        except Exception as e:
            raise DataAccessError(f"Failed to retrieve calexp: {e}")

    def get_deep_coadd(self, tract: int, patch: str, band: str) -> Optional[Any]:
        """
        Get a deep coadded image from Butler repository.

        Args:
            tract: Skymap tract
            patch: Skymap patch (e.g., "1,2")
            band: Filter band

        Returns:
            Deep coadd object or None if not found

        Raises:
            DataAccessError: If data retrieval fails
        """
        if not self.butler_client:
            raise DataAccessError("Butler client not available")

        try:
            return self.butler_client.get_deep_coadd(tract, patch, band)
        except Exception as e:
            raise DataAccessError(f"Failed to retrieve deep coadd: {e}")

    def get_cutout(self, ra: float, dec: float, size_arcsec: float = 60.0,
                   band: str = "i", dataset_type: str = "deepCoadd",
                   use_bbox: bool = True, backend: str = "auto") -> Optional[Any]:
        """
        Get a cutout from LSST data using coordinate-based efficient retrieval.

        This method demonstrates the full power of the enhanced RIPPLe data access
        with coordinate resolution, bbox optimization, and multi-backend support.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            size_arcsec (float): Cutout size in arcseconds
            band (str): Filter band (g, r, i, z, y)
            dataset_type (str): Type of dataset to retrieve
            use_bbox (bool): Whether to use bbox optimization
            backend (str): Backend to use ("butler", "rsp", or "auto")

        Returns:
            Optional[Any]: Cutout data or None if not found

        Raises:
            DataAccessError: If data retrieval fails
        """
        self.logger.info(f"Retrieving cutout at ({ra:.4f}, {dec:.4f}), size={size_arcsec}\", band={band}")

        # Auto-select backend
        if backend == "auto":
            if self.butler_client and self.butler_client.test_connection():
                backend = "butler"
            elif self.rsp_tap_client and self.rsp_tap_client.test_connection():
                backend = "rsp"
            else:
                raise DataAccessError("No working backend available")

        # Try Butler backend first
        if backend in ["butler", "auto"] and self.butler_client:
            try:
                return self.butler_client.get_cutout(
                    ra=ra, dec=dec, size_arcsec=size_arcsec,
                    band=band, dataset_type=dataset_type
                )
            except Exception as e:
                self.logger.warning(f"Butler cutout failed: {e}")
                if backend == "butler":
                    raise DataAccessError(f"Butler cutout failed: {e}")

        # Fall back to RSP if Butler fails
        if backend in ["rsp", "auto"] and self.rsp_tap_client:
            try:
                return self._get_rsp_cutout_fallback(ra, dec, size_arcsec, band)
            except Exception as e:
                self.logger.error(f"RSP cutout fallback failed: {e}")
                if backend == "rsp":
                    raise DataAccessError(f"RSP cutout failed: {e}")

        raise DataAccessError("All backends failed for cutout retrieval")

    def _get_rsp_cutout_fallback(self, ra: float, dec: float,
                                size_arcsec: float, band: str) -> Optional[Any]:
        """
        Fallback method to get cutout from RSP using TAP queries and DataLink service.

        This method provides a fallback when Butler is not available
        or when explicitly requested by the user.
        """
        try:
            # Search for images around coordinates
            images = self.rsp_tap_client.search_images(
                ra=ra, dec=dec, size=size_arcsec/3600.0,  # Convert arcsec to degrees
                bands=[band]
            )

            if not images:
                self.logger.warning(f"No images found at ({ra}, {dec}) for band {band}")
                return None

            # Get the best image (first result)
            image_info = images[0]

            # Get DataLink URL from access_url
            datalink_url = image_info.get("access_url") or image_info.get("product_url")
            if not datalink_url:
                self.logger.error("No DataLink URL available for cutout")
                return None

            self.logger.info(f"Querying DataLink service for image: {image_info.get('obs_id', 'unknown')}")

            # Get actual image download URL from DataLink service
            image_download_url = self._get_image_url_from_datalink(datalink_url)
            if not image_download_url:
                self.logger.error("Failed to get image download URL from DataLink service")
                return None

            # Download the actual FITS image
            fits_data = self._download_fits_image(image_download_url)
            if not fits_data:
                self.logger.error("Failed to download FITS image")
                return None

            # Extract cutout from the FITS data
            cutout_data = self._extract_cutout_from_fits(
                fits_data, ra, dec, size_arcsec, image_info
            )

            if cutout_data is not None:
                self.logger.info(f"Successfully extracted {band}-band cutout at ({ra:.4f}, {dec:.4f})")
                # Return just the numpy array for compatibility with CutoutSaver
                # The metadata will be handled separately by the preprocessing stage
                return cutout_data
            else:
                self.logger.error("Failed to extract cutout from FITS image")
                return None

        except Exception as e:
            self.logger.error(f"RSP fallback error: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None

    def _get_image_url_from_datalink(self, datalink_url: str) -> Optional[str]:
        """
        Query the DataLink service to get the actual image download URL.

        Args:
            datalink_url: URL to the DataLink service

        Returns:
            Signed URL for downloading the FITS image, or None if failed
        """
        try:
            import requests
            import xml.etree.ElementTree as ET

            # Get the session from the TAP service
            session = getattr(self.rsp_tap_client.tap_service, '_session', None)
            if not session:
                session = getattr(self.rsp_tap_client.tap_service, 'session', None)
            if not session:
                # Create a new session with Bearer token authentication
                session = requests.Session()
                session.headers['Authorization'] = f'Bearer {self.rsp_tap_client.access_token}'

            # Query DataLink service
            self.logger.debug(f"Querying DataLink service: {datalink_url}")
            response = session.get(datalink_url)
            response.raise_for_status()

            # Parse VOTable response using XML parser instead of astropy
            self.logger.debug(f"DataLink response length: {len(response.content)} bytes")

            # Try to get response text for debugging
            try:
                response_text = response.text
                self.logger.debug(f"DataLink response preview: {response_text[:500]}...")
            except:
                response_text = response.content.decode('utf-8', errors='replace')
                self.logger.debug(f"DataLink response preview (decoded): {response_text[:500]}...")

            # Parse XML
            root = ET.fromstring(response.content)

            # Define VOTable namespace
            ns = {'vot': 'http://www.ivoa.net/xml/VOTable/v1.3'}

            # Find the primary image URL
            for resource in root.findall('.//vot:RESOURCE', ns):
                resource_type = resource.get('type')
                if resource_type == 'results':
                    for table in resource.findall('.//vot:TABLE', ns):
                        for row in table.findall('.//vot:TR', ns):
                            access_url_elem = row.find('.//vot:TD', ns)
                            semantics_elem = row.find('.//vot:TD[1]', ns)  # First column is typically semantics
                            content_type_elem = row.findall('.//vot:TD', ns)

                            # Extract values based on position (simpler approach)
                            tds = row.findall('.//vot:TD', ns)
                            if len(tds) >= 2:
                                # First column is typically ID, second is access_url
                                access_url = tds[1].text if tds[1].text else ""

                                # Check if this is the primary image or a FITS file
                                if access_url and access_url != "URL" and access_url.startswith("http"):
                                    self.logger.debug(f"Found potential image URL: {access_url[:100]}...")
                                    return access_url

                # Also check for cutout service definitions
                elif resource.get('utype') == 'adhoc:service':
                    access_url_elem = resource.find('.//vot:PARAM[@name="accessURL"]', ns)
                    if access_url_elem is not None:
                        access_url = access_url_elem.get('value')
                        if access_url and access_url.startswith("http"):
                            self.logger.debug(f"Found service access URL: {access_url}")
                            # This is likely a cutout service, not the direct image
                            continue

            # Alternative: Look for any URL in the response
            import re
            url_pattern = r'https://[^\s<>"\'|{}^[]*\.fits[^\s<>"\'|{}^[]*'
            urls = re.findall(url_pattern, response_text)
            if urls:
                self.logger.debug(f"Found FITS URL via regex: {urls[0][:100]}...")
                return urls[0]

            self.logger.warning("No image URL found in DataLink response")
            return None

        except Exception as e:
            self.logger.error(f"DataLink service query failed: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None

    def _download_fits_image(self, download_url: str) -> Optional[bytes]:
        """
        Download the FITS image from the signed URL.

        Args:
            download_url: Signed URL for the FITS image

        Returns:
            FITS file data as bytes, or None if failed
        """
        try:
            import requests

            self.logger.debug(f"Downloading FITS image from: {download_url}")
            response = requests.get(download_url, timeout=300)
            response.raise_for_status()

            fits_data = response.content
            self.logger.debug(f"Downloaded {len(fits_data)} bytes of FITS data")

            return fits_data

        except Exception as e:
            self.logger.error(f"FITS image download failed: {e}")
            return None

    def _extract_cutout_from_fits(self, fits_data: bytes, ra: float, dec: float,
                                 size_arcsec: float, image_info: dict) -> Optional[np.ndarray]:
        """
        Extract a cutout from FITS image data.

        Args:
            fits_data: FITS file data as bytes
            ra: Center RA in degrees
            dec: Center Dec in degrees
            size_arcsec: Cutout size in arcseconds
            image_info: Image metadata from ObsCore

        Returns:
            Cutout data as numpy array, or None if failed
        """
        try:
            from astropy.io import fits
            from astropy.wcs import WCS
            import astropy.units as u
            from astropy.coordinates import SkyCoord

            # Open FITS file from memory
            with fits.open(io.BytesIO(fits_data)) as hdul:
                # Use the first image extension
                for hdu in hdul:
                    if hdu.data is not None and len(hdu.data.shape) >= 2:
                        image_hdu = hdu
                        break
                else:
                    self.logger.error("No image data found in FITS file")
                    return None

                image_data = image_hdu.data
                header = image_hdu.header

                # Get WCS from header
                wcs = WCS(header)

                # Convert cutout size to pixels
                pixel_scale = 0.2  # LSST pixel scale in arcsec/pixel (approximate)
                size_pixels = int(size_arcsec / pixel_scale)

                # Convert coordinates to pixel coordinates
                coord = SkyCoord(ra=ra*u.degree, dec=dec*u.degree)
                x, y = wcs.world_to_pixel(coord)

                # Calculate cutout bounds
                x_start = max(0, int(x - size_pixels//2))
                x_end = min(image_data.shape[1], int(x + size_pixels//2))
                y_start = max(0, int(y - size_pixels//2))
                y_end = min(image_data.shape[0], int(y + size_pixels//2))

                # Extract cutout
                cutout = image_data[y_start:y_end, x_start:x_end]

                # Check if cutout is the expected size
                if cutout.shape[0] != size_pixels or cutout.shape[1] != size_pixels:
                    self.logger.warning(f"Cutout size {cutout.shape} differs from requested {size_pixels}x{size_pixels}")

                self.logger.debug(f"Extracted cutout with shape {cutout.shape}")
                return cutout

        except Exception as e:
            self.logger.error(f"Cutout extraction failed: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None

    def batch_get_cutouts(self, coordinates: List[Tuple[float, float]],
                        size_arcsec: float = 60.0, band: str = "i",
                        max_workers: int = 4) -> List[Dict[str, Any]]:
        """
        Get multiple cutouts efficiently using parallel processing.

        Args:
            coordinates (List[Tuple[float, float]]): List of (ra, dec) tuples
            size_arcsec (float): Cutout size in arcseconds
            band (str): Filter band
            max_workers (int): Maximum number of parallel workers

        Returns:
            List[Dict[str, Any]]: List of cutout results with metadata
        """
        self.logger.info(f"Retrieving {len(coordinates)} cutouts in parallel")

        def process_coordinate(coord_tuple):
            ra, dec = coord_tuple
            try:
                cutout = self.get_cutout(ra, dec, size_arcsec, band)
                return {
                    "ra": ra,
                    "dec": dec,
                    "cutout": cutout,
                    "status": "success",
                    "error": None
                }
            except Exception as e:
                return {
                    "ra": ra,
                    "dec": dec,
                    "cutout": None,
                    "status": "error",
                    "error": str(e)
                }

        # Process in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all cutout requests
            future_to_coord = {
                executor.submit(process_coordinate, coord): coord for coord in coordinates
            }

            # Collect results
            for future in as_completed(future_to_coord.keys()):
                results.append(future.result())

        successful = sum(1 for r in results if r["status"] == "success")
        self.logger.info(f"Parallel cutout retrieval: {successful}/{len(results)} successful")

        return results

    def get_multi_band_cutout(self, ra: float, dec: float,
                            size_arcsec: float = 60.0,
                            bands: List[str] = ["g", "r", "i"],
                            backend: str = "auto") -> Dict[str, Optional[Any]]:
        """
        Get cutouts in multiple bands for the same coordinates.

        Args:
            ra (float): Right ascension in degrees
            dec (float): Declination in degrees
            size_arcsec (float): Cutout size in arcseconds
            bands (List[str]): List of filter bands
            backend (str): Backend to use

        Returns:
            Dict[str, Optional[Any]]: Dictionary mapping band to cutout
        """
        self.logger.info(f"Retrieving multi-band cutout at ({ra:.4f}, {dec:.4f}) for bands {bands}")

        results = {}
        for band in bands:
            try:
                cutout = self.get_cutout(ra, dec, size_arcsec, band, backend=backend)
                results[band] = cutout
            except Exception as e:
                self.logger.error(f"Failed to get {band}-band cutout: {e}")
                results[band] = None

        successful_bands = [band for band, cutout in results.items() if cutout is not None]
        self.logger.info(f"Multi-band cutout: {len(successful_bands)}/{len(bands)} bands successful")

        return results

    def query_catalog(self, adql_query: str) -> List[Dict]:
        """
        Execute a custom ADQL query on the TAP service.

        Args:
            adql_query: ADQL query string

        Returns:
            List of query results

        Raises:
            DataAccessError: If query fails
        """
        if not self.rsp_tap_client or not self.rsp_tap_client.tap_service:
            raise DataAccessError("TAP service not available for ADQL queries")

        try:
            return self.rsp_tap_client.query_catalog(adql_query)
        except Exception as e:
            raise DataAccessError(f"ADQL query failed: {e}")

    # Utility methods

    def list_available_tables(self) -> List[str]:
        """
        List available tables in the TAP service.

        Returns:
            List of table names

        Raises:
            DataAccessError: If listing tables fails
        """
        if not self.rsp_tap_client or not self.rsp_tap_client.tap_service:
            raise DataAccessError("TAP service not available")

        try:
            return self.rsp_tap_client.list_available_tables()
        except Exception as e:
            raise DataAccessError(f"Failed to list tables: {e}")

    def get_service_status(self) -> Dict[str, Any]:
        """
        Get detailed status of all services.

        Returns:
            Dictionary with service status information
        """
        status = {
            'data_fetcher_available': self.is_available(),
            'services': self.get_available_services(),
            'service_monitor_available': False,
            'tap_status': None,
            'sia_status': None
        }

        if self.rsp_tap_client and self.rsp_tap_client.service_monitor:
            status['service_monitor_available'] = True
            services = self.rsp_tap_client.service_monitor.check_all_services()
            status['tap_status'] = services.get('tap')
            status['sia_status'] = services.get('sia')

        return status

    def print_service_status(self) -> None:
        """Print a detailed service status report."""
        print("\n" + "="*60)
        print("LSST Data Fetcher - Service Status")
        print("="*60)

        services = self.get_available_services()
        print("Available Clients:")
        for service, available in services.items():
            status = "✓" if available else "✗"
            print(f"  {status} {service.replace('_', ' ').title()}")

        if self.rsp_tap_client and self.rsp_tap_client.service_monitor:
            print("\nService Monitor Status:")
            all_services = self.rsp_tap_client.service_monitor.check_all_services()
            for service_name, (status, message) in all_services.items():
                icon = "✓" if status else "✗"
                print(f"  {icon} {service_name.upper()}: {status} - {message}")

        print("="*60)

    def __repr__(self) -> str:
        """String representation of the data fetcher."""
        services = []
        if self.butler_client:
            services.append("Butler")
        if self.rsp_tap_client:
            services.append("RSP-TAP")

        return f"LsstDataFetcher(services={', '.join(services) if services else 'None'})"