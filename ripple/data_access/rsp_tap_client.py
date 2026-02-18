"""
RSP TAP Client for External Access

This module provides PyVO-based access to the Rubin Science Platform TAP service
for external data access, following the RSP API aspect documentation.
"""

import os
import logging
from typing import Optional, List, Dict, Any
import numpy as np

try:
    import pyvo
    from pyvo.dal import TAPService, TAPQuery
    from pyvo.dal.sia2 import SIA2Service, SIA2Query
except ImportError:
    raise ImportError("PyVO is required for RSP external access. Install with: pip install pyvo")

from .exceptions import DataAccessError, ButlerConnectionError
from .service_monitor import create_service_monitor


class RSPTAPClient:
    """
    Client for accessing Rubin Science Platform data via PyVO TAP service.

    This follows the RSP API aspect documentation for external access using
    IVOA standards (TAP, SIAv2, SODA).
    """

    # RSP TAP endpoint (from RSP documentation)
    TAP_URL = "https://data.lsst.cloud/api/tap"
    # SIA endpoints are different for different data releases
    # DP0.2: https://data.lsst.cloud/api/dp02/query
    # DP1: https://data.lsst.cloud/api/dp1/query
    SIA_URL_DP02 = "https://data.lsst.cloud/api/dp02/query"
    SIA_URL_DP1 = "https://data.lsst.cloud/api/dp1/query"
    SIA_URL = SIA_URL_DP02  # Default to DP0.2

    def __init__(self, access_token: Optional[str] = None, sia_url: Optional[str] = None,
                 enable_service_monitor: bool = True):
        """
        Initialize RSP TAP client.

        Args:
            access_token: RSP access token for authentication
            sia_url: SIA service URL (optional, defaults to DP0.2)
            enable_service_monitor: Enable service status monitoring
        """
        self.access_token = access_token or os.environ.get("RSP_ACCESS_TOKEN")

        if not self.access_token:
            raise ValueError("RSP access token required. Set RSP_ACCESS_TOKEN environment variable or pass token")

        # Use provided SIA URL or default to DP0.2
        self.SIA_URL = sia_url or self.SIA_URL_DP02

        # Initialize service monitor
        self.service_monitor = create_service_monitor(
            tap_url=self.TAP_URL,
            sia_url=self.SIA_URL
        ) if enable_service_monitor else None

        self._setup_authentication()
        self._initialize_services()

        # Check service status and provide user feedback
        if self.service_monitor:
            self._check_and_report_service_status()

    def _setup_authentication(self):
        """Set up authentication for RSP TAP access."""
        # Set up authentication for PyVO
        # PyVO supports bearer token authentication via environment variables
        os.environ["TAP_AUTH_TOKEN"] = self.access_token

        # Also set up for SIAv2 service
        # PyVO uses different environment variables for different services
        os.environ["SIA_AUTH_TOKEN"] = self.access_token

        # Try setting common authentication environment variables
        os.environ["X_AUTH_TOKEN"] = self.access_token
        os.environ["AUTH_TOKEN"] = self.access_token

        logging.info("RSP TAP and SIA authentication configured")

    def _check_and_report_service_status(self):
        """Check service status and provide user feedback."""
        if not self.service_monitor:
            return

        print("\n" + "="*60)
        print("RSP Service Status Check")
        print("="*60)

        services = self.service_monitor.check_all_services()

        for service_name, (status, message) in services.items():
            if service_name == 'tap':
                service_display = "TAP (Catalog Service)"
            elif service_name == 'sia':
                service_display = "SIAv2 (Image Service)"
            else:
                service_display = service_name.upper()

            status_icon = "✓" if status else "✗"
            status_text = "UP" if status else "DOWN"
            print(f"{status_icon} {service_display}: {status_text}")
            print(f"   Details: {message}")

        # Provide recommendations
        recommendations = self.service_monitor.get_service_recommendations()

        print("\nRecommendations:")
        for i, rec in enumerate(recommendations['recommendations'], 1):
            print(f"{i}. {rec}")

        if recommendations['workarounds']:
            print("\nWorkarounds:")
            for i, workaround in enumerate(recommendations['workarounds'], 1):
                print(f"• {workaround}")

        print("="*60)

    def _initialize_services(self):
        """Initialize TAP and SIA services with graceful handling of unavailability."""
        try:
            import requests
            from requests.auth import AuthBase, HTTPBasicAuth

            # Check service availability before attempting connections
            tap_available = True
            sia_available = True

            if self.service_monitor:
                tap_available, tap_message = self.service_monitor.get_service_status('tap')
                sia_available, sia_message = self.service_monitor.get_service_status('sia')

                if not tap_available:
                    logging.warning(f"TAP service unavailable: {tap_message}")
                if not sia_available:
                    logging.warning(f"SIAv2 service unavailable: {sia_message}")

            # Create separate authentication sessions for TAP and SIAv2
            # TAP service uses Bearer token authentication
            class BearerTokenAuth(AuthBase):
                """Custom authentication class for bearer token."""
                def __init__(self, token):
                    self.token = token

                def __call__(self, r):
                    r.headers['Authorization'] = f'Bearer {self.token}'
                    return r

            # Initialize TAP service for catalog queries
            self.tap_service = None
            if tap_available:
                try:
                    # Create TAP session with Bearer token authentication
                    tap_session = requests.Session()
                    tap_session.auth = BearerTokenAuth(self.access_token)

                    # Initialize TAP service for catalog queries
                    self.tap_service = TAPService(self.TAP_URL, session=tap_session)
                    logging.info(f"Connected to RSP TAP service: {self.TAP_URL}")
                except Exception as e:
                    logging.error(f"Failed to initialize TAP service: {e}")
                    self.tap_service = None
            else:
                logging.info("Skipping TAP service initialization - service unavailable")

            # Initialize SIAv2 service for image access
            self.sia_service = None
            if sia_available:
                try:
                    # SIAv2 service uses Basic authentication with x-oauth-basic as username
                    # Create SIAv2 session with Basic authentication
                    sia_session = requests.Session()
                    sia_session.auth = HTTPBasicAuth("x-oauth-basic", self.access_token)

                    # Note: Rubin's SIAv2 implementation is custom and may not support standard capabilities discovery
                    try:
                        # Try standard SIAv2 service initialization first
                        self.sia_service = SIA2Service(self.SIA_URL, session=sia_session)
                        logging.info(f"Connected to RSP SIAv2 service: {self.SIA_URL}")
                    except Exception as e:
                        # If standard initialization fails, create a custom SIAv2 client for Rubin's implementation
                        logging.warning(f"Standard SIAv2 initialization failed, creating custom client for Rubin SIAv2: {e}")
                        try:
                            # Create a custom SIAv2 service that bypasses capabilities discovery
                            self.sia_service = self._create_custom_sia_service(sia_session)
                            logging.info(f"Connected to RSP custom SIAv2 service: {self.SIA_URL}")
                        except Exception as e2:
                            logging.error(f"Failed to create custom SIAv2 service: {e2}")
                            self.sia_service = None
                except Exception as e:
                    logging.error(f"Failed to initialize SIAv2 service: {e}")
                    self.sia_service = None
            else:
                logging.info("Skipping SIAv2 service initialization - service unavailable")

            # Log summary of initialization
            if self.tap_service and self.sia_service:
                logging.info("✓ Both TAP and SIAv2 services initialized successfully")
            elif self.tap_service:
                logging.info("✓ TAP service initialized, SIAv2 service unavailable")
            elif self.sia_service:
                logging.info("✓ SIAv2 service initialized, TAP service unavailable")
            else:
                logging.warning("✗ Neither TAP nor SIAv2 services could be initialized")

        except Exception as e:
            raise ButlerConnectionError(f"Failed to initialize RSP services: {e}")

    def _create_custom_sia_service(self, auth_session):
        """
        Create a custom SIAv2 service for Rubin's implementation.

        Rubin's SIAv2 service doesn't follow standard capabilities discovery,
        so we create a minimal service that can handle direct queries.
        """
        class CustomSIA2Service:
            """Custom SIAv2 service for Rubin's implementation."""

            def __init__(self, base_url, session):
                self.base_url = base_url
                self.session = session
                self._capabilities = {
                    'capability': {
                        'image': True,
                        'metadata': True,
                        'cutout': True
                    }
                }

            def capabilities(self):
                """Return default capabilities for Rubin SIAv2."""
                return self._capabilities

            def search(self, **kwargs):
                """
                Execute SIAv2 search against Rubin's custom implementation.

                This method handles SIAv2 parameters and returns structured results
                for the RIPPLe pipeline.

                Args:
                    **kwargs: SIAv2 parameters (POS, BAND, MAXREC, TIME, etc.)

                Returns:
                    List of dictionaries containing image metadata
                """
                import json
                import urllib.parse

                logging.info(f"Executing SIAv2 search with parameters: {kwargs}")

                try:
                    # Convert SIAv2 parameters to Rubin's format
                    params = self._convert_sia_parameters(kwargs)

                    # Make POST request to Rubin SIAv2 endpoint
                    response = self.session.post(
                        self.base_url,
                        data=params,
                        headers={
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Accept': 'application/json'
                        }
                    )

                    response.raise_for_status()

                    # Parse Rubin's response format
                    results = self._parse_sia_response(response)

                    logging.info(f"SIAv2 search returned {len(results)} results")
                    return results

                except Exception as e:
                    logging.error(f"SIAv2 search failed: {e}")
                    return []

            def _convert_sia_parameters(self, kwargs):
                """
                Convert standard SIAv2 parameters to Rubin's format.

                Args:
                    kwargs: Standard SIAv2 parameters

                Returns:
                    dict: Parameters formatted for Rubin's SIAv2 endpoint
                """
                params = {}

                # Position search (RA,Dec;radius)
                if 'POS' in kwargs:
                    params['POS'] = kwargs['POS']

                # Filter bands
                if 'BAND' in kwargs:
                    # Convert list or single band to string
                    bands = kwargs['BAND']
                    if isinstance(bands, (list, tuple)):
                        params['BAND'] = ','.join(bands)
                    else:
                        params['BAND'] = bands

                # Maximum number of results
                if 'MAXREC' in kwargs:
                    params['MAXREC'] = str(kwargs['MAXREC'])

                # Time range (ISO format)
                if 'TIME' in kwargs:
                    params['TIME'] = kwargs['TIME']

                # Instrument
                if 'INSTRUMENT' in kwargs:
                    params['INSTRUMENT'] = kwargs['INSTRUMENT']

                # Exposure time
                if 'EXPTIME' in kwargs:
                    params['EXPTIME'] = kwargs['EXPTIME']

                # Calibration level
                if 'CALIB' in kwargs:
                    params['CALIB'] = kwargs['CALIB']

                return params

            def _parse_sia_response(self, response):
                """
                Parse Rubin's SIAv2 response into structured format.

                Args:
                    response: HTTP response from Rubin SIAv2 endpoint

                Returns:
                    List of dictionaries with image metadata
                """
                try:
                    # Try to parse as JSON first
                    if response.headers.get('content-type', '').startswith('application/json'):
                        data = response.json()
                        return self._parse_json_response(data)

                    # Try to parse as VOTable
                    elif 'votable' in response.headers.get('content-type', '').lower():
                        return self._parse_votable_response(response.text)

                    # Fallback: try to parse as text
                    else:
                        return self._parse_text_response(response.text)

                except Exception as e:
                    logging.error(f"Failed to parse SIAv2 response: {e}")
                    return []

            def _parse_json_response(self, data):
                """Parse JSON response from Rubin SIAv2."""
                results = []

                # Handle different JSON structures Rubin might return
                if isinstance(data, dict):
                    if 'data' in data:
                        # Standard format with data array
                        for item in data['data']:
                            results.append(self._normalize_image_metadata(item))
                    elif 'results' in data:
                        # Alternative format
                        for item in data['results']:
                            results.append(self._normalize_image_metadata(item))
                    elif isinstance(data, list):
                        # Direct array of results
                        for item in data:
                            results.append(self._normalize_image_metadata(item))

                return results

            def _parse_votable_response(self, votable_text):
                """Parse VOTable response from Rubin SIAv2."""
                try:
                    from astropy.io.votable import parse
                    votable = parse(votable_text)
                    results = []

                    for table in votable.iter_tables():
                        for row in table.array:
                            metadata = {}
                            for col_name, col_value in zip(table.colnames, row):
                                metadata[col_name] = col_value
                            results.append(self._normalize_image_metadata(metadata))

                    return results

                except ImportError:
                    logging.warning("astropy not available for VOTable parsing")
                    return []
                except Exception as e:
                    logging.error(f"VOTable parsing failed: {e}")
                    return []

            def _parse_text_response(self, text):
                """Parse plain text response (fallback)."""
                # Basic text parsing - would need to know Rubin's text format
                logging.warning("Text response parsing not implemented")
                return []

            def _normalize_image_metadata(self, raw_metadata):
                """
                Normalize image metadata from various formats to standard structure.

                Args:
                    raw_metadata: Raw metadata from Rubin's response

                Returns:
                    dict: Normalized image metadata
                """
                normalized = {
                    'id': raw_metadata.get('id', ''),
                    'title': raw_metadata.get('title', ''),
                    'instrument': raw_metadata.get('instrument', ''),
                    'band': raw_metadata.get('band', ''),
                    'ra': float(raw_metadata.get('ra', 0)),
                    'dec': float(raw_metadata.get('dec', 0)),
                    'size_arcsec': float(raw_metadata.get('size_arcsec', 0)),
                    'access_url': raw_metadata.get('access_url', ''),
                    'access_format': raw_metadata.get('access_format', ''),
                    'fov': raw_metadata.get('fov', ''),
                    'exptime': float(raw_metadata.get('exptime', 0)),
                    'obs_collection': raw_metadata.get('obs_collection', ''),
                    't_exptime': raw_metadata.get('t_exptime', ''),
                    't_min': raw_metadata.get('t_min', ''),
                    't_max': raw_metadata.get('t_max', ''),
                    'em_band': raw_metadata.get('em_band', ''),
                    'publisher_id': raw_metadata.get('publisher_id', ''),
                    'reference': raw_metadata.get('reference', ''),
                    'mirror_radius': raw_metadata.get('mirror_radius', ''),
                    'skypol': raw_metadata.get('skypol', ''),
                    's_region': raw_metadata.get('s_region', ''),
                    's_resolution': raw_metadata.get('s_resolution', ''),
                    's_xel1': raw_metadata.get('s_xel1', ''),
                    's_xel2': raw_metadata.get('s_xel2', ''),
                    's_ucd': raw_metadata.get('s_ucd', ''),
                    's_utype': raw_metadata.get('s_utype', ''),
                    'dataproduct_type': raw_metadata.get('dataproduct_type', 'image'),
                    'obs_id': raw_metadata.get('obs_id', ''),
                    'collection': raw_metadata.get('collection', ''),
                    'facet': raw_metadata.get('facet', ''),
                    'maxrec': raw_metadata.get('maxrec', ''),
                    'format': raw_metadata.get('format', ''),
                    'filesize': raw_metadata.get('filesize', ''),
                }

                return normalized

            def run_sync(self, **kwargs):
                """Alias for search method."""
                return self.search(**kwargs)

        return CustomSIA2Service(self.SIA_URL, auth_session)

    def test_connection(self) -> bool:
        """Test connection to RSP TAP service."""
        try:
            # Simple query to test connection
            query = "SELECT TOP 1 * FROM TAP_SCHEMA.tables"
            result = self.tap_service.search(query)

            if result and len(result) > 0:
                logging.info("✓ RSP TAP connection successful")
                return True
            else:
                logging.error("✗ RSP TAP connection test failed - no results")
                return False

        except Exception as e:
            logging.error(f"✗ RSP TAP connection test failed: {e}")
            return False

    def query_catalog(self, adql_query: str) -> List[Dict]:
        """
        Execute ADQL query on RSP TAP service.

        Args:
            adql_query: ADQL query string

        Returns:
            List of dictionaries with query results
        """
        try:
            logging.info(f"Executing ADQL query: {adql_query[:100]}...")

            # Execute query
            result = self.tap_service.search(adql_query)

            # Convert to list of dictionaries
            if result:
                data = result.to_table().to_pandas().to_dict('records')
                logging.info(f"Query returned {len(data)} rows")
                return data
            else:
                logging.warning("Query returned no results")
                return []

        except Exception as e:
            raise DataAccessError(f"ADQL query failed: {e}")

    def get_object_catalog(self, ra: float, dec: float, radius: float = 0.1,
                          table: str = "dp02_dc2_catalogs.Object") -> List[Dict]:
        """
        Get object catalog around a sky position.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Search radius in degrees
            table: Catalog table name

        Returns:
            List of object records
        """
        # Use ADQL cone search
        adql_query = f"""
        SELECT TOP 1000
               objectId, coord_ra, coord_dec,
               g_cModelFlux, r_cModelFlux, i_cModelFlux,
               g_cModelFluxErr, r_cModelFluxErr, i_cModelFluxErr,
               ext_shapeHSM_HsmPsfReg, ext_shapeHSM_HsmShapeReg,
               detect_isPrimary
        FROM {table}
        WHERE CONTAINS(POINT('ICRS', coord_ra, coord_dec),
                      CIRCLE('ICRS', {ra}, {dec}, {radius})) = 1
        AND detect_isPrimary = 1
        ORDER BY g_cModelFlux DESC
        """

        return self.query_catalog(adql_query)

    def search_images(self, ra: float, dec: float, size: float = 0.1,
                     collection: str = "2.2i/runs/DP0.2",
                     bands: List[str] = ["g", "r", "i"]) -> List[Dict]:
        """
        Search for images around a sky position using TAP/ObsTAP (fallback from SIAv2).

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            size: Image size in degrees
            collection: LSST data collection
            bands: Filter bands to search

        Returns:
            List of image metadata records
        """
        # First try SIAv2 if available
        if self.sia_service and self.service_monitor and self.service_monitor.is_service_available('sia'):
            try:
                # Create SIAv2 query
                query = self.sia_service.create_query(
                    pos=f"{ra},{dec}",
                    size=size,
                    collection=collection,
                    dataproduct_type="image"
                )

                # Execute query
                result = query.execute()

                # Filter by bands if specified
                images = []
                for record in result:
                    band = record.get("band", "").lower()
                    if not bands or band in bands:
                        images.append(dict(record))

                logging.info(f"Found {len(images)} images via SIAv2")
                return images

            except Exception as e:
                logging.warning(f"SIAv2 search failed, falling back to TAP: {e}")

        # Fallback to TAP/ObsTAP
        return self._search_images_via_tap(ra, dec, size, collection, bands)

    def _search_images_via_tap(self, ra: float, dec: float, size: float = 0.1,
                               collection: str = "2.2i/runs/DP0.2",
                               bands: List[str] = ["g", "r", "i"]) -> List[Dict]:
        """
        Search for images using TAP service with ObsCore table.

        This is the fallback method when SIAv2 is not available.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            size: Search radius in degrees
            collection: LSST data collection
            bands: Filter bands to search

        Returns:
            List of image metadata records
        """
        if not self.tap_service:
            raise DataAccessError("TAP service not available")

        try:
            # Build ADQL query for ObsCore
            # Convert size to search radius for the query
            search_radius = size

            # Create band constraint for ADQL
            band_constraint = ""
            if bands and len(bands) > 0:
                band_list = "', '".join(bands)
                band_constraint = f"AND lsst_band IN ('{band_list}')"

            # Main ObsCore query - less restrictive for DP1 data
            query = f"""
            SELECT dataproduct_type, dataproduct_subtype, calib_level,
                   lsst_band, em_min, em_max, lsst_tract, lsst_patch,
                   lsst_filter, lsst_visit, lsst_detector, t_exptime,
                   t_min, t_max, s_ra, s_dec, s_fov, obs_id,
                   obs_collection, o_ucd, facility_name, instrument_name,
                   s_region, access_url, access_format
            FROM ivoa.ObsCore
            WHERE calib_level = 2
              AND dataproduct_type = 'image'
              {band_constraint}
              AND CONTAINS(POINT('ICRS', {ra}, {dec}), s_region) = 1
            ORDER BY lsst_visit DESC
            """

            logging.info(f"Executing TAP ObsCore query for images at ({ra:.3f}, {dec:.3f})")

            # Execute the query
            job = self.tap_service.submit_job(query)
            job.run()
            job.wait(phases=['COMPLETED', 'ERROR'])

            if job.phase == 'ERROR':
                raise DataAccessError(f"TAP query failed: {job.error_msg}")

            # Get results
            results = job.fetch_result().to_table()

            # Convert to list of dictionaries
            images = []
            for row in results:
                image_dict = dict(row)
                # Normalize band names to lowercase
                if 'lsst_band' in image_dict:
                    image_dict['band'] = image_dict['lsst_band'].lower()
                images.append(image_dict)

            logging.info(f"Found {len(images)} images via TAP ObsCore")
            return images

        except Exception as e:
            logging.error(f"TAP ObsCore search failed: {e}")
            raise DataAccessError(f"TAP image search failed: {e}")

    def get_image_cutout(self, image_uri: str, ra: float, dec: float,
                        size: float = 0.05) -> Optional[np.ndarray]:
        """
        Retrieve image cutout using SODA service.

        Args:
            image_uri: URI of the image to cut out
            ra: Center RA in degrees
            dec: Center Dec in degrees
            size: Cutout size in degrees

        Returns:
            Numpy array with image data, or None if failed
        """
        try:
            # This would use SODA service for cutouts
            # Implementation depends on RSP SODA endpoint
            logging.warning("SODA cutout service not yet implemented")
            return None

        except Exception as e:
            raise DataAccessError(f"SODA cutout failed: {e}")

    def list_available_tables(self) -> List[str]:
        """List available tables in RSP TAP schema."""
        try:
            query = """
            SELECT table_name
            FROM TAP_SCHEMA.tables
            WHERE schema_name = 'dp02_dc2_catalogs'
            ORDER BY table_name
            """
            result = self.query_catalog(query)
            return [row["table_name"] for row in result]

        except Exception as e:
            raise DataAccessError(f"Failed to list tables: {e}")

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get column information for a specific table."""
        try:
            query = f"""
            SELECT column_name, data_type, description
            FROM TAP_SCHEMA.columns
            WHERE table_name = '{table_name}'
            ORDER BY column_name
            """
            result = self.query_catalog(query)
            return {row["column_name"]: row["data_type"] for row in result}

        except Exception as e:
            raise DataAccessError(f"Failed to get table schema: {e}")


def create_rsp_client(access_token: Optional[str] = None, sia_url: Optional[str] = None) -> RSPTAPClient:
    """
    Factory function to create RSP TAP client.

    Args:
        access_token: RSP access token
        sia_url: SIA service URL (optional, defaults to DP0.2)

    Returns:
        Initialized RSPTAPClient
    """
    return RSPTAPClient(access_token=access_token, sia_url=sia_url)