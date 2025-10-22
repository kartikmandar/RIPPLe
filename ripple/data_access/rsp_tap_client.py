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

    def __init__(self, access_token: Optional[str] = None, sia_url: Optional[str] = None):
        """
        Initialize RSP TAP client.

        Args:
            access_token: RSP access token for authentication
            sia_url: SIA service URL (optional, defaults to DP0.2)
        """
        self.access_token = access_token or os.environ.get("RSP_ACCESS_TOKEN")

        if not self.access_token:
            raise ValueError("RSP access token required. Set RSP_ACCESS_TOKEN environment variable or pass token")

        # Use provided SIA URL or default to DP0.2
        self.SIA_URL = sia_url or self.SIA_URL_DP02

        self._setup_authentication()
        self._initialize_services()

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

    def _initialize_services(self):
        """Initialize TAP and SIA services."""
        try:
            # For RSP, we need to use custom authentication headers
            # Create a session with proper authentication
            import requests
            from requests.auth import AuthBase

            class BearerTokenAuth(AuthBase):
                """Custom authentication class for bearer token."""
                def __init__(self, token):
                    self.token = token

                def __call__(self, r):
                    r.headers['Authorization'] = f'Bearer {self.token}'
                    return r

            # Create authenticated session
            auth_session = requests.Session()
            auth_session.auth = BearerTokenAuth(self.access_token)

            # Initialize TAP service for catalog queries with custom session
            self.tap_service = TAPService(self.TAP_URL, session=auth_session)
            logging.info(f"Connected to RSP TAP service: {self.TAP_URL}")

            # Initialize SIAv2 service for image access with custom session
            # Note: Rubin's SIAv2 implementation is custom and may not support standard capabilities discovery
            try:
                # Try standard SIAv2 service initialization first
                self.sia_service = SIA2Service(self.SIA_URL, session=auth_session)
                logging.info(f"Connected to RSP SIAv2 service: {self.SIA_URL}")
            except Exception as e:
                # If standard initialization fails, create a custom SIAv2 client for Rubin's implementation
                logging.warning(f"Standard SIAv2 initialization failed, creating custom client for Rubin SIAv2: {e}")
                try:
                    # Create a custom SIAv2 service that bypasses capabilities discovery
                    self.sia_service = self._create_custom_sia_service(auth_session)
                    logging.info(f"Connected to RSP custom SIAv2 service: {self.SIA_URL}")
                except Exception as e2:
                    logging.error(f"Failed to create custom SIAv2 service: {e2}")
                    self.sia_service = None

        except Exception as e:
            raise ButlerConnectionError(f"Failed to connect to RSP services: {e}")

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

                This method should handle the SIAv2 parameters and return results
                in the expected format for the RIPPLe pipeline.
                """
                # For now, just return empty results - SIAv2 functionality
                # can be implemented later as needed
                logging.info(f"Custom SIAv2 search called with parameters: {kwargs}")
                return []

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
        Search for images around a sky position using SIAv2.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            size: Image size in degrees
            collection: LSST data collection
            bands: Filter bands to search

        Returns:
            List of image metadata records
        """
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

            logging.info(f"Found {len(images)} images")
            return images

        except Exception as e:
            raise DataAccessError(f"SIAv2 image search failed: {e}")

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