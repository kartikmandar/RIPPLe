import logging
import os
from ripple.utils.logger import Logger
from ripple.pipeline.pipeline_stage import PipelineStage
from ripple.data_access import ButlerClient, ButlerConfig, RSPTAPClient, create_rsp_client
from ripple.utils.cutout_saver import CutoutSaver
from typing import Any, Dict, Optional

class DataSourceStage(PipelineStage):
    """
    Pipeline stage for data source operations.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataSourceStage.

        Args:
            config (Optional[Dict[str, Any]]): Full configuration dictionary for the pipeline.
                                               The stage will extract its specific configuration.
        """
        super().__init__(config)
        # Extract specific data_source configuration attributes from the full config
        # Extract specific data source configuration attributes from the full config
        data_source_config = self.config.get('data_source', {})
        self.source_type = data_source_config.get('type', 'data_folder')
        self.source_params = data_source_config.get('params', {})

        self.logger = logging.getLogger(__name__)

        # Store the full data_source config for Butler server access
        self.data_source_config = data_source_config

        # Initialize clients for remote server access
        self.butler_client = None
        self.rsp_tap_client = None

        # Initialize cutout saver for immediate saving
        self.cutout_saver = None
        self._initialize_cutout_saver()

        if self.source_type == 'butler_server':
            # Use PyVO/TAP for external RSP access (recommended approach)
            self._initialize_rsp_tap_client()
        elif self.source_type == 'butler_repo':
            # Use Butler for local repository access
            self._initialize_remote_butler()

    def _initialize_remote_butler(self):
        """Initialize remote Butler client for RSP access."""
        try:
            # Get authentication details from environment, checking for compatibility
            access_token = os.environ.get("RSP_ACCESS_TOKEN") or \
                           os.environ.get("LSST_ACCESS_TOKEN") or \
                           os.environ.get("ACCESS_TOKEN")

            # Validate that an access token is set before proceeding
            if not access_token:
                self.logger.error("RSP_ACCESS_TOKEN (or LSST_ACCESS_TOKEN/ACCESS_TOKEN) environment variable not set. "
                                  "Authentication cannot proceed.")
                self.butler_client = None
                return

            self.logger.info("RSP access token found.")

            # Get RSP configuration from data_source
            server_url = self.data_source_config.get('server_url')
            if not server_url:
                self.logger.error("server_url not found in data_source configuration.")
                self.butler_client = None
                return
            
            auth_method = self.data_source_config.get('auth_method', 'token')
            token_username = self.data_source_config.get('token_username', 'x-oauth-basic')
            collections = self.data_source_config.get('collections', [])

            # Create Butler configuration
            butler_config = ButlerConfig(
                server_url=server_url,
                access_token=access_token,
                token_username=token_username,
                auth_method=auth_method,
                collections=collections
            )

            # Initialize Butler client
            self.butler_client = ButlerClient(config=butler_config)
            self.logger.info(f"Remote Butler client initialized for {server_url}")

        except Exception as e:
            self.logger.error(f"Failed to initialize remote Butler client: {e}")
            self.butler_client = None

    def _initialize_cutout_saver(self):
        """Initialize the cutout saver for immediate saving after each extraction."""
        try:
            # Get output configuration
            output_config = self.config.get('output', {})

            if output_config.get('save_cutouts', False):
                output_dir = output_config.get('directory', './results/cutouts')
                self.cutout_saver = CutoutSaver(output_dir)
                self.logger.info(f"✓ Cutout saver initialized with output directory: {output_dir}")
            else:
                self.logger.info("Cutout saving disabled in output configuration")

        except Exception as e:
            self.logger.error(f"Failed to initialize cutout saver: {e}")
            self.cutout_saver = None

    def _initialize_rsp_tap_client(self):
        """Initialize RSP TAP client using PyVO for external access."""
        try:
            # Get authentication details from environment
            access_token = os.environ.get("RSP_ACCESS_TOKEN") or \
                           os.environ.get("LSST_ACCESS_TOKEN") or \
                           os.environ.get("ACCESS_TOKEN")

            # Validate that an access token is set before proceeding
            if not access_token:
                self.logger.error("RSP_ACCESS_TOKEN environment variable not set. "
                                  "Authentication cannot proceed.")
                self.rsp_tap_client = None
                return

            self.logger.info("RSP access token found.")

            # Get SIA URL from configuration if available
            sia_url = self.data_source_config.get('sia_url')

            # Initialize RSP TAP client using PyVO
            self.rsp_tap_client = create_rsp_client(access_token=access_token, sia_url=sia_url)

            # The client itself already tests and reports service status
            # Just verify we have at least one service available
            if not self.rsp_tap_client.tap_service and not self.rsp_tap_client.sia_service:
                self.logger.error("✗ Neither TAP nor SIAv2 services could be initialized")
                self.rsp_tap_client = None
            else:
                self.logger.info("✓ RSP TAP client initialized with available services")

        except Exception as e:
            self.logger.error(f"Failed to initialize RSP TAP client: {e}")
            self.rsp_tap_client = None

    def _ensure_coordinates(self):
        """
        Ensure coordinates are available. Generate automatically if not provided.
        """
        # Check if coordinates are already provided
        extraction_config = self.data_source_config.get('extraction', {})

        if extraction_config.get('coordinates'):
            self.logger.info(f"Using {len(extraction_config['coordinates'])} provided coordinates")
            return

        # Check if auto-discovery is enabled
        auto_discover = extraction_config.get('auto_discover', {})
        if auto_discover.get('enabled', False):
            self.logger.info("Auto-discovery enabled - generating coordinates...")
            self._auto_generate_coordinates()
        else:
            self.logger.warning("No coordinates provided and auto-discovery disabled. No data will be extracted.")

    def _auto_generate_coordinates(self):
        """
        Automatically generate coordinates for complete DP1 field coverage.
        """
        extraction_config = self.data_source_config.get('extraction', {})
        fields = extraction_config.get('fields', [])
        grid_config = extraction_config.get('grid_sampling', {})

        spacing_arcmin = grid_config.get('spacing_arcmin', 1.0)
        max_per_field = grid_config.get('max_points_per_field', 100)

        all_coordinates = []

        for field in fields:
            field_name = field['name']
            center_ra = float(field['center_ra'])
            center_dec = float(field['center_dec'])
            radius_deg = float(field['radius'])

            self.logger.info(f"Generating coordinates for field: {field_name}")

            # Generate grid coordinates
            field_coords = self._generate_field_grid(
                center_ra, center_dec, radius_deg,
                spacing_arcmin, field_name, max_per_field
            )

            all_coordinates.extend(field_coords)
            self.logger.info(f"Generated {len(field_coords)} coordinates for {field_name}")

        # Apply overall limit (if any - set to very high for no limits)
        max_total = self.config.get('advanced', {}).get('extraction_limits', {}).get('max_total_cutouts', 999999)
        if len(all_coordinates) > max_total:
            self.logger.warning(f"Limiting to {max_total} coordinates (was {len(all_coordinates)})")
            all_coordinates = all_coordinates[:max_total]
        else:
            self.logger.info(f"No overall limit applied - processing all {len(all_coordinates)} coordinates")

        # Update the configuration with generated coordinates
        self.data_source_config['extraction']['coordinates'] = all_coordinates
        self.logger.info(f"Total coordinates generated: {len(all_coordinates)}")

    def _generate_field_grid(self, center_ra, center_dec, radius_deg, spacing_arcmin, field_name, max_points):
        """
        Generate a grid of coordinates for a specific field.

        Args:
            center_ra (float): Field center RA in degrees
            center_dec (float): Field center Dec in degrees
            radius_deg (float): Field radius in degrees
            spacing_arcmin (float): Grid spacing in arcminutes
            field_name (str): Name of the field
            max_points (int): Maximum points to generate

        Returns:
            list: List of coordinate dictionaries
        """
        spacing_deg = float(spacing_arcmin) / 60.0

        # Calculate grid bounds
        ra_min = center_ra - radius_deg + spacing_deg/2
        ra_max = center_ra + radius_deg - spacing_deg/2
        dec_min = center_dec - radius_deg + spacing_deg/2
        dec_max = center_dec + radius_deg - spacing_deg/2

        coordinates = []

        # Generate grid points
        num_ra_points = int((ra_max - ra_min) / spacing_deg) + 1
        num_dec_points = int((dec_max - dec_min) / spacing_deg) + 1

        # Calculate total points and limit if necessary
        total_points = num_ra_points * num_dec_points
        if total_points > max_points:
            # Reduce grid density to fit within limit
            scale_factor = (max_points / total_points) ** 0.5
            num_ra_points = max(1, int(num_ra_points * scale_factor))
            num_dec_points = max(1, int(num_dec_points * scale_factor))
            self.logger.warning(f"Reducing grid density for {field_name} to {num_ra_points}x{num_dec_points}")

        # Clean field name for labels
        clean_name = field_name.replace(" ", "_").replace("-", "_")

        for i in range(num_ra_points):
            for j in range(num_dec_points):
                ra = ra_min + i * (ra_max - ra_min) / max(1, num_ra_points - 1)
                dec = dec_min + j * (dec_max - dec_min) / max(1, num_dec_points - 1)

                coordinates.append({
                    'ra': round(ra, 6),
                    'dec': round(dec, 6),
                    'label': f"{clean_name}_{i:02d}_{j:02d}",
                    'field': field_name
                })

        return coordinates

    def execute(self, data: Any = None) -> Any:
        """
        Execute the data source stage. This method contains the core logic for the stage.

        Args:
            data (Any, optional): Input data for the stage. Defaults to None.

        Returns:
            Any: Output data from the stage.
        """
        self.logger.info(f"Executing Data Source Stage (Type: {self.source_type})")

        # Auto-generate coordinates if not provided
        if self.source_type == 'butler_server':
            self._ensure_coordinates()

        if self.source_type == 'data_folder':
            data_path = self.source_params.get('path')
            self.logger.info(f"Processing data_folder source with path: {data_path}")
            if data_path and os.path.isdir(data_path):
                self.logger.info(f"Data folder found at: {data_path}")
                # Placeholder: In a real scenario, you would load or process data from this folder
                # For now, we'll just confirm its existence.
                data = {"status": "success", "message": f"Data folder verified at {data_path}", "path": data_path}
            else:
                self.logger.error(f"Data folder not found or path not provided: {data_path}")
                # Optionally, raise an exception or return an error status
                data = {"status": "error", "message": f"Data folder not found at {data_path}"}

        elif self.source_type == 'butler_repo':
            repo_path = self.source_params.get('path')
            self.logger.info(f"Processing butler_repo source with path: {repo_path}")
            if repo_path and os.path.isdir(repo_path):
                # A more robust check for a Butler repository might involve looking for specific files
                # like 'butler.yaml' or a '.butler' directory.
                # For now, we'll check if the path is a directory.
                self.logger.info(f"Butler repository directory found at: {repo_path}")
                # Placeholder: In a real scenario, you would connect to the Butler repository
                # and possibly fetch data or metadata.
                data = {"status": "success", "message": f"Butler repository verified at {repo_path}", "path": repo_path}
            else:
                self.logger.error(f"Butler repository not found or path not provided: {repo_path}")
                # Optionally, raise an exception or return an error status
                data = {"status": "error", "message": f"Butler repository not found at {repo_path}"}

        elif self.source_type == 'butler_server':
            self.logger.info("Processing butler_server source (RSP)")
            if self.rsp_tap_client:
                # Use PyVO/TAP client for RSP access
                self.logger.info("Using RSP TAP client for external access")

                # Get configuration
                collections = self.data_source_config.get('collections', [])
                server_url = self.data_source_config.get('server_url', 'unknown')

                # Test TAP connection with retry logic
                connection_success = False
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if self.rsp_tap_client.test_connection():
                            self.logger.info("✓ RSP TAP connection successful")
                            connection_success = True
                            break
                        else:
                            self.logger.warning(f"TAP connection test returned False (attempt {attempt + 1}/{max_retries})")
                    except Exception as e:
                        self.logger.warning(f"TAP connection attempt {attempt + 1}/{max_retries} failed: {e}")
                        if attempt < max_retries - 1:
                            self.logger.info("Retrying TAP connection in 5 seconds...")
                            import time
                            time.sleep(5)

                if not connection_success:
                    self.logger.error("✗ RSP TAP connection failed after multiple attempts")
                    data = {"status": "error", "message": "RSP TAP connection failed"}
                else:
                    # Get available tables to verify access
                    try:
                        tables = self.rsp_tap_client.list_available_tables()[:10]  # First 10 tables
                        self.logger.info(f"✓ Found {len(tables)} tables available")

                        # Test SIAv2 service if available
                        sia_available = False
                        if self.rsp_tap_client.sia_service:
                            try:
                                # Perform a sample SIAv2 search
                                test_results = self._test_sia_search()
                                sia_available = True
                                self.logger.info(f"✓ SIAv2 service working (found {len(test_results)} sample results)")
                            except Exception as e:
                                self.logger.warning(f"SIAv2 test failed: {e}")
                        else:
                            self.logger.info("SIAv2 service not available - will skip image searches")

                        # IMMEDIATE EXTRACTION AND SAVING
                        self._perform_immediate_extraction()

                        data = {
                            "status": "success",
                            "message": f"RSP TAP server connected at {server_url}",
                            "server_url": server_url,
                            "collections": collections,
                            "tables": tables,
                            "rsp_tap_client": self.rsp_tap_client,
                            "client_type": "pyvo_tap",
                            "sia_available": sia_available,
                            # Pass data source config and client to next stage for extraction
                            "data_source_config": self.data_source_config
                        }
                    except Exception as e:
                        self.logger.warning(f"Could not list tables: {e}")
                        data = {
                            "status": "success",
                            "message": f"RSP TAP server connected at {server_url}",
                            "server_url": server_url,
                            "collections": collections,
                            "rsp_tap_client": self.rsp_tap_client,
                            "client_type": "pyvo_tap"
                        }

        else:
            self.logger.warning(f"Unsupported data source type: {self.source_type}. Performing no operation.")
            data = {"status": "skipped", "message": f"Unsupported data source type: {self.source_type}"}

        if data.get("status") == "success":
            Logger.success("✓ Data Source Stage completed successfully")
        else:
            Logger.error("✗ Data Source Stage completed with errors")

        return data

    def _test_sia_search(self):
        """
        Test SIAv2 search with a simple query.

        Returns:
            list: Sample SIAv2 search results
        """
        if not self.rsp_tap_client or not self.rsp_tap_client.sia_service:
            return []

        # Search for images in a known DP0.2 area (tract 4431, patch 2,3)
        # Convert to RA,Dec coordinates (approximately)
        ra, dec = 62.0, -37.0  # Approximate center of DP0.2
        radius = 0.1  # degrees

        search_params = {
            'POS': f'{ra},{dec};{radius}',
            'BAND': 'i',  # i-band
            'MAXREC': 5,  # Just a few test results
            'INSTRUMENT': 'LSSTCam'
        }

        try:
            results = self.rsp_tap_client.sia_service.search(**search_params)
            return results
        except Exception as e:
            self.logger.error(f"SIAv2 test search failed: {e}")
            return []

    def search_images(self, ra: float, dec: float, radius: float = 0.1,
                     bands: list = None, max_results: int = 10):
        """
        Search for images using SIAv2 service with graceful fallback.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            radius: Search radius in degrees
            bands: List of filter bands (e.g., ['g', 'r', 'i'])
            max_results: Maximum number of results to return

        Returns:
            list: SIAv2 search results or empty list if service unavailable
        """
        if not self.rsp_tap_client:
            self.logger.warning("RSP TAP client not available for image search")
            return []

        if not self.rsp_tap_client.sia_service:
            # Check if SIAv2 service is known to be unavailable
            if self.rsp_tap_client.service_monitor and not self.rsp_tap_client.service_monitor.is_service_available('sia'):
                sia_status, sia_message = self.rsp_tap_client.service_monitor.get_service_status('sia')
                self.logger.warning(f"SIAv2 service unavailable: {sia_message}")
                self.logger.info("Image search skipped - SIAv2 service down")
                return []
            else:
                self.logger.warning("SIAv2 service not initialized for image search")
                return []

        search_params = {
            'POS': f'{ra},{dec};{radius}',
            'MAXREC': max_results,
            'INSTRUMENT': 'LSSTCam'
        }

        if bands:
            search_params['BAND'] = ','.join(bands)

        try:
            results = self.rsp_tap_client.sia_service.search(**search_params)
            self.logger.info(f"SIAv2 search found {len(results)} images")
            return results
        except Exception as e:
            self.logger.error(f"SIAv2 image search failed: {e}")
            return []

    def _extract_and_save_cutout_immediately(self, ra: float, dec: float, label: str, bands: list):
        """
        Extract and save a cutout immediately after successful extraction.

        Args:
            ra: Right ascension in degrees
            dec: Declination in degrees
            label: Label for this cutout
            bands: List of bands to extract
        """
        if not self.cutout_saver:
            return  # Skip saving if cutout saver not initialized

        try:
            from ripple.data_access.data_fetcher import LsstDataFetcher

            # Create a temporary data fetcher for this extraction
            temp_fetcher = LsstDataFetcher(self.data_source_config)

            # Extract multi-band cutout
            # Get cutout size from processing config (12.8 arcseconds for 64x64 pixels)
            processing_config = self.config.get('processing', {})
            cutout_params = processing_config.get('params', {})
            cutout_creation_params = cutout_params.get('cutout_creation', {})
            size_arcsec = cutout_creation_params.get('size_arcsec', 12.8)

            cutout_result = temp_fetcher.get_multi_band_cutout(ra, dec, size_arcsec, bands)

            # Check if we got any successful cutouts
            successful_bands = [band for band, cutout in cutout_result.items() if cutout is not None]

            if successful_bands:
                # Save immediately using cutout saver
                saved_files = self.cutout_saver.save_multi_band_cutouts(
                    cutouts=cutout_result,
                    ra=ra,
                    dec=dec,
                    label=label,
                    additional_metadata={
                        'extraction_method': 'immediate',
                        'field': 'DP1',
                        'bands': successful_bands
                    }
                )

                if saved_files:
                    self.logger.info(f"✓ Immediately saved cutout for {label}: {list(saved_files.keys())}")
                else:
                    self.logger.warning(f"Failed to save cutout for {label}")
            else:
                self.logger.error(f"Failed to extract cutout for {label}: no successful bands")

        except Exception as e:
            self.logger.error(f"Error in immediate extraction and saving for {label}: {e}")

    def _perform_immediate_extraction(self):
        """Perform immediate extraction and saving for all coordinates."""
        if not self.cutout_saver:
            self.logger.info("Skipping immediate extraction - cutout saver not initialized")
            return

        # Get coordinates from extraction config
        extraction_config = self.data_source_config.get('extraction', {})
        coordinates = extraction_config.get('coordinates', [])
        bands = extraction_config.get('bands', ['g', 'r', 'i'])

        if not coordinates:
            self.logger.warning("No coordinates found for immediate extraction")
            return

        self.logger.info(f"Starting immediate extraction for {len(coordinates)} coordinates")

        successful_extractions = 0
        failed_extractions = 0

        for idx, coord in enumerate(coordinates, 1):
            ra = coord['ra']
            dec = coord['dec']
            label = coord.get('label', f"coord_{idx:05d}")

            self.logger.info(f"Extracting cutout {idx}/{len(coordinates)}: RA={ra}, Dec={dec}")

            try:
                self._extract_and_save_cutout_immediately(ra, dec, label, bands)
                successful_extractions += 1
            except Exception as e:
                self.logger.error(f"Failed to extract and save cutout {idx}: {e}")
                failed_extractions += 1

        self.logger.info(f"Immediate extraction completed: {successful_extractions} successful, {failed_extractions} failed")