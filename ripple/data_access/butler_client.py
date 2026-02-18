import logging
import os
import time
from typing import Any, Optional, Union, List
from functools import wraps

from lsst.daf.butler import Butler, DatasetNotFoundError, DataIdValueError, ButlerUserError
from lsst.daf.butler._butler import Butler as _ButlerType
from lsst.geom import Box2I, Point2I, Extent2I, SpherePoint, degrees

from ripple.data_access.exceptions import InvalidRepositoryError
from ripple.data_access.config_examples import ButlerConfig

class ButlerClient:
    """
    A client for interacting with the LSST Science Pipelines Butler.

    This class provides a simplified interface for querying and retrieving data
    from a Butler repository, supporting both local and remote (RSP) access.
    """

    def __init__(self, repo_path: str = None, server_url: str = None,
                 collection: str = None, collections: List[str] = None,
                 access_token: str = None, auth_method: str = "none",
                 config: ButlerConfig = None):
        """
        Initializes the ButlerClient.

        Args:
            repo_path (str, optional): The path to the local Butler repository.
            server_url (str, optional): The URL of the remote Butler server.
            collection (str, optional): A single collection to use for data queries.
            collections (List[str], optional): Multiple collections to use for data queries.
            access_token (str, optional): Access token for remote authentication.
            auth_method (str, optional): Authentication method ('token', 'none', 'certificate').
            config (ButlerConfig, optional): Configuration object (alternative to individual parameters).
        """
        # Handle configuration via config object or individual parameters
        if config:
            self.config = config
        else:
            # Create config from individual parameters
            self.config = ButlerConfig(
                repo_path=repo_path,
                server_url=server_url,
                collections=collections or ([collection] if collection else []),
                access_token=access_token,
                auth_method=auth_method
            )

        # Validate configuration
        self._validate_config()

        # Initialize Butler instance
        self.butler = self._initialize_butler()

    def _validate_config(self):
        """Validate the Butler configuration."""
        from ripple.data_access.config_examples import validate_config

        validation = validate_config(self.config)
        if not validation['valid']:
            raise ValueError(f"Invalid Butler configuration: {validation['errors']}")

    def _initialize_butler(self) -> Butler:
        """
        Initializes the Butler instance with appropriate authentication.

        Returns:
            Butler: An instance of the lsst.daf.butler.Butler.

        Raises:
            InvalidRepositoryError: If the repository configuration is invalid.
        """
        try:
            if self.config.auth_method == "token":
                # Remote authenticated Butler connection
                return self._initialize_remote_butler()
            elif self.config.repo_path:
                # Local Butler connection
                return Butler(
                    self.config.repo_path,
                    collections=self.config.collections,
                    run=self.config.collections[0] if self.config.collections else None
                )
            else:
                raise ValueError("Invalid configuration: either repo_path or authenticated server_url required")

        except Exception as e:
            logging.error(f"Failed to initialize Butler: {e}")
            raise InvalidRepositoryError(f"Failed to initialize Butler: {e}")

    def _initialize_remote_butler(self) -> Butler:
        """
        Initialize a remote Butler connection with token authentication using current RSP API.

        Returns:
            Butler: Authenticated Butler instance for remote access.
        """
        try:
            # Set up authentication environment for Butler
            self._setup_authentication()

            # Try lsst.rsp approach first (recommended for RSP)
            # Authentication is already set up by _setup_authentication() call above
            logging.info(f"Attempting to connect to RSP Butler using lsst.rsp module")
            try:
                from lsst.rsp import get_butler

                # Use lsst.rsp to get Butler client
                butler = get_butler(
                    collections=self.config.collections,
                    run=self.config.collections[0] if self.config.collections else None
                )
                logging.info("✓ Connected to RSP Butler using lsst.rsp module")
                return butler

            except ImportError:
                logging.warning("lsst.rsp module not available, falling back to direct Butler access")
            except Exception as e1:
                logging.warning(f"lsst.rsp Butler connection failed: {e1}")

            # Fallback to direct Butler initialization methods
            logging.info(f"Attempting to connect to RSP Butler at: {self.config.server_url}")

            # Method 1: Try with proper RSP endpoint and collections
            try:
                # For current RSP, we need to use the correct endpoint format
                butler = Butler(
                    self.config.server_url,
                    collections=self.config.collections,
                    run=self.config.collections[0] if self.config.collections else None
                )
                logging.info("✓ Connected to RSP Butler using direct method")
                return butler
            except Exception as e2:
                logging.warning(f"Direct Butler connection failed: {e2}")

            # Method 2: Try alternative endpoint format
            try:
                # Try alternative RSP endpoint format
                alt_endpoint = self.config.server_url.replace('/api/butler/', '/butler/')
                butler = Butler(
                    alt_endpoint,
                    collections=self.config.collections,
                    run=self.config.collections[0] if self.config.collections else None
                )
                logging.info(f"✓ Connected to RSP Butler using alternative endpoint: {alt_endpoint}")
                return butler
            except Exception as e3:
                logging.warning(f"Alternative endpoint connection failed: {e3}")

            # Method 3: Try using just the base URL with repo subdirectory
            try:
                base_url = self.config.server_url.split('/api/butler/')[0]
                repo_url = f"{base_url}/butler"
                butler = Butler(
                    repo_url,
                    collections=self.config.collections,
                    run=self.config.collections[0] if self.config.collections else None
                )
                logging.info(f"✓ Connected to RSP Butler using base repo URL: {repo_url}")
                return butler
            except Exception as e4:
                logging.warning(f"Base repo URL connection failed: {e4}")

            # Method 4: Last resort - try without collections first
            try:
                butler = Butler(self.config.server_url)
                logging.info("✓ Connected to generic Butler (limited functionality)")
                return butler
            except Exception as e5:
                logging.error(f"All Butler connection methods failed: {e5}")
                raise InvalidRepositoryError(f"Failed to initialize remote Butler: {e5}")

        except Exception as e:
            logging.error(f"Failed to initialize remote Butler: {e}")
            raise InvalidRepositoryError(f"Failed to initialize remote Butler: {e}")

    def _setup_authentication(self):
        """
        Set up authentication for remote Butler access.

        This method configures the environment for token-based authentication
        with the Rubin Science Platform. It standardizes the environment variables
        used for the access token (ACCESS_TOKEN, LSST_ACCESS_TOKEN, RSP_ACCESS_TOKEN)
        for compatibility, ensuring the same token is used across all.
        """
        if self.config.auth_method == "token":
            # Determine the access token to use, prioritizing config, then environment variables
            token_to_use = self.config.access_token
            if not token_to_use:
                # Fallback to checking environment variables if not in config
                token_to_use = os.environ.get("ACCESS_TOKEN") or \
                                os.environ.get("LSST_ACCESS_TOKEN") or \
                                os.environ.get("RSP_ACCESS_TOKEN")

            if not token_to_use:
                logging.error("Access token is required for token authentication but was not found in config or environment variables.")
                raise ValueError("Access token is required for token authentication but was not found.")

            # Set the standardized token for all relevant environment variables
            os.environ["ACCESS_TOKEN"] = token_to_use
            os.environ["LSST_ACCESS_TOKEN"] = token_to_use
            os.environ["RSP_ACCESS_TOKEN"] = token_to_use
            os.environ["BUTLER_AUTH_USERNAME"] = self.config.token_username

            # Set up HTTP authentication headers for RSP
            os.environ["LSST_HTTP_AUTH_BEARER_TOKEN"] = token_to_use

            # For RSP Butler access, we need to use the data repository URL directly
            # instead of a Butler index file. This is specific to how RSP is configured.
            if self.config.server_url.endswith('/'):
                data_repo_url = f"{self.config.server_url.rstrip('/')}"
            else:
                data_repo_url = self.config.server_url

            if self.config.collections:
                logging.info(f"Configured token-based authentication for remote Butler access.")
                logging.info(f"RSP data repository: {data_repo_url}")
                logging.info(f"Using collections: {self.config.collections}")
            else:
                logging.info(f"Configured token-based authentication for remote Butler access.")
                logging.info(f"RSP data repository: {data_repo_url}")

            logging.info("Standardized ACCESS_TOKEN, LSST_ACCESS_TOKEN, and RSP_ACCESS_TOKEN environment variables.")
            logging.info("Set LSST_HTTP_AUTH_BEARER_TOKEN for HTTP authentication.")

    def get_calexp(self, visit: int, detector: int) -> Optional[Any]:
        """
        Retrieves a calibrated exposure (calexp).

        Args:
            visit (int): The visit ID.
            detector (int): The detector ID.

        Returns:
            Optional[Any]: The calexp object, or None if not found.
        """
        data_id = {"visit": visit, "detector": detector}

        # Validate DataId first
        is_valid, result = self._validate_dataid('calexp', data_id)
        if not is_valid:
            logging.error(f"Invalid DataId for calexp: {result}")
            return None

        try:
            return self.butler.get('calexp', dataId=data_id)
        except DatasetNotFoundError:
            logging.warning(f"calexp not found for visit={visit}, detector={detector}")
            return None
        except Exception as e:
            logging.error(f"Error retrieving calexp for visit={visit}, detector={detector}: {e}")
            return None

    def get_deepCoadd(self, tract: int, patch: int, band: str = None,
                    bbox: Box2I = None, use_bbox: bool = False) -> Optional[Any]:
        """
        Retrieves a deep coadded image (deepCoadd) with optional bbox support.

        Args:
            tract (int): The tract ID.
            patch (int): The patch ID.
            band (str, optional): Filter band (e.g., 'g', 'r', 'i').
            bbox (Box2I, optional): Bounding box for efficient cutout retrieval.
            use_bbox (bool): Whether to use bbox parameter for efficient access.

        Returns:
            Optional[Any]: The deepCoadd object, or None if not found.
        """
        data_id = {"tract": tract, "patch": patch}
        if band:
            data_id["band"] = band

        # Validate DataId first
        is_valid, result = self._validate_dataid('deepCoadd', data_id)
        if not is_valid:
            logging.error(f"Invalid DataId for deepCoadd: {result}")
            return None

        try:
            if use_bbox and bbox:
                # Use bbox-based retrieval for efficiency
                return self._get_with_bbox_retry('deepCoadd', data_id, bbox)
            else:
                # Standard retrieval
                return self.butler.get('deepCoadd', dataId=data_id)
        except DatasetNotFoundError:
            band_str = f", band={band}" if band else ""
            logging.warning(f"deepCoadd not found for tract={tract}, patch={patch}{band_str}")
            return None
        except Exception as e:
            band_str = f", band={band}" if band else ""
            logging.error(f"Error retrieving deepCoadd for tract={tract}, patch={patch}{band_str}: {e}")
            return None

    def get_source_catalog(self, visit: int, detector: int) -> Optional[Any]:
        """
        Retrieves a source catalog.

        Args:
            visit (int): The visit ID.
            detector (int): The detector ID.

        Returns:
            Optional[Any]: The source catalog object, or None if not found.
        """
        data_id = {"visit": visit, "detector": detector}

        # Validate DataId first
        is_valid, result = self._validate_dataid('sourceTable', data_id)
        if not is_valid:
            logging.error(f"Invalid DataId for sourceTable: {result}")
            return None

        try:
            return self.butler.get('sourceTable', dataId=data_id)
        except DatasetNotFoundError:
            logging.warning(f"sourceTable not found for visit={visit}, detector={detector}")
            return None
        except Exception as e:
            logging.error(f"Error retrieving sourceTable for visit={visit}, detector={detector}: {e}")
            return None

    def get_object_catalog(self, tract: int, patch: int, band: str = None) -> Optional[Any]:
        """
        Retrieves an object catalog for coadded data.

        Args:
            tract (int): The tract ID.
            patch (int): The patch ID.
            band (str, optional): Filter band (e.g., 'g', 'r', 'i').

        Returns:
            Optional[Any]: The object catalog, or None if not found.
        """
        data_id = {"tract": tract, "patch": patch}
        if band:
            data_id["band"] = band

        # Validate DataId first
        is_valid, result = self._validate_dataid('objectTable', data_id)
        if not is_valid:
            logging.error(f"Invalid DataId for objectTable: {result}")
            return None

        try:
            return self.butler.get('objectTable', dataId=data_id)
        except DatasetNotFoundError:
            band_str = f", band={band}" if band else ""
            logging.warning(f"objectTable not found for tract={tract}, patch={patch}{band_str}")
            return None
        except Exception as e:
            band_str = f", band={band}" if band else ""
            logging.error(f"Error retrieving objectTable for tract={tract}, patch={patch}{band_str}: {e}")
            return None

    def query_datasets_by_tract(self, dataset_type: str, tract: int,
                                bands: List[str] = None,
                                where_extra: str = None) -> List[Any]:
        """
        Query datasets efficiently for a specific tract.

        Args:
            dataset_type (str): Type of dataset to query (e.g., 'deepCoadd')
            tract (int): Tract number
            bands (List[str], optional): Filter bands to include
            where_extra (str, optional): Additional WHERE clause conditions

        Returns:
            List[Any]: List of dataset references
        """
        try:
            # Build efficient WHERE clause
            where_clause = f"tract = {tract}"
            if where_extra:
                where_clause += f" AND {where_extra}"

            if bands:
                band_list = "', '".join(bands)
                where_clause += f" AND band IN ('{band_list}')"

            refs = list(self.butler.registry.queryDatasets(
                datasetType=dataset_type,
                where=where_clause,
                collections=self.config.collections
            ))

            logging.info(f"Found {len(refs)} datasets for tract {tract}")
            return refs

        except Exception as e:
            logging.error(f"Failed to query datasets for tract {tract}: {e}")
            return []

    def get_available_tracts(self, dataset_type: str = "deepCoadd") -> List[int]:
        """
        Get list of available tracts for a dataset type.

        Args:
            dataset_type (str): Type of dataset to check

        Returns:
            List[int]: List of available tract numbers
        """
        try:
            # Query distinct tracts using registry
            refs = list(self.butler.registry.queryDatasets(
                datasetType=dataset_type,
                collections=self.config.collections
            ))

            # Extract unique tract numbers
            tracts = sorted(set(ref.dataId.get("tract") for ref in refs if ref.dataId.get("tract")))
            logging.info(f"Found {len(tracts)} available tracts for {dataset_type}")
            return tracts

        except Exception as e:
            logging.error(f"Failed to get available tracts: {e}")
            return []

    def get_dataset_metadata(self, dataset_type: str, data_id: dict) -> dict:
        """
        Get metadata for a dataset without loading full data.

        Args:
            dataset_type (str): Type of dataset
            data_id (dict): DataId for dataset

        Returns:
            dict: Metadata dictionary
        """
        try:
            # Get dataset reference
            ref = self.butler.find_dataset(dataset_type, data_id,
                                         collections=self.config.collections)
            if ref is None:
                return {"exists": False, "error": "Dataset not found"}

            # Get component metadata
            metadata = {
                "exists": True,
                "data_id": data_id,
                "run": ref.run,
                "dataset_type": ref.datasetType,
                "components": {}
            }

            # Try to get common components
            components = ["wcs", "bbox", "psf", "metadata"]
            for component in components:
                try:
                    component_path = f"{dataset_type}.{component}"
                    metadata["components"][component] = self.butler.get(component_path, data_id)
                except Exception as e:
                    logging.warning(f"Could not get {component} metadata: {e}")
                    metadata["components"][component] = None

            return metadata

        except Exception as e:
            return {"exists": False, "error": str(e)}

    def batch_get_datasets(self, dataset_refs: List[Any],
                        processor_func: callable = None,
                        max_workers: int = 4) -> List[Any]:
        """
        Retrieve multiple datasets in parallel using threading.

        Args:
            dataset_refs (List[Any]): List of dataset references
            processor_func (callable, optional): Function to process each dataset
            max_workers (int): Maximum number of worker threads

        Returns:
            List[Any]: Processed results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        results = []

        def process_single_ref(ref):
            try:
                data = self.butler.get(ref)
                if processor_func:
                    return processor_func(data, ref)
                else:
                    return {"data": data, "ref": ref, "status": "success"}
            except Exception as e:
                logging.error(f"Failed to process {ref.dataId}: {e}")
                return {"data": None, "ref": ref, "status": "error", "error": str(e)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_ref = {
                executor.submit(process_single_ref, ref): ref for ref in dataset_refs
            }

            # Collect results as they complete
            for future in as_completed(future_to_ref.keys()):
                ref = future_to_ref[future]
                results.append(future.result())

        logging.info(f"Processed {len(results)} datasets in parallel")
        return results

    def test_connection(self) -> bool:
        """
        Test the Butler connection by attempting to query available dataset types.

        Returns:
            bool: True if connection is successful, False otherwise.
        """
        try:
            # Try to list dataset types - this tests the connection
            dataset_types = list(self.butler.registry.queryDatasetTypes())
            logging.info(f"Butler connection successful. Found {len(dataset_types)} dataset types.")
            return True
        except Exception as e:
            logging.error(f"Butler connection test failed: {e}")
            return False

    def _validate_dataid(self, dataset_type: str, data_id: dict) -> tuple[bool, dict]:
        """
        Validate a DataId before using it for Butler operations.

        Returns:
            tuple[bool, dict]: (is_valid, expanded_dataId_or_error_message)
        """
        try:
            # Expand DataId to check if all dimensions are valid
            expanded = self.butler.registry.expandDataId(data_id)

            # Check if dataset actually exists
            ref = self.butler.find_dataset(dataset_type, expanded,
                                         collections=self.config.collections)

            if ref is None:
                return False, f"Dataset {dataset_type} not found for {data_id}"

            return True, expanded

        except DataIdValueError as e:
            return False, f"Invalid DataId values: {e}"
        except Exception as e:
            return False, f"Validation error: {e}"

    def _get_with_bbox_retry(self, dataset_type: str, data_id: dict,
                           bbox: Box2I, max_retries: int = 3) -> Optional[Any]:
        """
        Get dataset with bbox parameter and retry logic.

        This method implements bbox-based cutout retrieval with comprehensive
        error handling and retry logic for production use.
        """
        for attempt in range(max_retries):
            try:
                # Primary retrieval with bbox
                result = self.butler.get(
                    dataset_type,
                    dataId=data_id,
                    parameters={"bbox": bbox, "origin": "PARENT"}
                )
                logging.info(f"Successfully retrieved {dataset_type} with bbox on attempt {attempt + 1}")
                return result

            except DataIdValueError as e:
                # Don't retry DataId errors - these are configuration issues
                logging.error(f"DataId error for {dataset_type}: {e}")
                return None

            except (DatasetNotFoundError, ButlerUserError, Exception) as e:
                # Handle missing data and other errors appropriately
                if "not found" in str(e).lower():
                    logging.warning(f"Dataset {dataset_type} not found: {e}")
                    return None
                else:
                    # Retry other errors
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logging.warning(f"Attempt {attempt + 1} failed for {dataset_type}, retrying in {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Failed to retrieve {dataset_type} after {max_retries} attempts: {e}")
                        return None

    def get_cutout(self, ra: float, dec: float, size_arcsec: float = 60.0,
                    band: str = "i", dataset_type: str = "deepCoadd",
                    skymap: str = None) -> Optional[Any]:
        """
        Get a cutout from deepCoadd at given coordinates using efficient bbox retrieval.

        This method demonstrates production-ready coordinate-based cutout retrieval
        with proper tract/patch resolution and bbox optimization.
        """
        try:
            # Get skymap for coordinate resolution
            if skymap:
                skymap_obj = self.butler.get("skyMap", {"skymap": skymap})
            else:
                skymap_obj = self.butler.get("skyMap")

            # Find tract and patch for coordinates
            coord = SpherePoint(ra * degrees, dec * degrees)
            tract_info = skymap_obj.findTract(coord)
            patch_info = tract_info.findPatch(coord)

            # Build DataId
            data_id = {
                "tract": tract_info.tract_id,
                "patch": patch_info.getIndex(),
                "band": band
            }
            if skymap:
                data_id["skymap"] = skymap

            # Get WCS for coordinate conversion
            wcs = self.butler.get(f"{dataset_type}.wcs", data_id)
            pixel_pos = wcs.skyToPixel(coord)

            # Calculate bbox for cutout
            pixel_scale = wcs.getPixelScale().asArcseconds()
            size_pixels = int(size_arcsec / pixel_scale)

            bbox = Box2I(
                Point2I(int(pixel_pos.x - size_pixels/2),
                       int(pixel_pos.y - size_pixels/2)),
                Extent2I(size_pixels, size_pixels)
            )

            # Use bbox-based retrieval with retry
            return self._get_with_bbox_retry(dataset_type, data_id, bbox)

        except Exception as e:
            logging.error(f"Failed to retrieve cutout at ({ra}, {dec}): {e}")
            return None