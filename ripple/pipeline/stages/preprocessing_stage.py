from ripple.utils.logger import Logger
from ripple.pipeline.pipeline_stage import PipelineStage
from ripple.utils.cutout_saver import CutoutSaver
from ripple.data_access import LsstDataFetcher
from ripple.preprocessing import Preprocessor, CutoutCreator, PreprocessingConfig
from typing import Any, Dict, Optional, List, Tuple

class PreprocessingStage(PipelineStage):
    """
    Pipeline stage for data preprocessing operations.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the PreprocessingStage.

        Args:
            config (Optional[Dict[str, Any]]): Full configuration dictionary for the pipeline.
                                               The stage will extract its specific configuration.
        """
        super().__init__(config)
        # Extract specific processing configuration attributes from the full config
        # Handle both plain dict and RepoConfig objects
        if hasattr(self.config, 'get'):
            # RepoConfig object
            processing_specific_config = self.config.get('processing', {})
            output_config = self.config.get('output', {})
        else:
            # Plain dictionary
            processing_specific_config = self.config.get('processing', {})
            output_config = self.config.get('output', {})

        self.preprocessing_steps = processing_specific_config.get('steps', [])
        self.preprocessing_params = processing_specific_config.get('params', {})

        # Initialize cutout saver if output is configured
        self.cutout_saver = None
        self.data_fetcher = None

        if output_config.get('save_cutouts', False):
            output_dir = output_config.get('directory', './results/cutouts')
            self.cutout_saver = CutoutSaver(output_dir)
            Logger.info(f"✓ Cutout saver initialized with output directory: {output_dir}")
        else:
            Logger.info("Cutout saving disabled in output configuration")

    def execute(self, data: Any = None) -> Any:
        """
        Execute the preprocessing stage.

        Args:
            data (Any, optional): Input data for the stage. Defaults to None.

        Returns:
            Any: Output data from the stage.
        """
        Logger.info("Executing Preprocessing Stage")

        # Initialize a data fetcher for BOTH backends (RSP server or local
        # Butler / data_folder). The local-Butler path was previously
        # unreachable because it was gated on the presence of an RSP client.
        if data and isinstance(data, dict):
            try:
                Logger.info("Initializing data fetcher for cutout extraction")
                self.data_fetcher = self._build_data_fetcher(data)
                if self.data_fetcher is not None:
                    Logger.info("✓ Data fetcher initialized for cutout extraction")
                else:
                    Logger.info("No data fetcher available for this configuration")
            except Exception as e:
                Logger.error(f"Failed to initialize data fetcher: {e}")
                return data

        if not self.preprocessing_steps:
            Logger.info("No preprocessing steps configured. Skipping.")
            return data

        Logger.info(f"Found {len(self.preprocessing_steps)} preprocessing steps: {self.preprocessing_steps}")
        for step in self.preprocessing_steps:
            Logger.info(f"Applying preprocessing step: {step}")
            if step == 'cleaning':
                Logger.info("Performing data cleaning.")
                # Add logic for data cleaning
                # Example: data = self._clean_data(data, self.preprocessing_params.get('cleaning', {}))
            elif step == 'transformation':
                Logger.info("Performing data transformation.")
                # Add logic for data transformation
                # Example: data = self._transform_data(data, self.preprocessing_params.get('transformation', {}))
            elif step == 'normalization' or step == 'band_normalization':
                if step == 'band_normalization':
                    Logger.info("Performing band normalization.")
                    data = self._normalize_bands(data, self.preprocessing_params.get('band_normalization', {}))
                else:
                    Logger.info("Performing data normalization.")
                    # Add logic for data normalization
                    # Example: data = self._normalize_data(data, self.preprocessing_params.get('normalization', {}))
            elif step == 'cutout_creation':
                Logger.info("Performing cutout creation.")
                data = self._create_cutouts(data, self.preprocessing_params.get('cutout_creation', {}))
            elif step == 'rgb_composite':
                Logger.info("Creating RGB composites.")
                if self.cutout_saver:
                    Logger.info("Saving RGB composites to disk.")
                    data = self._save_rgb_composites(data)
                else:
                    Logger.info("Processing RGB composites in memory.")
                    data = self._process_rgb_composites(data)
            else:
                Logger.warning(f"Unknown preprocessing step: {step}. Skipping.")
        
        Logger.success("✓ Preprocessing Stage completed")
        return data

    def _build_data_fetcher(self, data: Dict[str, Any]) -> Optional[LsstDataFetcher]:
        """Build an LsstDataFetcher for the active backend.

        Returns an RSP-aware fetcher when the previous stage provided an
        ``rsp_tap_client`` (reusing that already-authenticated client), and
        otherwise constructs a fetcher from ``data_source_config`` for the
        local butler_repo / data_folder configuration. This makes both the
        remote-RSP and local-Butler paths reachable from a single entry point.
        """
        data_source_config = data.get('data_source_config', {})

        # NOTE: for a ``butler_server`` config, LsstDataFetcher.__init__ will
        # attempt its own RSP client initialization here (and may emit a
        # spurious connection error in the log) before we override it below.
        # The explicit ``fetcher.rsp_tap_client = ...`` override always wins, so
        # the upstream, already-authenticated client is the one actually used.
        Logger.info(f"Creating LsstDataFetcher with config: {data_source_config}")
        fetcher = LsstDataFetcher(data_source_config)

        # Reuse the RSP client already built (and authenticated) upstream so we
        # do not re-create the remote connection in the preprocessing stage.
        rsp_tap_client = data.get('rsp_tap_client')
        if rsp_tap_client is not None:
            Logger.info("Reusing RSP TAP client provided by the data source stage")
            fetcher.rsp_tap_client = rsp_tap_client

        return fetcher

    def _collect_coordinates(self, data: Any) -> List[Dict[str, Any]]:
        """Collect the coordinate list from the pipeline data dict.

        Supports both shapes seen in the pipeline contract:
        ``data['coordinates']`` and the nested
        ``data['data_source_config']['extraction']['coordinates']``.
        """
        if not isinstance(data, dict):
            return []

        coordinates = data.get('coordinates')
        if coordinates:
            return coordinates

        data_source_config = data.get('data_source_config', {})
        extraction_config = data_source_config.get('extraction', {})
        return extraction_config.get('coordinates', [])

    # Private helper methods for preprocessing steps
    def _clean_data(self, data: Any, params: Dict[str, Any]) -> Any:
        """Clean data by removing bad pixels and applying masks."""
        if data is None:
            return data

        # Placeholder for actual cleaning logic
        Logger.info("Applying data cleaning (bad pixel removal, masking)")
        return data

    def _transform_data(self, data: Any, params: Dict[str, Any]) -> Any:
        """Apply data transformations."""
        if data is None:
            return data

        # Placeholder for actual transformation logic
        Logger.info("Applying data transformations")
        return data

    def _normalize_data(self, data: Any, params: Dict[str, Any]) -> Any:
        """Normalize data using specified method."""
        if data is None:
            return data

        method = params.get('method', 'asinh')
        scale_factor = params.get('scale_factor', 1.0)

        Logger.info(f"Normalizing data using {method} method (scale: {scale_factor})")

        # Placeholder for actual normalization logic
        return data

    def _normalize_bands(self, data: Any, params: Dict[str, Any]) -> Any:
        """Normalize multi-band data using specified method."""
        if data is None:
            return data

        method = params.get('method', 'asinh')
        scale_factor = params.get('scale_factor', 1.0)
        normalize_bands = params.get('normalize_bands', True)

        Logger.info(f"Normalizing bands using {method} method (scale: {scale_factor})")

        try:
            # Handle extraction results with cutouts
            if isinstance(data, dict) and 'extraction_results' in data:
                extraction_results = data['extraction_results']
                Logger.info(f"Normalizing {len(extraction_results)} extraction results")

                for result in extraction_results:
                    if result['status'] == 'success' and result.get('cutout'):
                        # Apply normalization to each band in the cutout
                        cutouts = result['cutout']
                        if isinstance(cutouts, dict):
                            for band, cutout_data in cutouts.items():
                                if cutout_data is not None:
                                    # Apply asinh normalization
                                    if method == 'asinh':
                                        # Simple asinh normalization: asinh(data * scale_factor)
                                        import numpy as np
                                        if hasattr(cutout_data, 'array'):  # Butler exposure object
                                            normalized = np.arcsinh(cutout_data.array * scale_factor)
                                            cutout_data.array = normalized
                                        elif hasattr(cutout_data, 'data'):  # Numpy array with metadata
                                            normalized = np.arcsinh(cutout_data.data * scale_factor)
                                            cutout_data.data = normalized
                                        else:  # Direct numpy array
                                            normalized = np.arcsinh(cutout_data * scale_factor)
                                            cutouts[band] = normalized

                Logger.info("✓ Band normalization completed")

            return data

        except Exception as e:
            Logger.error(f"Band normalization failed: {e}")
            return data

    def _create_cutouts(self, data: Any, params: Dict[str, Any]) -> Any:
        """Create cutouts and delegate the cutout->tensor work to Preprocessor.

        Extraction happens exactly once, here, via CutoutCreator (removing the
        duplicate fetch+save that previously overlapped with
        DataSourceStage._perform_immediate_extraction). The Preprocessor turns
        the per-coordinate band dicts into a dense tensor plus a manifest, which
        are placed back into the returned data dict alongside the legacy
        ``extraction_results`` so downstream steps keep working.
        """
        if self.data_fetcher is None:
            Logger.error("Data fetcher not initialized - cannot create cutouts")
            return data

        try:
            # Build the preprocessing configuration from the stage params so the
            # tensor side (size, bands, normalization, ...) is config-driven.
            config = PreprocessingConfig.from_dict(params)
            bands = list(config.bands)

            coordinates = self._collect_coordinates(data)
            Logger.info(f"Extracting cutouts for {len(coordinates)} coordinates in bands {bands}")

            if not coordinates:
                Logger.warning("No coordinates found in config - using default test coordinates")
                # Use default coordinates for testing
                coordinates = [
                    {'ra': 62.0, 'dec': -37.0, 'label': 'test_001'},
                    {'ra': 61.5, 'dec': -36.8, 'label': 'test_002'}
                ]

            # Single extraction: CutoutCreator forwards each coordinate to the
            # shared data_fetcher.get_multi_band_cutout call.
            cutout_creator = CutoutCreator(self.data_fetcher, config)

            extraction_results = []
            items = []
            for i, coord in enumerate(coordinates):
                ra = coord.get('ra', 0.0)
                dec = coord.get('dec', 0.0)
                label = coord.get('label', f'coord_{i+1:03d}')

                Logger.info(f"Extracting cutout {i+1}/{len(coordinates)}: RA={ra:.3f}, Dec={dec:.3f}")

                try:
                    cutouts = cutout_creator.create(ra, dec)
                    extraction_results.append({
                        'ra': ra, 'dec': dec, 'label': label,
                        'cutout': cutouts, 'status': 'success', 'error': None
                    })
                    items.append({
                        'bands': cutouts,
                        'meta': {'ra': ra, 'dec': dec, 'label': label},
                    })
                    Logger.info(f"✓ Successfully extracted cutout {i+1}/{len(coordinates)}")
                except Exception as e:
                    Logger.error(f"Failed to extract cutout {i+1}: {e}")
                    extraction_results.append({
                        'ra': ra, 'dec': dec, 'label': label,
                        'cutout': None, 'status': 'error', 'error': str(e)
                    })

            successful = sum(1 for r in extraction_results if r['status'] == 'success')
            Logger.info(f"Cutout extraction summary: {successful}/{len(extraction_results)} successful")

            # Delegate cutout->tensor conversion + manifest to the Preprocessor.
            out_dir = self.cutout_saver.get_output_directory() if self.cutout_saver is not None else None
            Logger.info(f"Delegating {len(items)} cutouts to Preprocessor (out_dir={out_dir})")
            preprocessor = Preprocessor(config)
            result = preprocessor.run(items, out_dir=out_dir)
            Logger.info(
                f"Preprocessor accepted {len(result.accepted_indices)}/{len(items)} cutouts"
            )

            # Return data for next stage (will be used by rgb_composite step).
            return {
                'extraction_results': extraction_results,
                'total_coordinates': len(coordinates),
                'successful_extractions': successful,
                'tensor': result.tensor,
                'preprocess_manifest': result.manifest,
                'status': 'completed'
            }

        except Exception as e:
            Logger.error(f"Cutout creation failed: {e}")
            return data

    def _save_rgb_composites(self, data: Any) -> Any:
        """Save RGB composites using the cutout saver."""
        if data is None or self.cutout_saver is None:
            return data

        try:
            # Handle extraction results from cutout creation step
            if isinstance(data, dict) and 'extraction_results' in data:
                extraction_results = data['extraction_results']
                Logger.info(f"Processing {len(extraction_results)} extraction results for saving")

                saved_count = 0
                for result in extraction_results:
                    if result['status'] == 'success':
                        try:
                            saved_files = self._save_single_composite(result)
                            if saved_files:
                                saved_count += 1
                        except Exception as e:
                            Logger.error(f"Failed to save composite for {result['label']}: {e}")

                Logger.info(f"RGB composites saved: {saved_count}/{len(extraction_results)} successful")
                data['rgb_saved'] = saved_count
                data['rgb_total'] = len(extraction_results)

            else:
                # Fallback to old logic for backward compatibility
                if hasattr(data, 'get') or isinstance(data, dict):
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if hasattr(value, '__contains__') and isinstance(value, dict):
                                if 'ra' in value and 'dec' in value:
                                    self._save_single_composite(value)
                    elif hasattr(data, '__iter__'):
                        for i, cutout_data in enumerate(data):
                            if hasattr(cutout_data, 'get') and cutout_data.get('cutout'):
                                self._save_single_composite(cutout_data, label=f"batch_{i+1:03d}")

            Logger.success("✓ RGB composite saving completed")

        except Exception as e:
            Logger.error(f"Failed to save RGB composites: {e}")

        return data

    def _save_single_composite(self, cutout_data: Dict, label: str = None):
        """Save a single RGB composite from cutout data."""
        try:
            ra = cutout_data.get('ra', 0.0)
            dec = cutout_data.get('dec', 0.0)
            cutouts = cutout_data.get('cutout', {})

            # Save the multi-band cutouts
            saved_files = self.cutout_saver.save_multi_band_cutouts(
                cutouts=cutouts,
                ra=ra,
                dec=dec,
                label=label,
                additional_metadata=cutout_data.get('metadata', {})
            )

            Logger.info(f"✓ Saved cutout at RA={ra:.3f}, Dec={dec:.3f}")
            return saved_files

        except Exception as e:
            Logger.error(f"Failed to save single composite: {e}")
            return {}

    def _process_rgb_composites(self, data: Any) -> Any:
        """Process RGB composites in memory without saving to disk."""
        if data is None:
            return data

        try:
            # Handle extraction results from cutout creation step
            if isinstance(data, dict) and 'extraction_results' in data:
                extraction_results = data['extraction_results']
                Logger.info(f"Processing {len(extraction_results)} extraction results for RGB composites")

                rgb_params = self.preprocessing_params.get('rgb_composite', {})
                # Default to the canonical RGB band order (i->R, r->G, g->B),
                # derived from CutoutSaver.RGB_BAND_ORDER so the in-memory path
                # and the on-disk composite never diverge.
                red_band, green_band, blue_band = CutoutSaver.RGB_BAND_ORDER
                mapping = rgb_params.get(
                    'mapping', {'R': red_band, 'G': green_band, 'B': blue_band}
                )
                stretch_method = rgb_params.get('stretch_method', 'asinh')
                clip_percentiles = rgb_params.get('clip_percentiles', [1, 99])

                processed_count = 0
                for result in extraction_results:
                    if result['status'] == 'success':
                        try:
                            rgb_composite = self._create_single_rgb_composite(result, mapping, stretch_method, clip_percentiles)
                            if rgb_composite is not None:
                                result['rgb_composite'] = rgb_composite
                                processed_count += 1
                        except Exception as e:
                            Logger.error(f"Failed to create RGB composite for {result['label']}: {e}")

                Logger.info(f"RGB composites processed: {processed_count}/{len(extraction_results)} successful")
                data['rgb_processed'] = processed_count
                data['rgb_total'] = len(extraction_results)

            Logger.success("✓ RGB composite processing completed")

        except Exception as e:
            Logger.error(f"Failed to process RGB composites: {e}")

        return data

    def _create_single_rgb_composite(self, cutout_data: Dict, mapping: Dict, stretch_method: str, clip_percentiles: List) -> Optional[Any]:
        """Create a single RGB composite from cutout data."""
        try:
            ra = cutout_data.get('ra', 0.0)
            dec = cutout_data.get('dec', 0.0)
            cutouts = cutout_data.get('cutout', {})

            # Get the data for each band according to mapping
            rgb_arrays = {}
            for color, band in mapping.items():
                if band in cutouts and cutouts[band] is not None:
                    cutout_data = cutouts[band]
                    if hasattr(cutout_data, 'array'):  # Butler exposure object
                        rgb_arrays[color] = cutout_data.array
                    elif hasattr(cutout_data, 'data'):  # Numpy array with metadata
                        rgb_arrays[color] = cutout_data.data
                    else:  # Direct numpy array
                        rgb_arrays[color] = cutout_data

            if len(rgb_arrays) < 3:
                Logger.warning(f"Insufficient bands for RGB composite at RA={ra:.3f}, Dec={dec:.3f}")
                return None

            # Create RGB composite
            import numpy as np
            rgb_shape = rgb_arrays['R'].shape
            rgb_image = np.zeros((rgb_shape[0], rgb_shape[1], 3), dtype=np.float32)

            for i, color in enumerate(['R', 'G', 'B']):
                if color in rgb_arrays:
                    band_data = rgb_arrays[color].astype(np.float32)

                    # Apply stretch method
                    if stretch_method == 'asinh':
                        band_data = np.arcsinh(band_data)
                    elif stretch_method == 'log':
                        band_data = np.log10(band_data + 1)
                    elif stretch_method == 'sqrt':
                        band_data = np.sqrt(band_data)

                    # Clip percentiles
                    if clip_percentiles and len(clip_percentiles) == 2:
                        vmin, vmax = np.percentile(band_data[~np.isnan(band_data)], clip_percentiles)
                        band_data = np.clip((band_data - vmin) / (vmax - vmin), 0, 1)

                    rgb_image[:, :, i] = band_data

            return rgb_image

        except Exception as e:
            Logger.error(f"Failed to create single RGB composite: {e}")
            return None