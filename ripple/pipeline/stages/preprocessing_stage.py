from ripple.utils.logger import Logger
from ripple.pipeline.pipeline_stage import PipelineStage
from ripple.utils.cutout_saver import CutoutSaver
from ripple.data_access import LsstDataFetcher
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

        # Debug: Check actual config structure
        print(f"DEBUG: data_source type: {type(self.config)}")
        print(f"DEBUG: has 'get' method: {hasattr(self.config, 'get')}")
        if hasattr(self.config, 'data_source'):
            ds = getattr(self.config, 'data_source', 'MISSING')
            print(f"DEBUG: data_source found: {type(ds)} - keys: {list(ds.keys()) if isinstance(ds, dict) else 'NOT_DICT'}")
        if hasattr(self.config, 'processing'):
            proc = getattr(self.config, 'processing', None)  # No default to see actual content
            print(f"DEBUG: processing found: {type(proc)} - keys: {list(proc.keys()) if isinstance(proc, dict) else 'NOT_DICT'}")
            if isinstance(proc, dict) and proc:
                # Found valid processing config!
                print(f"DEBUG: Valid processing config found with {len(proc)} keys")
            else:
                print(f"DEBUG: Processing config is empty or invalid: {proc}")
            print(f"DEBUG: All config attributes: {[attr for attr in dir(self.config) if not attr.startswith('_')]}")

        # Debug logging
        print(f"DEBUG: PreprocessingStage.__init__")
        print(f"DEBUG: Full config type: {type(self.config)}")
        print(f"DEBUG: processing_specific_config: {processing_specific_config}")
        print(f"DEBUG: preprocessing_steps: {self.preprocessing_steps}")
        print(f"DEBUG: Config has 'get' method: {hasattr(self.config, 'get')}")

        # Debug: check what's in the config object
        if hasattr(self.config, 'processing'):
            print(f"DEBUG: Config has 'processing' attribute: {getattr(self.config, 'processing', 'NOT_FOUND')}")
        if hasattr(self.config, 'data_source'):
            print(f"DEBUG: Config has 'data_source' attribute: {getattr(self.config, 'data_source', 'NOT_FOUND')}")

        # Debug logging
        print(f"DEBUG: PreprocessingStage.__init__")
        print(f"DEBUG: Full config type: {type(self.config)}")
        print(f"DEBUG: processing_specific_config: {processing_specific_config}")
        print(f"DEBUG: preprocessing_steps: {self.preprocessing_steps}")

        # Initialize cutout saver if output is configured
        self.cutout_saver = None
        self.data_fetcher = None

        # Debug output config
        print(f"DEBUG: output_config = {output_config}")
        print(f"DEBUG: save_cutouts = {output_config.get('save_cutouts', 'NOT_FOUND')}")

        if output_config.get('save_cutouts', False):
            output_dir = output_config.get('directory', './results/cutouts')
            self.cutout_saver = CutoutSaver(output_dir)
            Logger.info(f"✓ Cutout saver initialized with output directory: {output_dir}")
            print(f"DEBUG: CutoutSaver created for directory: {output_dir}")
        else:
            Logger.info("Cutout saving disabled in output configuration")
            print("DEBUG: Cutout saving is DISABLED")

        # Initialize data fetcher (will be configured when execute is called)
        self.data_fetcher = None

    def execute(self, data: Any = None) -> Any:
        """
        Execute the preprocessing stage.

        Args:
            data (Any, optional): Input data for the stage. Defaults to None.

        Returns:
            Any: Output data from the stage.
        """
        Logger.info("Executing Preprocessing Stage")

        # Initialize data fetcher if we have RSP client from previous stage
        if data and isinstance(data, dict) and data.get('rsp_tap_client'):
            try:
                # Get data source configuration from previous stage
                data_source_config = data.get('data_source_config', {})
                Logger.info("Initializing data fetcher for cutout extraction")

                # Create LsstDataFetcher for extracting cutouts
                Logger.info(f"Creating LsstDataFetcher with config: {data_source_config}")
                self.data_fetcher = LsstDataFetcher(data_source_config)
                Logger.info("✓ Data fetcher initialized for cutout extraction")

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
        """Create 64x64 cutouts from coordinates using data fetcher."""
        if self.data_fetcher is None:
            Logger.error("Data fetcher not initialized - cannot create cutouts")
            return data

        try:
            # Get coordinates from data source config
            data_source_config = data.get('data_source_config', {}) if isinstance(data, dict) else {}
            extraction_config = data_source_config.get('extraction', {})
            coordinates = extraction_config.get('coordinates', [])
            bands = params.get('bands', ['g', 'r', 'i'])
            size_arcsec = params.get('size_arcsec', 12.8)

            Logger.info(f"Extracting cutouts for {len(coordinates)} coordinates in bands {bands}")

            if not coordinates:
                Logger.warning("No coordinates found in config - using default test coordinates")
                # Use default coordinates for testing
                coordinates = [
                    {'ra': 62.0, 'dec': -37.0, 'label': 'test_001'},
                    {'ra': 61.5, 'dec': -36.8, 'label': 'test_002'}
                ]

            # Extract cutouts for each coordinate
            extraction_results = []
            for i, coord in enumerate(coordinates):
                try:
                    ra = coord.get('ra', 0.0)
                    dec = coord.get('dec', 0.0)
                    label = coord.get('label', f'coord_{i+1:03d}')

                    Logger.info(f"Extracting cutout {i+1}/{len(coordinates)}: RA={ra:.3f}, Dec={dec:.3f}")

                    # Get multi-band cutout using data fetcher
                    cutouts = self.data_fetcher.get_multi_band_cutout(
                        ra=ra,
                        dec=dec,
                        size_arcsec=size_arcsec,
                        bands=bands
                    )

                    extraction_result = {
                        'ra': ra,
                        'dec': dec,
                        'label': label,
                        'cutout': cutouts,
                        'status': 'success',
                        'error': None
                    }

                    extraction_results.append(extraction_result)
                    Logger.info(f"✓ Successfully extracted cutout {i+1}/{len(coordinates)}")

                except Exception as e:
                    Logger.error(f"Failed to extract cutout {i+1}: {e}")
                    extraction_result = {
                        'ra': coord.get('ra', 0.0),
                        'dec': coord.get('dec', 0.0),
                        'label': coord.get('label', f'coord_{i+1:03d}'),
                        'cutout': None,
                        'status': 'error',
                        'error': str(e)
                    }
                    extraction_results.append(extraction_result)

            # Prepare summary
            successful = sum(1 for r in extraction_results if r['status'] == 'success')
            Logger.info(f"Cutout extraction summary: {successful}/{len(extraction_results)} successful")

            # Save cutouts immediately since RGB composites might not be processed
            if self.cutout_saver is not None:
                saved_count = 0
                for result in extraction_results:
                    if result['status'] == 'success':
                        try:
                            saved_files = self._save_single_composite(result)
                            if saved_files:
                                saved_count += 1
                                Logger.info(f"✓ Saved cutout for {result.get('label', 'unknown')}: {list(saved_files.keys())}")
                        except Exception as e:
                            Logger.error(f"Failed to save cutout for {result.get('label', 'unknown')}: {e}")
                Logger.info(f"Individual band cutouts saved: {saved_count}/{len(extraction_results)} successful")
            else:
                Logger.warning("Cutout saver not available - cutouts not saved to disk")

            # Return data for next stage (will be used by rgb_composite step)
            return {
                'extraction_results': extraction_results,
                'total_coordinates': len(coordinates),
                'successful_extractions': successful,
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
                mapping = rgb_params.get('mapping', {'R': 'r', 'G': 'g', 'B': 'i'})
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