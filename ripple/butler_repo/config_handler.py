"""
Configuration handler for Butler repository management.

This module handles loading, validation, and management of configuration
for Butler repository creation and data ingestion.
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DataSourceConfig:
    """Configuration for data source."""
    type: str  # 'butler_repo', 'butler_server', 'data_folder'
    path: Optional[str] = None
    server_url: Optional[str] = None
    collections: List[str] = field(default_factory=list)
    create_if_missing: bool = True


@dataclass
class InstrumentConfig:
    """Configuration for instrument."""
    name: str  # e.g., 'HSC', 'LSSTCam', 'DECam'
    class_name: str  # e.g., 'lsst.obs.subaru.HyperSuprimeCam'
    filters: List[str] = field(default_factory=list)
    detector_list: Optional[List[int]] = None


@dataclass
class IngestionConfig:
    """Configuration for data ingestion."""
    raw_data_pattern: Optional[str] = None  # Glob pattern for raw data
    calibration_path: Optional[str] = None
    reference_catalog_path: Optional[str] = None
    transfer_mode: str = "symlink"  # symlink, copy, move, direct
    define_visits: bool = True
    write_curated_calibrations: bool = True
    skip_existing: bool = True
    processes: int = 1  # Number of parallel processes


@dataclass
class ButlerConfig:
    """Butler-specific configuration."""
    dimension_config: Optional[str] = None
    seed_config: Optional[str] = None
    standalone: bool = False
    override: bool = False
    registry_db: str = "sqlite"  # sqlite or postgresql
    postgres_url: Optional[str] = None


@dataclass
class ProcessingConfig:
    """Configuration for data processing."""
    cutout_size: int = 64
    batch_size: int = 32
    max_workers: int = 4
    cache_size: int = 1000
    enable_performance_monitoring: bool = True
    output_dir: Optional[str] = None


@dataclass
class RepoConfig:
    """Complete repository configuration."""
    # Core configurations that have a fixed structure
    instrument: InstrumentConfig
    butler: ButlerConfig

    # Pipeline stage configurations, kept as flexible dictionaries
    data_source: Dict[str, Any]
    ingestion: Dict[str, Any]
    processing: Dict[str, Any]
    model: Dict[str, Any] # Added for the model stage
    output: Dict[str, Any] # Added for output configuration

    def get(self, key: str, default=None):
        """
        Get configuration value by key, with optional default.
        This makes RepoConfig compatible with dict-like access.
        """
        # Try to get from stage-specific configs first
        for stage_config in [self.data_source, self.ingestion, self.processing, self.model, self.output]:
            if key in stage_config:
                return stage_config[key]

        # Fallback to instrument and butler configs
        if hasattr(self.instrument, key):
            return getattr(self.instrument, key)
        if hasattr(self.butler, key):
            return getattr(self.butler, key)

        return default

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'RepoConfig':
        """Create RepoConfig from dictionary."""
        # Get instrument config and validate required fields
        instrument_config_dict = config_dict.get('instrument', {})
        if 'name' not in instrument_config_dict or 'class_name' not in instrument_config_dict:
            missing_fields = []
            if 'name' not in instrument_config_dict:
                missing_fields.append('name')
            if 'class_name' not in instrument_config_dict:
                missing_fields.append('class_name')
            raise ValueError(f"Missing required instrument configuration fields: {', '.join(missing_fields)}")
        
        return cls(
            instrument=InstrumentConfig(**instrument_config_dict),
            butler=ButlerConfig(**config_dict.get('butler', {})),
            data_source=config_dict.get('data_source', {}),
            ingestion=config_dict.get('ingestion', {}),
            processing=config_dict.get('processing', {}),
            model=config_dict.get('model', {}),
            output=config_dict.get('output', {})
        )


def load_config(config_path: Union[str, Path]) -> RepoConfig:
    """
    Load configuration from YAML file.
    
    Parameters
    ----------
    config_path : str or Path
        Path to configuration YAML file
        
    Returns
    -------
    RepoConfig
        Loaded configuration object
        
    Raises
    ------
    FileNotFoundError
        If config file doesn't exist
    ValueError
        If config file is invalid
    """
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
            
        # Expand environment variables
        config_dict = _expand_env_vars(config_dict)
        
        # Create config object
        config = RepoConfig.from_dict(config_dict)
        
        # Validate configuration
        validate_config(config)
        
        return config
        
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML configuration: {e}")
    except Exception as e:
        raise ValueError(f"Error loading configuration: {e}")


def validate_config(config: RepoConfig) -> None:
    """
    Validate configuration.
    
    Parameters
    ----------
    config : RepoConfig
        Configuration to validate
        
    Raises
    ------
    ValueError
        If configuration is invalid
    """
    # Validate instrument
    if not config.instrument.name:
        raise ValueError("Instrument name is required")
    
    if not config.instrument.class_name:
        raise ValueError("Instrument class name is required")
    
    # Validate butler config
    if config.butler.registry_db not in ['sqlite', 'postgresql', 'remote']:
        raise ValueError(f"Invalid registry database: {config.butler.registry_db}")

    if config.butler.registry_db == 'postgresql' and not config.butler.postgres_url:
        raise ValueError("PostgreSQL URL required when using PostgreSQL registry")
    
    # Note: Pipeline stage configurations (data_source, ingestion, processing, model)
    # are now flexible dictionaries and are not validated here.
    # Individual stages are responsible for validating their own configurations.

    # Validate RSP-specific configuration if needed
    validate_rsp_configuration(config)

    logger.info("Core configuration validated successfully")


def _expand_env_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively expand environment variables in configuration."""
    if isinstance(config_dict, dict):
        # Special handling for RSP authentication
        result = {}
        for k, v in config_dict.items():
            if k == 'auth_method' and v == 'token':
                # Ensure RSP_ACCESS_TOKEN is available when using token auth
                if not os.environ.get("RSP_ACCESS_TOKEN"):
                    logger.warning("RSP_ACCESS_TOKEN environment variable not found, but token authentication is requested")
            result[k] = _expand_env_vars(v)
        return result
    elif isinstance(config_dict, list):
        return [_expand_env_vars(item) for item in config_dict]
    elif isinstance(config_dict, str):
        return os.path.expandvars(config_dict)
    else:
        return config_dict

def validate_rsp_configuration(config: RepoConfig) -> None:
    """
    Validate RSP-specific configuration requirements.

    Parameters
    ----------
    config : RepoConfig
        Configuration to validate

    Raises
    ------
    ValueError
        If RSP configuration is invalid
    """
    data_source = config.data_source

    if data_source.get('type') == 'butler_server':
        # Check required fields for RSP access
        if not data_source.get('server_url'):
            raise ValueError("server_url is required for butler_server data source")

        if data_source.get('auth_method') == 'token':
            if not os.environ.get("RSP_ACCESS_TOKEN"):
                raise ValueError("RSP_ACCESS_TOKEN environment variable is required for token authentication")

            if not data_source.get('collections'):
                raise ValueError("collections are required for Butler server access")

        logger.info("RSP configuration validated successfully")


def get_default_config() -> RepoConfig:
    """Get default configuration.

    The pipeline-stage configurations (data_source, ingestion, processing,
    model, output) are stored as flexible dicts to match the RepoConfig schema
    and the dict-based access used throughout the runtime (e.g.
    ``config.data_source.get('type')``). Only ``instrument`` and ``butler`` are
    strongly typed dataclasses.
    """
    return RepoConfig(
        instrument=InstrumentConfig(
            name='HSC',
            class_name='lsst.obs.subaru.HyperSuprimeCam',
            filters=['g', 'r', 'i', 'z', 'y']
        ),
        butler=ButlerConfig(
            standalone=False,
            override=False,
            registry_db='sqlite'
        ),
        data_source={
            'type': 'data_folder',
            'path': './data',
            'collections': ['raw/all', 'calib', 'refcats'],
            'create_if_missing': True
        },
        ingestion={
            'raw_data_pattern': '**/*.fits',
            'transfer_mode': 'symlink',
            'define_visits': True,
            'write_curated_calibrations': True,
            'skip_existing': True,
            'processes': 1
        },
        processing={
            'cutout_size': 64,
            'batch_size': 32,
            'max_workers': 4,
            'cache_size': 1000,
            'enable_performance_monitoring': True
        },
        model={},
        output={}
    )


def save_config(config: RepoConfig, output_path: Union[str, Path]) -> None:
    """
    Save configuration to YAML file.
    
    Parameters
    ----------
    config : RepoConfig
        Configuration to save
    output_path : str or Path
        Output file path
    """
    output_path = Path(output_path)

    # Convert to dictionary. instrument/butler are typed dataclasses; the
    # remaining pipeline-stage configurations are stored as flexible dicts
    # (matching RepoConfig.from_dict and the runtime's dict-based access).
    config_dict = {
        'instrument': asdict(config.instrument),
        'butler': asdict(config.butler),
        'data_source': dict(config.data_source),
        'ingestion': dict(config.ingestion),
        'processing': dict(config.processing),
        'model': dict(config.model),
        'output': dict(config.output),
    }

    # Remove None values
    config_dict = _remove_none_values(config_dict)
    
    # Save to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
    
    logger.info(f"Configuration saved to {output_path}")


def _remove_none_values(d: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively remove None values from dictionary."""
    if isinstance(d, dict):
        return {k: _remove_none_values(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [_remove_none_values(item) for item in d]
    else:
        return d