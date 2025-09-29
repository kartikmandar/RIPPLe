"""
Test suite for the config_handler module.

This module tests the functionality of the config_handler module including:
- Configuration loading
- Configuration validation
- Default configuration
- Configuration saving
"""

import unittest
from unittest.mock import patch, mock_open
import tempfile
import os
import shutil
from pathlib import Path
import yaml

# Import the modules to test
from ripple.butler_repo.config_handler import (
    DataSourceConfig, InstrumentConfig, IngestionConfig, 
    ButlerConfig, ProcessingConfig, RepoConfig,
    load_config, validate_config, get_default_config, save_config
)


class TestConfigHandler(unittest.TestCase):
    """Test configuration handling functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir) / "test_config.yaml"
        
        # Create a default test configuration
        self.test_config_dict = {
            'data_source': {
                'type': 'data_folder',
                'path': './test_data',
                'collections': ['raw/all', 'calib', 'refcats'],
                'create_if_missing': True
            },
            'instrument': {
                'name': 'HSC',
                'class_name': 'lsst.obs.subaru.HyperSuprimeCam',
                'filters': ['g', 'r', 'i', 'z', 'y']
            },
            'ingestion': {
                'raw_data_pattern': '**/*.fits',
                'transfer_mode': 'symlink',
                'define_visits': True,
                'write_curated_calibrations': True,
                'skip_existing': True,
                'processes': 1
            },
            'butler': {
                'standalone': False,
                'override': False,
                'registry_db': 'sqlite'
            },
            'processing': {
                'cutout_size': 64,
                'batch_size': 32,
                'max_workers': 4,
                'cache_size': 1000,
                'enable_performance_monitoring': True
            }
        }
        
        # Write test configuration to file
        with open(self.config_path, 'w') as f:
            yaml.dump(self.test_config_dict, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_load_config_success(self):
        """Test successful configuration loading."""
        config = load_config(self.config_path)
        
        self.assertIsInstance(config, RepoConfig)
        self.assertEqual(config.data_source.type, 'data_folder')
        self.assertEqual(config.instrument.name, 'HSC')
        self.assertEqual(config.ingestion.transfer_mode, 'symlink')
        self.assertEqual(config.butler.registry_db, 'sqlite')
        self.assertEqual(config.processing.cutout_size, 64)
    
    def test_load_config_file_not_found(self):
        """Test loading configuration with non-existent file."""
        non_existent_path = Path(self.temp_dir) / "non_existent.yaml"
        
        with self.assertRaises(FileNotFoundError):
            load_config(non_existent_path)
    
    def test_load_config_invalid_yaml(self):
        """Test loading configuration with invalid YAML."""
        invalid_yaml_path = Path(self.temp_dir) / "invalid.yaml"
        with open(invalid_yaml_path, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        with self.assertRaises(ValueError):
            load_config(invalid_yaml_path)
    
    def test_validate_config_success(self):
        """Test successful configuration validation."""
        config = load_config(self.config_path)
        # Should not raise any exception
        validate_config(config)
    
    def test_validate_config_invalid_data_source_type(self):
        """Test validation with invalid data source type."""
        config = load_config(self.config_path)
        config.data_source.type = 'invalid_type'
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Invalid data source type", str(context.exception))
    
    def test_validate_config_missing_butler_repo_path(self):
        """Test validation with missing butler repository path."""
        config = load_config(self.config_path)
        config.data_source.type = 'butler_repo'
        config.data_source.path = None
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Butler repository path required", str(context.exception))
    
    def test_validate_config_missing_butler_server_url(self):
        """Test validation with missing butler server URL."""
        config = load_config(self.config_path)
        config.data_source.type = 'butler_server'
        config.data_source.server_url = None
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Server URL required", str(context.exception))
    
    def test_validate_config_missing_instrument_name(self):
        """Test validation with missing instrument name."""
        config = load_config(self.config_path)
        config.instrument.name = ''
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Instrument name is required", str(context.exception))
    
    def test_validate_config_invalid_transfer_mode(self):
        """Test validation with invalid transfer mode."""
        config = load_config(self.config_path)
        config.ingestion.transfer_mode = 'invalid_mode'
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Invalid transfer mode", str(context.exception))
    
    def test_validate_config_invalid_registry_db(self):
        """Test validation with invalid registry database."""
        config = load_config(self.config_path)
        config.butler.registry_db = 'invalid_db'
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("Invalid registry database", str(context.exception))
    
    def test_validate_config_missing_postgres_url(self):
        """Test validation with missing PostgreSQL URL."""
        config = load_config(self.config_path)
        config.butler.registry_db = 'postgresql'
        config.butler.postgres_url = None
        
        with self.assertRaises(ValueError) as context:
            validate_config(config)
        
        self.assertIn("PostgreSQL URL required", str(context.exception))
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        config = get_default_config()
        
        self.assertIsInstance(config, RepoConfig)
        self.assertEqual(config.data_source.type, 'data_folder')
        self.assertEqual(config.instrument.name, 'HSC')
        self.assertEqual(config.ingestion.transfer_mode, 'symlink')
        self.assertEqual(config.butler.registry_db, 'sqlite')
        self.assertEqual(config.processing.cutout_size, 64)
    
    def test_save_config(self):
        """Test saving configuration to file."""
        config = get_default_config()
        output_path = Path(self.temp_dir) / "saved_config.yaml"
        
        save_config(config, output_path)
        
        self.assertTrue(output_path.exists())
        
        # Load and verify the saved configuration
        loaded_config = load_config(output_path)
        self.assertEqual(loaded_config.data_source.type, config.data_source.type)
        self.assertEqual(loaded_config.instrument.name, config.instrument.name)
        self.assertEqual(loaded_config.ingestion.transfer_mode, config.ingestion.transfer_mode)
    
    def test_config_from_dict(self):
        """Test creating configuration from dictionary."""
        config = RepoConfig.from_dict(self.test_config_dict)
        
        self.assertEqual(config.data_source.type, 'data_folder')
        self.assertEqual(config.instrument.name, 'HSC')
        self.assertEqual(config.ingestion.transfer_mode, 'symlink')
        self.assertEqual(config.butler.registry_db, 'sqlite')
        self.assertEqual(config.processing.cutout_size, 64)
    
    def test_data_source_config_creation(self):
        """Test DataSourceConfig creation."""
        config = DataSourceConfig(
            type='data_folder',
            path='./test_data',
            collections=['raw/all', 'calib'],
            create_if_missing=True
        )
        
        self.assertEqual(config.type, 'data_folder')
        self.assertEqual(config.path, './test_data')
        self.assertEqual(config.collections, ['raw/all', 'calib'])
        self.assertTrue(config.create_if_missing)
    
    def test_instrument_config_creation(self):
        """Test InstrumentConfig creation."""
        config = InstrumentConfig(
            name='HSC',
            class_name='lsst.obs.subaru.HyperSuprimeCam',
            filters=['g', 'r', 'i', 'z', 'y']
        )
        
        self.assertEqual(config.name, 'HSC')
        self.assertEqual(config.class_name, 'lsst.obs.subaru.HyperSuprimeCam')
        self.assertEqual(config.filters, ['g', 'r', 'i', 'z', 'y'])
    
    def test_ingestion_config_creation(self):
        """Test IngestionConfig creation."""
        config = IngestionConfig(
            raw_data_pattern='**/*.fits',
            transfer_mode='symlink',
            define_visits=True,
            write_curated_calibrations=True,
            skip_existing=True,
            processes=1
        )
        
        self.assertEqual(config.raw_data_pattern, '**/*.fits')
        self.assertEqual(config.transfer_mode, 'symlink')
        self.assertTrue(config.define_visits)
        self.assertTrue(config.write_curated_calibrations)
        self.assertTrue(config.skip_existing)
        self.assertEqual(config.processes, 1)
    
    def test_butler_config_creation(self):
        """Test ButlerConfig creation."""
        config = ButlerConfig(
            standalone=False,
            override=False,
            registry_db='sqlite'
        )
        
        self.assertFalse(config.standalone)
        self.assertFalse(config.override)
        self.assertEqual(config.registry_db, 'sqlite')
    
    def test_processing_config_creation(self):
        """Test ProcessingConfig creation."""
        config = ProcessingConfig(
            cutout_size=64,
            batch_size=32,
            max_workers=4,
            cache_size=1000,
            enable_performance_monitoring=True
        )
        
        self.assertEqual(config.cutout_size, 64)
        self.assertEqual(config.batch_size, 32)
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.cache_size, 1000)
        self.assertTrue(config.enable_performance_monitoring)
    
    def test_expand_env_vars(self):
        """Test environment variable expansion in configuration."""
        # Set environment variable
        os.environ['TEST_DATA_PATH'] = '/path/to/test/data'
        
        # Create config with environment variable
        config_dict = {
            'data_source': {
                'type': 'data_folder',
                'path': '${TEST_DATA_PATH}',
                'collections': ['raw/all', 'calib'],
                'create_if_missing': True
            },
            'instrument': {
                'name': 'HSC',
                'class_name': 'lsst.obs.subaru.HyperSuprimeCam'
            }
        }
        
        # Import the _expand_env_vars function to test environment variable expansion
        from ripple.butler_repo.config_handler import _expand_env_vars
        
        # Expand environment variables before creating config
        expanded_config_dict = _expand_env_vars(config_dict)
        config = RepoConfig.from_dict(expanded_config_dict)
        
        # Environment variable should be expanded
        self.assertEqual(config.data_source.path, '/path/to/test/data')
        
        # Clean up
        del os.environ['TEST_DATA_PATH']


if __name__ == '__main__':
    unittest.main()