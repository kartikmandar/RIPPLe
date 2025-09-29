"""
Test suite for the repo_manager module.

This module tests the functionality of the repo_manager module including:
- Repository setup
- Repository path determination
- Data existence checking
- Manual data ingestion
- Data fetcher initialization
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path

# Import the modules to test
from ripple.butler_repo.repo_manager import ButlerRepoManager
from ripple.butler_repo.config_handler import get_default_config
from ripple.butler_repo.ingest_data import DataIngestor


class TestRepoManager(unittest.TestCase):
    """Test repository manager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = Path(self.temp_dir) / "test_repo"
        self.data_path = Path(self.temp_dir) / "test_data"
        
        # Create directories
        self.repo_path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)
        
        # Create butler.yaml to simulate existing repository
        (self.repo_path / "butler.yaml").touch()
        
        # Create test configuration
        self.test_config = get_default_config()
        self.test_config.data_source.path = str(self.data_path)
        
        # Create repository manager
        self.manager = ButlerRepoManager(self.test_config)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._determine_repository_path')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._create_and_setup_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._initialize_data_fetcher')
    def test_setup_repository_success(self, mock_init_fetcher, mock_create_setup, mock_determine_path):
        """Test successful repository setup."""
        # Mock method calls
        mock_determine_path.return_value = (self.repo_path, False)
        mock_create_setup.return_value = True
        
        success, result = self.manager.setup_repository()
        
        self.assertTrue(success)
        self.assertEqual(str(result), str(self.repo_path))
        self.assertEqual(self.manager.repo_path, self.repo_path)
        
        # Verify methods were called
        mock_determine_path.assert_called_once()
        mock_create_setup.assert_not_called()  # Not needed since repo already exists
        mock_init_fetcher.assert_called_once()
    
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._determine_repository_path')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._create_and_setup_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._initialize_data_fetcher')
    def test_setup_repository_create_new(self, mock_init_fetcher, mock_create_setup, mock_determine_path):
        """Test repository setup with new repository creation."""
        # Mock method calls
        mock_determine_path.return_value = (self.repo_path, True)
        mock_create_setup.return_value = True
        
        success, result = self.manager.setup_repository()
        
        self.assertTrue(success)
        self.assertEqual(str(result), str(self.repo_path))
        self.assertEqual(self.manager.repo_path, self.repo_path)
        
        # Verify methods were called
        mock_determine_path.assert_called_once()
        mock_create_setup.assert_called_once_with(self.repo_path)
        mock_init_fetcher.assert_called_once()
    
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._determine_repository_path')
    def test_setup_repository_creation_failure(self, mock_determine_path):
        """Test repository setup with repository creation failure."""
        # Mock method calls
        mock_determine_path.return_value = (self.repo_path, True)
        
        # Patch _create_and_setup_repository to return False
        with patch.object(self.manager, '_create_and_setup_repository', return_value=False):
            success, result = self.manager.setup_repository()
            
            self.assertFalse(success)
            self.assertEqual(result, "Failed to create repository")
    
    def test_determine_repository_path_butler_repo_exists(self):
        """Test determining repository path for existing butler repository."""
        self.test_config.data_source.type = 'butler_repo'
        self.test_config.data_source.create_if_missing = False
        # Fix: Set the path to point to where the butler.yaml file actually exists
        self.test_config.data_source.path = str(self.repo_path)
        
        repo_path, needs_creation = self.manager._determine_repository_path()
        
        self.assertEqual(repo_path, Path(self.test_config.data_source.path))
        self.assertFalse(needs_creation)
    
    def test_determine_repository_path_butler_repo_create_if_missing(self):
        """Test determining repository path for butler repository with create_if_missing."""
        self.test_config.data_source.type = 'butler_repo'
        self.test_config.data_source.path = str(self.repo_path / "new_repo")
        self.test_config.data_source.create_if_missing = True
        
        repo_path, needs_creation = self.manager._determine_repository_path()
        
        self.assertEqual(repo_path, Path(self.test_config.data_source.path))
        self.assertTrue(needs_creation)
    
    def test_determine_repository_path_butler_repo_not_found(self):
        """Test determining repository path for non-existent butler repository."""
        self.test_config.data_source.type = 'butler_repo'
        self.test_config.data_source.path = str(self.repo_path / "non_existent")
        self.test_config.data_source.create_if_missing = False
        
        with self.assertRaises(ValueError) as context:
            self.manager._determine_repository_path()
        
        self.assertIn("Butler repository not found", str(context.exception))
    
    def test_determine_repository_path_data_folder_exists(self):
        """Test determining repository path for existing data folder."""
        self.test_config.data_source.type = 'data_folder'
        
        # Create butler_repo subdirectory to simulate existing repository
        butler_repo_path = Path(self.test_config.data_source.path) / "butler_repo"
        butler_repo_path.mkdir(parents=True, exist_ok=True)
        (butler_repo_path / "butler.yaml").touch()
        
        repo_path, needs_creation = self.manager._determine_repository_path()
        
        self.assertEqual(repo_path, butler_repo_path)
        self.assertFalse(needs_creation)
    
    def test_determine_repository_path_data_folder_new(self):
        """Test determining repository path for new data folder."""
        self.test_config.data_source.type = 'data_folder'
        
        # Remove butler_repo subdirectory if it exists
        butler_repo_path = Path(self.test_config.data_source.path) / "butler_repo"
        if butler_repo_path.exists():
            shutil.rmtree(butler_repo_path)
        
        repo_path, needs_creation = self.manager._determine_repository_path()
        
        self.assertEqual(repo_path, butler_repo_path)
        self.assertTrue(needs_creation)
    
    def test_determine_repository_path_data_folder_not_found(self):
        """Test determining repository path for non-existent data folder."""
        self.test_config.data_source.type = 'data_folder'
        self.test_config.data_source.path = str(self.data_path / "non_existent")
        
        with self.assertRaises(ValueError) as context:
            self.manager._determine_repository_path()
        
        self.assertIn("Data folder not found", str(context.exception))
    
    def test_determine_repository_path_butler_server(self):
        """Test determining repository path for butler server."""
        self.test_config.data_source.type = 'butler_server'
        self.test_config.data_source.server_url = "http://example.com"
        
        repo_path, needs_creation = self.manager._determine_repository_path()
        
        self.assertIsNone(repo_path)
        self.assertFalse(needs_creation)
    
    def test_determine_repository_path_unknown_type(self):
        """Test determining repository path with unknown data source type."""
        self.test_config.data_source.type = 'unknown_type'
        
        with self.assertRaises(ValueError) as context:
            self.manager._determine_repository_path()
        
        self.assertIn("Unknown data source type", str(context.exception))
    
    @patch('subprocess.run')
    def test_check_data_exists_success(self, mock_subprocess):
        """Test checking if data exists successfully."""
        # Mock successful subprocess call with data collections
        mock_subprocess.return_value = MagicMock(
            returncode=0, 
            stdout="instrument/HSC\nHSC/raw/all\nHSC/calib\nrefcats", 
            stderr=""
        )
        
        result = self.manager._check_data_exists(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "query-collections")
        self.assertIn(str(self.repo_path), args[0])
    
    @patch('subprocess.run')
    def test_check_data_exists_no_data(self, mock_subprocess):
        """Test checking if data exists when no data is present."""
        # Mock successful subprocess call with no data collections
        mock_subprocess.return_value = MagicMock(
            returncode=0, 
            stdout="instrument/HSC", 
            stderr=""
        )
        
        result = self.manager._check_data_exists(self.repo_path)
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_check_data_exists_failure(self, mock_subprocess):
        """Test checking if data exists when command fails."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(
            returncode=1, 
            stdout="", 
            stderr="Command failed"
        )
        
        result = self.manager._check_data_exists(self.repo_path)
        
        self.assertFalse(result)
    
    @patch('ripple.butler_repo.create_repo.initialize_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._check_data_exists')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._manual_data_ingestion')
    def test_create_and_setup_repository_data_folder_with_export(self, mock_manual, mock_check_data, mock_init):
        """Test creating and setting up repository with data folder and export file."""
        # Set up data folder configuration
        self.test_config.data_source.type = 'data_folder'
        
        # Create export file
        export_file = Path(self.test_config.data_source.path) / "export.yaml"
        export_file.touch()
        
        # Mock method calls
        mock_init.return_value = True
        mock_check_data.return_value = False  # No data exists yet
        
        result = self.manager._create_and_setup_repository(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify methods were called
        mock_init.assert_called_once_with(self.test_config, str(self.repo_path))
        mock_check_data.assert_called_once_with(self.repo_path)
        mock_manual.assert_not_called()  # Manual ingestion not needed with export file
    
    @patch('ripple.butler_repo.create_repo.initialize_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._check_data_exists')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._manual_data_ingestion')
    def test_create_and_setup_repository_data_folder_with_existing_data(self, mock_manual, mock_check_data, mock_init):
        """Test creating and setting up repository with existing data."""
        # Set up data folder configuration
        self.test_config.data_source.type = 'data_folder'
        
        # Create export file
        export_file = Path(self.test_config.data_source.path) / "export.yaml"
        export_file.touch()
        
        # Mock method calls
        mock_init.return_value = True
        mock_check_data.return_value = True  # Data already exists
        
        result = self.manager._create_and_setup_repository(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify methods were called
        mock_init.assert_called_once_with(self.test_config, str(self.repo_path))
        mock_check_data.assert_called_once_with(self.repo_path)
        mock_manual.assert_not_called()  # Manual ingestion not needed with existing data
    
    @patch('ripple.butler_repo.create_repo.initialize_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._check_data_exists')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._manual_data_ingestion')
    @patch.object(DataIngestor, 'import_from_export')
    def test_create_and_setup_repository_data_folder_export_import_failure(self, mock_import, mock_manual, mock_check_data, mock_init):
        """Test creating and setting up repository with export file import failure."""
        # Set up data folder configuration
        self.test_config.data_source.type = 'data_folder'
        
        # Create export file
        export_file = Path(self.test_config.data_source.path) / "export.yaml"
        export_file.touch()
        
        # Mock method calls
        mock_init.return_value = True
        mock_check_data.return_value = False  # No data exists yet
        mock_import.return_value = False  # Import fails
        mock_manual.return_value = True  # Manual ingestion succeeds
        
        result = self.manager._create_and_setup_repository(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify methods were called
        mock_init.assert_called_once_with(self.test_config, str(self.repo_path))
        mock_check_data.assert_called_once_with(self.repo_path)
        mock_manual.assert_called_once_with(self.repo_path)
    
    @patch('ripple.butler_repo.create_repo.initialize_repository')
    @patch('ripple.butler_repo.repo_manager.ButlerRepoManager._manual_data_ingestion')
    def test_create_and_setup_repository_data_folder_no_export(self, mock_manual, mock_init):
        """Test creating and setting up repository with data folder but no export file."""
        # Set up data folder configuration
        self.test_config.data_source.type = 'data_folder'
        
        # Don't create export file
        
        # Mock method calls
        mock_init.return_value = True
        mock_manual.return_value = True
        
        result = self.manager._create_and_setup_repository(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify methods were called
        mock_init.assert_called_once_with(self.test_config, str(self.repo_path))
        mock_manual.assert_called_once_with(self.repo_path)
    
    @patch('ripple.butler_repo.create_repo.initialize_repository')
    def test_create_and_setup_repository_initialization_failure(self, mock_init):
        """Test creating and setting up repository with initialization failure."""
        # Mock method calls
        mock_init.return_value = False  # Initialization fails
        
        result = self.manager._create_and_setup_repository(self.repo_path)
        
        self.assertFalse(result)
        
        # Verify methods were called
        mock_init.assert_called_once_with(self.test_config, str(self.repo_path))
    
    @patch.object(DataIngestor, 'ingest_all')
    def test_manual_data_ingestion_success(self, mock_ingest_all):
        """Test successful manual data ingestion."""
        # Mock ingestion results
        mock_ingest_all.return_value = {
            "raw_data": {"success": True, "count": 2, "errors": []},
            "calibrations": {"success": True, "count": 3, "errors": []},
            "reference_catalogs": {"success": False, "count": 0, "errors": []},
            "visits_defined": True
        }
        
        result = self.manager._manual_data_ingestion(self.repo_path)
        
        self.assertTrue(result)
        
        # Verify ingest_all was called
        mock_ingest_all.assert_called_once()
    
    @patch.object(DataIngestor, 'ingest_all')
    def test_manual_data_ingestion_partial_success(self, mock_ingest_all):
        """Test manual data ingestion with partial success."""
        # Mock ingestion results
        mock_ingest_all.return_value = {
            "raw_data": {"success": False, "count": 0, "errors": []},
            "calibrations": {"success": True, "count": 3, "errors": []},
            "reference_catalogs": {"success": False, "count": 0, "errors": []},
            "visits_defined": False
        }
        
        result = self.manager._manual_data_ingestion(self.repo_path)
        
        self.assertTrue(result)  # Should still be True due to calibrations success
        
        # Verify ingest_all was called
        mock_ingest_all.assert_called_once()
    
    @patch.object(DataIngestor, 'ingest_all')
    def test_manual_data_ingestion_failure(self, mock_ingest_all):
        """Test manual data ingestion failure."""
        # Mock ingestion results
        mock_ingest_all.return_value = {
            "raw_data": {"success": False, "count": 0, "errors": []},
            "calibrations": {"success": False, "count": 0, "errors": []},
            "reference_catalogs": {"success": False, "count": 0, "errors": []},
            "visits_defined": False
        }
        
        result = self.manager._manual_data_ingestion(self.repo_path)
        
        self.assertFalse(result)
        
        # Verify ingest_all was called
        mock_ingest_all.assert_called_once()
    
    @patch('ripple.butler_repo.repo_manager.LsstDataFetcher')
    def test_initialize_data_fetcher_success(self, mock_data_fetcher):
        """Test successful data fetcher initialization."""
        # Mock data fetcher class
        mock_fetcher_instance = MagicMock()
        mock_data_fetcher.return_value = mock_fetcher_instance
        
        # Set repo_path
        self.manager.repo_path = self.repo_path
        
        result = self.manager._initialize_data_fetcher()
        
        self.assertIsNone(result)  # Method returns None
        self.assertEqual(self.manager.data_fetcher, mock_fetcher_instance)
        
        # Verify data fetcher was created with correct config
        mock_data_fetcher.assert_called_once()
        args, kwargs = mock_data_fetcher.call_args
        config = args[0]
        self.assertEqual(config.repo_path, str(self.repo_path))
        self.assertIn(f"{self.test_config.instrument.name}/defaults", config.collections)
    
    def test_initialize_data_fetcher_no_repo_path(self):
        """Test data fetcher initialization with no repo path."""
        # Don't set repo_path
        self.manager.repo_path = None
        
        result = self.manager._initialize_data_fetcher()
        
        self.assertIsNone(result)  # Method returns None
        self.assertIsNone(self.manager.data_fetcher)
    
    @patch('ripple.butler_repo.repo_manager.LsstDataFetcher')
    def test_initialize_data_fetcher_exception(self, mock_data_fetcher):
        """Test data fetcher initialization with exception."""
        # Mock data fetcher class to raise exception
        mock_data_fetcher.side_effect = Exception("Test exception")
        
        # Set repo_path
        self.manager.repo_path = self.repo_path
        
        result = self.manager._initialize_data_fetcher()
        
        self.assertIsNone(result)  # Method returns None
        self.assertIsNone(self.manager.data_fetcher)
    
    def test_get_data_fetcher(self):
        """Test getting data fetcher instance."""
        # Set mock data fetcher
        mock_fetcher = MagicMock()
        self.manager.data_fetcher = mock_fetcher
        
        result = self.manager.get_data_fetcher()
        
        self.assertEqual(result, mock_fetcher)
    
    def test_log_ingestion_summary(self):
        """Test logging ingestion summary."""
        # Mock ingestion results
        results = {
            "raw_data": {"success": True, "count": 2, "errors": []},
            "calibrations": {"success": True, "count": 3, "errors": ["Warning 1"]},
            "reference_catalogs": {"success": False, "count": 0, "errors": ["Error 1", "Error 2"]},
            "visits_defined": True
        }
        
        # Mock logger
        with patch('ripple.butler_repo.repo_manager.logger') as mock_logger:
            self.manager._log_ingestion_summary(results)
            
            # Verify logger was called
            self.assertTrue(mock_logger.info.called)
            self.assertTrue(mock_logger.warning.called)
    
    def test_manual_data_ingestion_with_hsc_data_structure(self):
        """Test manual data ingestion with HSC data structure."""
        # Create HSC data structure
        hsc_data_path = Path(self.test_config.data_source.path) / "HSC"
        hsc_data_path.mkdir(parents=True, exist_ok=True)
        (hsc_data_path / "raw").mkdir(parents=True, exist_ok=True)
        (hsc_data_path / "calib").mkdir(parents=True, exist_ok=True)
        
        # Create test files
        (hsc_data_path / "raw" / "file1.fits").touch()
        (hsc_data_path / "raw" / "file2.fits").touch()
        (hsc_data_path / "calib" / "bias.fits").touch()
        
        # Mock ingest_all to return success
        with patch.object(DataIngestor, 'ingest_all') as mock_ingest_all:
            mock_ingest_all.return_value = {
                "raw_data": {"success": True, "count": 2, "errors": []},
                "calibrations": {"success": True, "count": 1, "errors": []},
                "reference_catalogs": {"success": False, "count": 0, "errors": []},
                "visits_defined": True
            }
            
            result = self.manager._manual_data_ingestion(self.repo_path)
            
            self.assertTrue(result)
            
            # Verify config was updated with HSC paths
            self.assertEqual(self.test_config.ingestion.raw_data_pattern, "HSC/raw/**/*.fits")
            self.assertEqual(self.test_config.ingestion.calibration_path, str(hsc_data_path / "calib"))
    
    def test_manual_data_ingestion_with_refcats(self):
        """Test manual data ingestion with refcats."""
        # Create refcats directory
        refcats_path = Path(self.test_config.data_source.path) / "refcats"
        refcats_path.mkdir(parents=True, exist_ok=True)
        
        # Create test catalog file
        (refcats_path / "gaia_dr2_20200414.fits").touch()
        
        # Mock ingest_all to return success
        with patch.object(DataIngestor, 'ingest_all') as mock_ingest_all:
            mock_ingest_all.return_value = {
                "raw_data": {"success": True, "count": 0, "errors": []},
                "calibrations": {"success": True, "count": 0, "errors": []},
                "reference_catalogs": {"success": True, "count": 1, "errors": []},
                "visits_defined": True
            }
            
            result = self.manager._manual_data_ingestion(self.repo_path)
            
            self.assertTrue(result)
            
            # Verify config was updated with refcats path
            self.assertEqual(self.test_config.ingestion.reference_catalog_path, str(refcats_path))


if __name__ == '__main__':
    unittest.main()