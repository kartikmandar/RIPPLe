"""
Test suite for the create_repo module.

This module tests the functionality of the create_repo module including:
- Repository initialization
- Repository verification
- Repository information retrieval
- Repository emptiness check
"""

import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
import shutil
from pathlib import Path

# Import the modules to test
from ripple.butler_repo.create_repo import (
    initialize_repository, verify_repository, 
    get_repository_info, is_repository_empty
)
from ripple.butler_repo.config_handler import get_default_config


class TestCreateRepo(unittest.TestCase):
    """Test repository creation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = Path(self.temp_dir) / "test_repo"
        
        # Create a default test configuration
        self.test_config = get_default_config()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('subprocess.run')
    def test_initialize_repository_success(self, mock_subprocess):
        """Test successful repository initialization."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Repository created", stderr="")
        
        result = initialize_repository(self.test_config, str(self.repo_path))
        
        self.assertTrue(result)
        self.assertTrue(self.repo_path.parent.exists())
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "create")
        self.assertIn(str(self.repo_path), args[0])
    
    @patch('subprocess.run')
    def test_initialize_repository_already_exists(self, mock_subprocess):
        """Test repository initialization when repository already exists."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        result = initialize_repository(self.test_config, str(self.repo_path))
        
        self.assertTrue(result)
        # Subprocess should not be called
        mock_subprocess.assert_not_called()
    
    @patch('subprocess.run')
    def test_initialize_repository_failure(self, mock_subprocess):
        """Test repository initialization failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Creation failed")
        
        result = initialize_repository(self.test_config, str(self.repo_path))
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_verify_repository_success(self, mock_subprocess):
        """Test successful repository verification."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock successful subprocess calls
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Repository info", stderr=""),  # butler info
            MagicMock(returncode=0, stdout="raw/all\ncalib\nrefcats", stderr=""),  # query-collections
            MagicMock(returncode=0, stdout="Dataset1\nDataset2", stderr="")  # query-datasets
        ]
        
        result = verify_repository(str(self.repo_path))
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertIn('raw_output', result['info'])
        self.assertIn('collections', result['info'])
        self.assertEqual(result['info']['collection_count'], 3)
        # The implementation subtracts 1 for the header row
        self.assertEqual(result["info"]["dataset_count"], 1)
    
    def test_verify_repository_path_not_exists(self):
        """Test repository verification with non-existent path."""
        result = verify_repository(str(self.repo_path))
        
        self.assertFalse(result['valid'])
        # Check if any error message contains the expected text
        self.assertTrue(any("Repository path does not exist" in error for error in result['errors']))
    
    def test_verify_repository_missing_butler_yaml(self):
        """Test repository verification with missing butler.yaml."""
        self.repo_path.mkdir(parents=True, exist_ok=True)
        
        result = verify_repository(str(self.repo_path))
        
        self.assertFalse(result['valid'])
        # Check if any error message contains the expected text
        self.assertTrue(any("butler.yaml not found" in error for error in result['errors']))
    
    @patch('subprocess.run')
    def test_verify_repository_info_failure(self, mock_subprocess):
        """Test repository verification with info command failure."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock failed info call but successful collections call
        mock_subprocess.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="Info failed"),  # butler info
            MagicMock(returncode=0, stdout="raw/all\ncalib", stderr="")  # query-collections
        ]
        
        result = verify_repository(str(self.repo_path))
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertTrue(any("Could not get repository info" in warning for warning in result['warnings']))
        self.assertIn('collections', result['info'])
    
    @patch('subprocess.run')
    def test_verify_repository_collections_failure(self, mock_subprocess):
        """Test repository verification with collections command failure."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock successful info call but failed collections call
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Repository info", stderr=""),  # butler info
            MagicMock(returncode=1, stdout="", stderr="Collections failed"),  # query-collections
        ]
        
        result = verify_repository(str(self.repo_path))
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertTrue(any("Could not query collections" in warning for warning in result['warnings']))
        self.assertIn('raw_output', result['info'])
    
    @patch('subprocess.run')
    def test_verify_repository_datasets_failure(self, mock_subprocess):
        """Test repository verification with datasets command failure."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock successful info and collections calls but failed datasets call
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Repository info", stderr=""),  # butler info
            MagicMock(returncode=0, stdout="raw/all\ncalib", stderr=""),  # query-collections
            MagicMock(returncode=1, stdout="", stderr="Datasets failed")  # query-datasets
        ]
        
        result = verify_repository(str(self.repo_path))
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
        # Check if any warning message contains the expected text
        self.assertTrue(any("Could not query datasets" in warning for warning in result['warnings']))
        self.assertIn('raw_output', result['info'])
        self.assertIn('collections', result['info'])
    
    @patch('subprocess.run')
    def test_get_repository_info_success(self, mock_subprocess):
        """Test successful repository info retrieval."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock successful subprocess calls
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Key1: Value1\nKey2: Value2", stderr=""),  # butler info
            MagicMock(returncode=0, stdout="raw/all\ncalib", stderr="")  # query-collections
        ]
        
        result = get_repository_info(str(self.repo_path))
        
        self.assertIsNotNone(result)
        self.assertEqual(result['key1'], 'Value1')
        self.assertEqual(result['key2'], 'Value2')
        self.assertEqual(result['collections'], ['raw/all', 'calib'])
    
    def test_get_repository_info_missing_butler_yaml(self):
        """Test repository info retrieval with missing butler.yaml."""
        result = get_repository_info(str(self.repo_path))
        
        self.assertIsNone(result)
    
    @patch('subprocess.run')
    def test_get_repository_info_failure(self, mock_subprocess):
        """Test repository info retrieval failure."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock failed subprocess call
        mock_subprocess.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="Info failed"),  # butler info
        ]
        
        result = get_repository_info(str(self.repo_path))
        
        self.assertIsNone(result)
    
    @patch('subprocess.run')
    def test_get_repository_info_collections_failure(self, mock_subprocess):
        """Test repository info retrieval with collections command failure."""
        # Create butler.yaml to simulate existing repository
        self.repo_path.mkdir(parents=True, exist_ok=True)
        (self.repo_path / "butler.yaml").touch()
        
        # Mock successful info call but failed collections call
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Key1: Value1\nKey2: Value2", stderr=""),  # butler info
            MagicMock(returncode=1, stdout="", stderr="Collections failed")  # query-collections
        ]
        
        result = get_repository_info(str(self.repo_path))
        
        self.assertIsNotNone(result)
        self.assertEqual(result['key1'], 'Value1')
        self.assertEqual(result['key2'], 'Value2')
        # collections should not be present
        self.assertNotIn('collections', result)
    
    @patch('ripple.butler_repo.create_repo.verify_repository')
    def test_is_repository_empty_success(self, mock_verify):
        """Test checking if repository is empty."""
        # Mock verification result with no data collections
        mock_verify.return_value = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': {
                'collections': ['instrument/HSC', 'instrument/HSC/calib']
            }
        }
        
        result = is_repository_empty(str(self.repo_path))
        
        self.assertTrue(result)
    
    @patch('ripple.butler_repo.create_repo.verify_repository')
    def test_is_repository_empty_with_data(self, mock_verify):
        """Test checking if repository is empty when it contains data."""
        # Mock verification result with data collections
        mock_verify.return_value = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': {
                'collections': ['instrument/HSC', 'HSC/raw/all', 'HSC/calib']
            }
        }
        
        result = is_repository_empty(str(self.repo_path))
        
        self.assertFalse(result)
    
    @patch('ripple.butler_repo.create_repo.verify_repository')
    def test_is_repository_empty_with_refcats(self, mock_verify):
        """Test checking if repository is empty when it contains refcats."""
        # Mock verification result with refcats collections
        mock_verify.return_value = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'info': {
                'collections': ['instrument/HSC', 'refcats/gaia_dr2']
            }
        }
        
        result = is_repository_empty(str(self.repo_path))
        
        self.assertFalse(result)
    
    @patch('ripple.butler_repo.create_repo.verify_repository')
    def test_is_repository_empty_invalid_repo(self, mock_verify):
        """Test checking if repository is empty when repository is invalid."""
        # Mock verification result with invalid repository
        mock_verify.return_value = {
            'valid': False,
            'errors': ['Repository path does not exist'],
            'warnings': [],
            'info': {}
        }
        
        result = is_repository_empty(str(self.repo_path))
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_initialize_repository_with_postgres(self, mock_subprocess):
        """Test repository initialization with PostgreSQL."""
        # Configure for PostgreSQL
        self.test_config.butler.registry_db = 'postgresql'
        self.test_config.butler.postgres_url = 'postgresql://user:pass@localhost/db'
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Repository created", stderr="")
        
        result = initialize_repository(self.test_config, str(self.repo_path))
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "create")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn('--postgres', args[0])
        self.assertIn(self.test_config.butler.postgres_url, args[0])
    
    @patch('subprocess.run')
    def test_initialize_repository_with_override(self, mock_subprocess):
        """Test repository initialization with override."""
        # Configure for override
        self.test_config.butler.override = True
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Repository created", stderr="")
        
        result = initialize_repository(self.test_config, str(self.repo_path))
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "create")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn('--override', args[0])


if __name__ == '__main__':
    unittest.main()