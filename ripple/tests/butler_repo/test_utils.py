"""
Test suite for the utils module.

This module tests the functionality of the utils module including:
- LSST environment checking
- Data file finding
- Instrument detection from FITS
- Instrument information retrieval
- File size formatting
- Repository size estimation
- Butler command validation
- Butler version retrieval
"""

import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path

# Import the modules to test
from ripple.butler_repo.utils import (
    check_lsst_environment, find_data_files, 
    detect_instrument_from_fits, get_instrument_info,
    format_size, estimate_repository_size, 
    validate_butler_command, get_butler_version
)


class TestUtils(unittest.TestCase):
    """Test utility functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('subprocess.run')
    def test_check_lsst_environment_success(self, mock_subprocess):
        """Test successful LSST environment check."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(
            returncode=0, 
            stdout="lsst_distrib 12345", 
            stderr=""
        )
        
        result = check_lsst_environment()
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "eups")
        self.assertEqual(args[0][1], "list")
        self.assertEqual(args[0][2], "lsst_distrib")
    
    @patch('subprocess.run')
    def test_check_lsst_environment_failure(self, mock_subprocess):
        """Test LSST environment check failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(
            returncode=1, 
            stdout="", 
            stderr="Package not found"
        )
        
        result = check_lsst_environment()
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_check_lsst_environment_file_not_found(self, mock_subprocess):
        """Test LSST environment check with eups command not found."""
        # Mock FileNotFoundError
        mock_subprocess.side_effect = FileNotFoundError()
        
        result = check_lsst_environment()
        
        self.assertFalse(result)
    
    def test_find_data_files(self):
        """Test finding data files."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        (test_dir / "file1.fits").touch()
        (test_dir / "file2.fits").touch()
        (test_dir / "file3.txt").touch()
        
        # Find FITS files
        files = find_data_files(test_dir, ["*.fits"])
        
        self.assertEqual(len(files), 2)
        self.assertTrue(any(f.name == "file1.fits" for f in files))
        self.assertTrue(any(f.name == "file2.fits" for f in files))
    
    def test_find_data_files_with_max(self):
        """Test finding data files with maximum file limit."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        (test_dir / "file1.fits").touch()
        (test_dir / "file2.fits").touch()
        (test_dir / "file3.fits").touch()
        
        # Find FITS files with max limit
        files = find_data_files(test_dir, ["*.fits"], max_files=2)
        
        self.assertEqual(len(files), 2)
    
    def test_find_data_files_multiple_patterns(self):
        """Test finding data files with multiple patterns."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        (test_dir / "file1.fits").touch()
        (test_dir / "file2.fits").touch()
        (test_dir / "file3.fz").touch()
        (test_dir / "file4.txt").touch()
        
        # Find FITS and FZ files
        files = find_data_files(test_dir, ["*.fits", "*.fz"])
        
        self.assertEqual(len(files), 3)
        self.assertTrue(any(f.name == "file1.fits" for f in files))
        self.assertTrue(any(f.name == "file2.fits" for f in files))
        self.assertTrue(any(f.name == "file3.fz" for f in files))
    
    def test_find_data_files_recursive(self):
        """Test finding data files recursively."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        subdir = test_dir / "subdir"
        subdir.mkdir(parents=True, exist_ok=True)
        
        (test_dir / "file1.fits").touch()
        (subdir / "file2.fits").touch()
        
        # Find FITS files recursively
        files = find_data_files(test_dir, ["**/*.fits"])
        
        self.assertEqual(len(files), 2)
        self.assertTrue(any(f.name == "file1.fits" for f in files))
        self.assertTrue(any(f.name == "file2.fits" for f in files))
    
    @patch('lsst.afw.fits.readMetadata')
    def test_detect_instrument_from_fits_lsst_success(self, mock_read_metadata):
        """Test successful instrument detection from FITS using LSST tools."""
        # Create test file
        test_file = Path(self.temp_dir) / "test.fits"
        test_file.touch()
        
        # Mock metadata
        mock_metadata = MagicMock()
        mock_metadata.__getitem__ = lambda self, key: "HSC" if key == "INSTRUME" else None
        mock_read_metadata.return_value = mock_metadata
        
        result = detect_instrument_from_fits(test_file)
        
        self.assertEqual(result, "HSC")
        
        # Verify readMetadata was called
        mock_read_metadata.assert_called_once_with(str(test_file))
    
    @patch('lsst.afw.fits.readMetadata')
    @patch('astropy.io.fits.open')
    def test_detect_instrument_from_fits_astropy_fallback(self, mock_fits_open, mock_read_metadata):
        """Test instrument detection from FITS using astropy fallback."""
        # Create test file
        test_file = Path(self.temp_dir) / "test.fits"
        test_file.touch()
        
        # Mock LSST readMetadata to raise exception
        mock_read_metadata.side_effect = Exception("LSST not available")
        
        # Mock astropy fits
        mock_header = MagicMock()
        mock_header.__getitem__ = lambda self, key: "LSST" if key == "INSTRUME" else None
        mock_hdul = MagicMock()
        mock_hdul.__enter__ = MagicMock(return_value=mock_hdul)
        mock_hdul.__exit__ = MagicMock(return_value=None)
        mock_hdul.__getitem__ = MagicMock(return_value=mock_header)
        mock_fits_open.return_value = mock_hdul
        
        result = detect_instrument_from_fits(test_file)
        
        self.assertEqual(result, "LSSTCam")
        
        # Verify both methods were called
        mock_read_metadata.assert_called_once_with(str(test_file))
        mock_fits_open.assert_called_once_with(test_file)
    
    @patch('lsst.afw.fits.readMetadata')
    @patch('astropy.io.fits.open')
    def test_detect_instrument_from_fits_no_detection(self, mock_fits_open, mock_read_metadata):
        """Test instrument detection from FITS with no detection."""
        # Create test file
        test_file = Path(self.temp_dir) / "test.fits"
        test_file.touch()
        
        # Mock both methods to raise exceptions
        mock_read_metadata.side_effect = Exception("LSST not available")
        mock_fits_open.side_effect = Exception("Astropy not available")
        
        result = detect_instrument_from_fits(test_file)
        
        self.assertIsNone(result)
    
    def test_get_instrument_info_hsc(self):
        """Test getting HSC instrument information."""
        info = get_instrument_info("HSC")
        
        self.assertEqual(info["class_name"], "lsst.obs.subaru.HyperSuprimeCam")
        self.assertEqual(info["filters"], ["g", "r", "i", "z", "y", "NB0387", "NB0816", "NB0921"])
        self.assertEqual(info["detectors"], list(range(104)))
        self.assertEqual(info["skymap"], "hsc_rings_v1")
    
    def test_get_instrument_info_lsst(self):
        """Test getting LSST instrument information."""
        info = get_instrument_info("LSSTCam")
        
        self.assertEqual(info["class_name"], "lsst.obs.lsst.LsstCam")
        self.assertEqual(info["filters"], ["u", "g", "r", "i", "z", "y"])
        self.assertEqual(info["detectors"], list(range(189)))
        self.assertEqual(info["skymap"], "lsst_cells_v1")
    
    def test_get_instrument_info_unknown(self):
        """Test getting information for unknown instrument."""
        info = get_instrument_info("UnknownInstrument")
        
        self.assertEqual(info, {})
    
    def test_format_size(self):
        """Test formatting file sizes."""
        self.assertEqual(format_size(500), "500.0 B")
        self.assertEqual(format_size(1536), "1.5 KB")
        self.assertEqual(format_size(1048576), "1.0 MB")
        self.assertEqual(format_size(1073741824), "1.0 GB")
        self.assertEqual(format_size(1099511627776), "1.0 TB")
        self.assertEqual(format_size(1125899906842624), "1.0 PB")
    
    def test_estimate_repository_size(self):
        """Test estimating repository size."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Create test FITS files with known sizes
        file1 = test_dir / "file1.fits"
        file2 = test_dir / "file2.fits"
        file1.touch()
        file2.touch()
        
        # Set file sizes
        file1.write_bytes(b'\0' * 1024)  # 1 KB
        file2.write_bytes(b'\0' * 2048)  # 2 KB
        
        # Estimate size
        estimate = estimate_repository_size(test_dir)
        
        self.assertEqual(estimate["data_size"], 3072)  # 3 KB
        self.assertEqual(estimate["data_size_formatted"], "3.0 KB")
        self.assertEqual(estimate["file_count"], 2)
        self.assertEqual(estimate["estimated_registry_size"], 20000)  # 2 files * 10KB
        self.assertEqual(estimate["estimated_registry_size_formatted"], "19.5 KB")
        self.assertEqual(estimate["total_size"], 23072)  # 3 KB + 20 KB
        self.assertEqual(estimate["total_size_formatted"], "22.5 KB")
    
    def test_estimate_repository_size_with_butler_yaml(self):
        """Test estimating repository size with butler.yaml."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Create butler.yaml
        (test_dir / "butler.yaml").touch()
        
        # Create test FITS files with known sizes
        file1 = test_dir / "file1.fits"
        file2 = test_dir / "file2.fits"
        file1.touch()
        file2.touch()
        
        # Set file sizes
        file1.write_bytes(b'\0' * 1024)  # 1 KB
        file2.write_bytes(b'\0' * 2048)  # 2 KB
        
        # Estimate size
        estimate = estimate_repository_size(test_dir)
        
        self.assertEqual(estimate["data_size"], 3072)  # 3 KB
        self.assertEqual(estimate["data_size_formatted"], "3.0 KB")
        self.assertEqual(estimate["file_count"], 2)  # Only FITS files counted
        self.assertEqual(estimate["estimated_registry_size"], 20000)  # 2 files * 10KB
        self.assertEqual(estimate["estimated_registry_size_formatted"], "19.5 KB")
        self.assertEqual(estimate["total_size"], 23072)  # 3 KB + 20 KB
        self.assertEqual(estimate["total_size_formatted"], "22.5 KB")
    
    @patch('subprocess.run')
    def test_validate_butler_command_success(self, mock_subprocess):
        """Test successful butler command validation."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Help text", stderr="")
        
        result = validate_butler_command()
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "--help")
    
    @patch('subprocess.run')
    def test_validate_butler_command_failure(self, mock_subprocess):
        """Test butler command validation failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Command not found")
        
        result = validate_butler_command()
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_validate_butler_command_file_not_found(self, mock_subprocess):
        """Test butler command validation with command not found."""
        # Mock FileNotFoundError
        mock_subprocess.side_effect = FileNotFoundError()
        
        result = validate_butler_command()
        
        self.assertFalse(result)
    
    @patch('lsst.daf.butler.__version__')
    def test_get_butler_version_lsst_package(self, mock_version):
        """Test getting butler version from LSST package."""
        # Mock version
        mock_version.__get__ = MagicMock(return_value="1.2.3")
        
        result = get_butler_version()
        
        self.assertEqual(result, "Butler 1.2.3")
    
    @patch('lsst.daf.butler.__version__')
    @patch('subprocess.run')
    def test_get_butler_version_fallback_to_command(self, mock_subprocess, mock_version):
        """Test getting butler version with fallback to command."""
        # Mock version to be None
        mock_version.__get__ = MagicMock(return_value=None)
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Help text", stderr="")
        
        result = get_butler_version()
        
        self.assertEqual(result, "Butler (version unknown)")
        
        # Verify subprocess was called
        mock_subprocess.assert_called_once()
    
    @patch('lsst.daf.butler.__version__')
    @patch('subprocess.run')
    def test_get_butler_version_no_version(self, mock_subprocess, mock_version):
        """Test getting butler version with no version available."""
        # Mock version to be None
        mock_version.__get__ = MagicMock(return_value=None)
        
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Command not found")
        
        result = get_butler_version()
        
        self.assertIsNone(result)
    
    def test_find_data_files_with_nonexistent_directory(self):
        """Test finding data files with non-existent directory."""
        non_existent_dir = Path(self.temp_dir) / "non_existent"
        
        files = find_data_files(non_existent_dir, ["*.fits"])
        
        self.assertEqual(len(files), 0)
    
    def test_find_data_files_with_no_matching_files(self):
        """Test finding data files with no matching files."""
        # Create test files
        test_dir = Path(self.temp_dir) / "test_data"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        (test_dir / "file1.txt").touch()
        (test_dir / "file2.txt").touch()
        
        # Find FITS files (none exist)
        files = find_data_files(test_dir, ["*.fits"])
        
        self.assertEqual(len(files), 0)
    
    def test_estimate_repository_size_with_empty_directory(self):
        """Test estimating repository size with empty directory."""
        # Create empty directory
        test_dir = Path(self.temp_dir) / "empty"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        # Estimate size
        estimate = estimate_repository_size(test_dir)
        
        self.assertEqual(estimate["data_size"], 0)
        self.assertEqual(estimate["data_size_formatted"], "0.0 B")
        self.assertEqual(estimate["file_count"], 0)
        self.assertEqual(estimate["estimated_registry_size"], 0)
        self.assertEqual(estimate["estimated_registry_size_formatted"], "0.0 B")
        self.assertEqual(estimate["total_size"], 0)
        self.assertEqual(estimate["total_size_formatted"], "0.0 B")
    
    def test_estimate_repository_size_with_nonexistent_directory(self):
        """Test estimating repository size with non-existent directory."""
        non_existent_dir = Path(self.temp_dir) / "non_existent"
        
        # Estimate size
        estimate = estimate_repository_size(non_existent_dir)
        
        self.assertEqual(estimate["data_size"], 0)
        self.assertEqual(estimate["data_size_formatted"], "0.0 B")
        self.assertEqual(estimate["file_count"], 0)
        self.assertEqual(estimate["estimated_registry_size"], 0)
        self.assertEqual(estimate["estimated_registry_size_formatted"], "0.0 B")
        self.assertEqual(estimate["total_size"], 0)
        self.assertEqual(estimate["total_size_formatted"], "0.0 B")


if __name__ == '__main__':
    unittest.main()