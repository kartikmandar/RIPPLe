"""
Test suite for the ingest_data module.

This module tests the functionality of the ingest_data module including:
- Raw data ingestion
- Calibration ingestion
- Reference catalog ingestion
- Visit definition
- Import from export file
"""

import unittest
import subprocess
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path
from datetime import datetime

# Import the modules to test
from ripple.butler_repo.ingest_data import DataIngestor
from ripple.butler_repo.config_handler import get_default_config


class TestIngestData(unittest.TestCase):
    """Test data ingestion functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = Path(self.temp_dir) / "test_repo"
        self.data_path = Path(self.temp_dir) / "test_data"
        self.calib_path = Path(self.temp_dir) / "calib"
        self.refcat_path = Path(self.temp_dir) / "refcats"
        
        # Create directories
        self.repo_path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.calib_path.mkdir(parents=True, exist_ok=True)
        self.refcat_path.mkdir(parents=True, exist_ok=True)
        
        # Create butler.yaml to simulate existing repository
        (self.repo_path / "butler.yaml").touch()
        
        # Create test configuration
        self.test_config = get_default_config()
        self.test_config.data_source.path = str(self.data_path)
        self.test_config.ingestion.calibration_path = str(self.calib_path)
        self.test_config.ingestion.reference_catalog_path = str(self.refcat_path)
        
        # Create test data files
        (self.data_path / "raw_file1.fits").touch()
        (self.data_path / "raw_file2.fits").touch()
        
        # Create test calibration files
        (self.calib_path / "bias_file1.fits").touch()
        (self.calib_path / "dark_file1.fits").touch()
        (self.calib_path / "flat_file1.fits").touch()
        
        # Create test reference catalog files
        (self.refcat_path / "gaia_dr2_20200414.fits").touch()
        (self.refcat_path / "ps1_pv3_3pi_20170110.fits").touch()
        
        # Create data ingestor
        self.ingestor = DataIngestor(str(self.repo_path), self.test_config)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    @patch('subprocess.run')
    def test_ingest_all_success(self, mock_subprocess):
        """Test successful ingestion of all data types."""
        # Mock successful subprocess calls
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Ingested 2 files", stderr="")
        
        results = self.ingestor.ingest_all()
        
        self.assertTrue(results["raw_data"]["success"])
        self.assertEqual(results["raw_data"]["count"], 2)
        self.assertTrue(results["calibrations"]["success"])
        self.assertTrue(results["reference_catalogs"]["success"])
        self.assertTrue(results["visits_defined"])
    
    @patch('subprocess.run')
    def test_ingest_all_partial_success(self, mock_subprocess):
        """Test partial success ingestion of all data types."""
        # Mock mixed success subprocess calls
        mock_subprocess.side_effect = [
            MagicMock(returncode=0, stdout="Ingested 2 files", stderr=""),  # raw data success
            subprocess.CalledProcessError(1, "butler", stderr="Ingestion failed"),  # bias calibrations ingestion failure
            subprocess.CalledProcessError(1, "butler", stderr="Certification failed"),  # bias calibrations certification failure
            subprocess.CalledProcessError(1, "butler", stderr="Ingestion failed"),  # dark calibrations ingestion failure
            subprocess.CalledProcessError(1, "butler", stderr="Certification failed"),  # dark calibrations certification failure
            subprocess.CalledProcessError(1, "butler", stderr="Ingestion failed"),  # flat calibrations ingestion failure
            subprocess.CalledProcessError(1, "butler", stderr="Certification failed"),  # flat calibrations certification failure
            MagicMock(returncode=0, stdout="Registered dataset type", stderr=""),  # refcats dataset type registration 1
            MagicMock(returncode=0, stdout="Ingested files", stderr=""),  # refcats ingestion 1
            MagicMock(returncode=0, stdout="Added to collection", stderr=""),  # refcats collection 1
            MagicMock(returncode=0, stdout="Registered dataset type", stderr=""),  # refcats dataset type registration 2
            MagicMock(returncode=0, stdout="Ingested files", stderr=""),  # refcats ingestion 2
            MagicMock(returncode=0, stdout="Added to collection", stderr=""),  # refcats collection 2
            MagicMock(returncode=0, stdout="Visits defined", stderr="")  # visits success
        ]
        
        results = self.ingestor.ingest_all()
        
        self.assertTrue(results["raw_data"]["success"])
        self.assertEqual(results["raw_data"]["count"], 2)
        self.assertFalse(results["calibrations"]["success"])
        self.assertTrue(results["reference_catalogs"]["success"])
        self.assertTrue(results["visits_defined"])
    
    @patch('subprocess.run')
    def test_ingest_raw_data_success(self, mock_subprocess):
        """Test successful raw data ingestion."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Ingested 2 files", stderr="")
        
        results = self.ingestor.ingest_raw_data()
        
        self.assertTrue(results["success"])
        self.assertEqual(results["count"], 2)
        self.assertEqual(len(results["errors"]), 0)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "ingest-raws")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn(str(self.data_path / "raw_file1.fits"), args[0])
        self.assertIn(str(self.data_path / "raw_file2.fits"), args[0])
    
    def test_ingest_raw_data_no_files(self):
        """Test raw data ingestion with no files found."""
        # Remove data files
        for file in self.data_path.glob("*.fits"):
            file.unlink()
        
        results = self.ingestor.ingest_raw_data()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("No raw data files found", results["errors"])
    
    @patch('subprocess.run')
    def test_ingest_raw_data_failure(self, mock_subprocess):
        """Test raw data ingestion failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Ingestion failed")
        
        results = self.ingestor.ingest_raw_data()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("Failed to ingest batch", results["errors"])
    
    @patch('subprocess.run')
    def test_ingest_raw_data_with_skip_existing(self, mock_subprocess):
        """Test raw data ingestion with skip_existing option."""
        # Configure to skip existing
        self.test_config.ingestion.skip_existing = True
        self.ingestor = DataIngestor(str(self.repo_path), self.test_config)
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Ingested 2 files", stderr="")
        
        results = self.ingestor.ingest_raw_data()
        
        self.assertTrue(results["success"])
        self.assertEqual(results["count"], 2)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "ingest-raws")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn("--skip-existing", args[0])
    
    @patch('subprocess.run')
    def test_define_visits_success(self, mock_subprocess):
        """Test successful visit definition."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Visits defined", stderr="")
        
        result = self.ingestor.define_visits()
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "define-visits")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn(self.test_config.instrument.class_name, args[0])
    
    @patch('subprocess.run')
    def test_define_visits_failure(self, mock_subprocess):
        """Test visit definition failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Visit definition failed")
        
        result = self.ingestor.define_visits()
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_ingest_calibrations_success(self, mock_subprocess):
        """Test successful calibration ingestion."""
        # Mock successful subprocess calls
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Ingested files", stderr="")
        
        results = self.ingestor.ingest_calibrations()
        
        self.assertTrue(results["success"])
        self.assertEqual(results["count"], 3)  # bias, dark, flat
        self.assertEqual(len(results["errors"]), 0)
        
        # Verify subprocess was called correctly
        # 3 calibration types * 2 calls each (ingest + certify) = 6 calls
        self.assertEqual(mock_subprocess.call_count, 6)
    
    def test_ingest_calibrations_path_not_exists(self):
        """Test calibration ingestion with non-existent path."""
        # Remove calibration directory
        shutil.rmtree(self.calib_path)
        
        results = self.ingestor.ingest_calibrations()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("Calibration path does not exist", results["errors"])
    
    @patch('subprocess.run')
    def test_ingest_calibrations_failure(self, mock_subprocess):
        """Test calibration ingestion failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Ingestion failed")
        
        results = self.ingestor.ingest_calibrations()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("Failed to ingest", results["errors"])
    
    @patch('subprocess.run')
    def test_certify_calibrations_success(self, mock_subprocess):
        """Test successful calibration certification."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Calibrations certified", stderr="")
        
        result = self.ingestor._certify_calibrations("bias")
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "certify-calibrations")
        self.assertIn(str(self.repo_path), args[0])
    
    @patch('subprocess.run')
    def test_certify_calibrations_failure(self, mock_subprocess):
        """Test calibration certification failure."""
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Certification failed")
        
        result = self.ingestor._certify_calibrations("bias")
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_ingest_reference_catalogs_success(self, mock_subprocess):
        """Test successful reference catalog ingestion."""
        # Mock successful subprocess calls
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Ingested files", stderr="")
        
        results = self.ingestor.ingest_reference_catalogs()
        
        self.assertTrue(results["success"])
        self.assertEqual(results["count"], 2)  # gaia, ps1
        self.assertEqual(len(results["errors"]), 0)
        
        # Verify subprocess was called correctly
        self.assertEqual(mock_subprocess.call_count, 6)  # 2 catalogs * 3 operations each
    
    def test_ingest_reference_catalogs_path_not_exists(self):
        """Test reference catalog ingestion with non-existent path."""
        # Remove reference catalog directory
        shutil.rmtree(self.refcat_path)
        
        results = self.ingestor.ingest_reference_catalogs()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("Reference catalog path does not exist", results["errors"])
    
    def test_ingest_reference_catalogs_no_catalogs(self):
        """Test reference catalog ingestion with no catalogs found."""
        # Remove reference catalog files
        for file in self.refcat_path.glob("*.fits"):
            file.unlink()
        
        results = self.ingestor.ingest_reference_catalogs()
        
        self.assertFalse(results["success"])
        self.assertEqual(results["count"], 0)
        self.assertIn("No reference catalogs found", results["errors"])
    
    @patch('subprocess.run')
    def test_register_refcat_dataset_type_success(self, mock_subprocess):
        """Test successful reference catalog dataset type registration."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Dataset type registered", stderr="")
        
        result = self.ingestor._register_refcat_dataset_type("gaia_dr2")
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "register-dataset-type")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn("gaia_dr2", args[0])
    
    @patch('subprocess.run')
    def test_register_refcat_dataset_type_already_exists(self, mock_subprocess):
        """Test reference catalog dataset type registration when it already exists."""
        # Mock subprocess call with "already exists" in stderr
        mock_subprocess.return_value = MagicMock(
            returncode=1, 
            stdout="", 
            stderr="Dataset type 'gaia_dr2' already exists"
        )
        
        result = self.ingestor._register_refcat_dataset_type("gaia_dr2")
        
        self.assertTrue(result)  # Should still return True
    
    @patch('subprocess.run')
    def test_ingest_refcat_files_success(self, mock_subprocess):
        """Test successful reference catalog file ingestion."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Files ingested", stderr="")
        
        result = self.ingestor._ingest_refcat_files(
            "gaia_dr2", 
            [self.refcat_path / "gaia_dr2_20200414.fits"]
        )
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "ingest-files")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn("gaia_dr2", args[0])
    
    @patch('subprocess.run')
    def test_add_to_refcats_collection_success(self, mock_subprocess):
        """Test successful addition to refcats collection."""
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Added to collection", stderr="")
        
        result = self.ingestor._add_to_refcats_collection("gaia_dr2")
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "collection-chain")
        self.assertIn(str(self.repo_path), args[0])
    
    @patch('subprocess.run')
    def test_import_from_export_success(self, mock_subprocess):
        """Test successful import from export file."""
        # Create export file
        export_file = Path(self.temp_dir) / "export.yaml"
        export_file.touch()
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Import successful", stderr="")
        
        result = self.ingestor.import_from_export(str(export_file), str(self.data_path))
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "import")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn(str(self.data_path), args[0])
        self.assertIn("--export-file", args[0])
    
    @patch('subprocess.run')
    def test_import_from_export_failure(self, mock_subprocess):
        """Test import from export file failure."""
        # Create export file
        export_file = Path(self.temp_dir) / "export.yaml"
        export_file.touch()
        
        # Mock failed subprocess call
        mock_subprocess.return_value = MagicMock(returncode=1, stdout="", stderr="Import failed")
        
        result = self.ingestor.import_from_export(str(export_file), str(self.data_path))
        
        self.assertFalse(result)
    
    @patch('subprocess.run')
    def test_import_from_export_with_transfer_mode(self, mock_subprocess):
        """Test import from export file with transfer mode."""
        # Create export file
        export_file = Path(self.temp_dir) / "export.yaml"
        export_file.touch()
        
        # Configure transfer mode
        self.test_config.ingestion.transfer_mode = "copy"
        self.ingestor = DataIngestor(str(self.repo_path), self.test_config)
        
        # Mock successful subprocess call
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="Import successful", stderr="")
        
        result = self.ingestor.import_from_export(str(export_file), str(self.data_path))
        
        self.assertTrue(result)
        
        # Verify subprocess was called correctly
        mock_subprocess.assert_called_once()
        args, kwargs = mock_subprocess.call_args
        self.assertEqual(args[0][0], "butler")
        self.assertEqual(args[0][1], "import")
        self.assertIn(str(self.repo_path), args[0])
        self.assertIn(str(self.data_path), args[0])
        self.assertIn("--export-file", args[0])
        self.assertIn("--transfer", args[0])
        self.assertIn("copy", args[0])
    
    def test_detect_reference_catalogs_gaia_dr2(self):
        """Test detection of Gaia DR2 reference catalog."""
        # Create test Gaia DR2 file
        gaia_file = self.refcat_path / "gaia_dr2_20200414.fits"
        gaia_file.touch()
        
        catalogs = self.ingestor._detect_reference_catalogs(self.refcat_path)
        
        self.assertIn("gaia_dr2_20200414", catalogs)
        self.assertEqual(catalogs["gaia_dr2_20200414"], [gaia_file])
    
    def test_detect_reference_catalogs_gaia_dr3(self):
        """Test detection of Gaia DR3 reference catalog."""
        # Create test Gaia DR3 file
        gaia_file = self.refcat_path / "gaia_dr3_20230101.fits"
        gaia_file.touch()
        
        catalogs = self.ingestor._detect_reference_catalogs(self.refcat_path)
        
        self.assertIn("gaia_dr3", catalogs)
        self.assertEqual(catalogs["gaia_dr3"], [gaia_file])
    
    def test_detect_reference_catalogs_ps1(self):
        """Test detection of PS1 reference catalog."""
        # Create test PS1 file
        ps1_file = self.refcat_path / "ps1_pv3_3pi_20170110.fits"
        ps1_file.touch()
        
        catalogs = self.ingestor._detect_reference_catalogs(self.refcat_path)
        
        self.assertIn("ps1_pv3_3pi_20170110", catalogs)
        self.assertEqual(catalogs["ps1_pv3_3pi_20170110"], [ps1_file])
    
    def test_detect_reference_catalogs_2mass(self):
        """Test detection of 2MASS reference catalog."""
        # Create test 2MASS file
        twomass_file = self.refcat_path / "2mass_20200101.fits"
        twomass_file.touch()
        
        catalogs = self.ingestor._detect_reference_catalogs(self.refcat_path)
        
        self.assertIn("2mass", catalogs)
        self.assertEqual(catalogs["2mass"], [twomass_file])
    
    def test_detect_reference_catalogs_no_files(self):
        """Test detection of reference catalogs with no files."""
        # Remove all reference catalog files
        for file in self.refcat_path.glob("*.fits"):
            file.unlink()
        
        catalogs = self.ingestor._detect_reference_catalogs(self.refcat_path)
        
        self.assertEqual(len(catalogs), 0)


if __name__ == '__main__':
    unittest.main()