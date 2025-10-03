#!/usr/bin/env python
"""
RIPPLe Main Pipeline Entry Point.

This is the main entry point for the RIPPLe (Rubin Image Preparation and 
Processing Lensing engine) pipeline. It handles repository setup, data access,
and pipeline execution based on user configuration.

Usage:
    python -m ripple.main config.yaml
    python -m ripple.main --help
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Add ripple to path if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

from ripple.butler_repo import ButlerRepoManager, load_config, get_default_config, save_config
from ripple.butler_repo.utils import check_lsst_environment, validate_butler_command
from ripple.data_access import LsstDataFetcher, ButlerConfig
from ripple.utils.logger import Logger


class RipplePipeline:
    """Main RIPPLe pipeline orchestrator."""
    
    def __init__(self, config_path: str):
        """
        Initialize RIPPLe pipeline.
        
        Parameters
        ----------
        config_path : str
            Path to configuration YAML file
        """
        self.config_path = Path(config_path)
        self.config = None
        self.repo_manager = None
        self.data_fetcher = None
        self.repo_path = None
        
    def run(self) -> int:
        """
        Run the complete pipeline.
        
        Returns
        -------
        int
            Exit code (0 for success, non-zero for failure)
        """
        Logger.header("=" * 60)
        Logger.header("RIPPLe Pipeline - Rubin Image Preparation and Processing")
        Logger.header("=" * 60)
        
        try:
            # Step 1: Check environment
            if not self._check_environment():
                return 1
            
            # Step 2: Load configuration
            if not self._load_configuration():
                return 1
            
            # Step 3: Setup Butler repository
            if not self._setup_repository():
                return 1
            
            # Step 4: Run pipeline operations
            if not self._run_pipeline():
                return 1
            
            Logger.header("\n" + "=" * 60)
            Logger.header("RIPPLe pipeline execution completed successfully!")
            Logger.header(f"Repository location: {self.repo_path}")
            Logger.header("=" * 60)
            
            return 0
            
        except KeyboardInterrupt:
            Logger.warning("\nPipeline interrupted by user")
            return 130
        except Exception as e:
            Logger.error(f"Pipeline failed with error: {e}")
            return 1
    
    def _check_environment(self) -> bool:
        """Check if LSST environment is properly configured."""
        Logger.step("Step 1", "Checking environment...")
        
        # Check LSST stack
        if not check_lsst_environment():
            Logger.error("\nLSST Science Pipelines not found!")
            Logger.error("Please activate the LSST environment:")
            Logger.error("  source /path/to/lsst_stack/loadLSST.sh")
            Logger.error("  setup lsst_distrib")
            return False
        
        # Check butler command
        if not validate_butler_command():
            Logger.error("Butler command not found. Please ensure LSST stack is properly set up.")
            return False
        
        Logger.success("✓ Environment check passed")
        return True
    
    def _load_configuration(self) -> bool:
        """Load and validate configuration."""
        Logger.step("Step 2", f"Loading configuration from {self.config_path}")
        
        try:
            self.config = load_config(self.config_path)
            Logger.success("✓ Configuration loaded successfully")
            
            # Log key configuration details
            Logger.info(f"  Data source: {self.config.data_source.get('type', 'N/A')}")
            Logger.info(f"  Instrument: {self.config.instrument.name}")
            
            return True
            
        except FileNotFoundError:
            Logger.error(f"Configuration file not found: {self.config_path}")
            Logger.error("Use --generate-config to create a template configuration")
            return False
        except Exception as e:
            Logger.error(f"Failed to load configuration: {e}")
            return False
    
    def _setup_repository(self) -> bool:
        """Set up Butler repository based on configuration."""
        Logger.step("Step 3", "Setting up Butler repository...")
        
        self.repo_manager = ButlerRepoManager(self.config)
        success, result = self.repo_manager.setup_repository()
        
        if success:
            self.repo_path = result
            Logger.success(f"✓ Repository ready at: {result}")
            return True
        else:
            Logger.error(f"Failed to setup repository: {result}")
            return False
    
    def _initialize_data_access(self) -> bool:
        """Initialize data access layer."""
        Logger.step("Step 4", "Initializing data access...")
        
        # Skip for server-based repositories
        if self.config.data_source.get('type') == 'butler_server':
            Logger.info("Using remote Butler server - skipping local data access initialization")
            return True
        
        try:
            # Get data fetcher from repo manager if available
            self.data_fetcher = self.repo_manager.get_data_fetcher()
            
            if self.data_fetcher:
                Logger.success("✓ Data access initialized from repository manager")
            else:
                # Create new data fetcher
                butler_config = ButlerConfig(
                    repo_path=str(self.repo_path),
                    collections=self.config.data_source.get('params', {}).get('collections') or [
                        f"{self.config.instrument.name}/defaults",
                        f"{self.config.instrument.name}/raw/all",
                        f"{self.config.instrument.name}/calib",
                        "refcats"
                    ],
                    instrument=self.config.instrument.name,
                    cache_size=self.config.processing.get('cache_size', 1000),
                    enable_performance_monitoring=self.config.processing.get('enable_performance_monitoring', True)
                )
                
                self.data_fetcher = LsstDataFetcher(butler_config)
                Logger.success("✓ Data access initialized")
            
            # Test data access
            validation = self.data_fetcher.validate_configuration()
            if validation['butler_connection']:
                Logger.success("✓ Butler connection verified")
            else:
                Logger.warning("Butler connection validation failed")
            
            return True
            
        except Exception as e:
            Logger.error(f"Failed to initialize data access: {e}")
            return False
    
    def _run_pipeline(self) -> bool:
        """Run the main pipeline operations using PipelineBuilder and PipelineExecutor."""
        Logger.step("Step 5", "Running pipeline operations...")
        
        try:
            # Import the new pipeline components
            from ripple.pipeline.pipeline_builder import PipelineBuilder
            from ripple.pipeline.pipeline_executor import PipelineExecutor
            
            # 1. Build the pipeline using the loaded configuration
            Logger.info("Building pipeline from configuration...")
            pipeline_builder = PipelineBuilder(config=self.config)
            pipeline = pipeline_builder.build_pipeline()
            Logger.success(f"✓ Pipeline '{pipeline.name}' built successfully with {len(pipeline.stages)} stages.")
            
            # 2. Execute the pipeline
            Logger.info("Executing pipeline...")
            pipeline_executor = PipelineExecutor(pipeline=pipeline)
            pipeline_executor.execute()
            Logger.success("✓ Pipeline execution initiated.")
            
        except ImportError as e:
            Logger.error(f"Failed to import pipeline components: {e}")
            return False
        except Exception as e:
            Logger.error(f"An error occurred during pipeline operations: {e}")
            return False
        
        Logger.success("\n✓ Pipeline operations completed")
        return True


def main():
    """Main entry point for RIPPLe pipeline."""
    parser = argparse.ArgumentParser(
        description="RIPPLe - Rubin Image Preparation and Processing Lensing engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run pipeline with configuration file
  python -m ripple.main --config-file config.yaml
  
  # Generate default configuration
  python -m ripple.main --generate-config my_config.yaml
  
  # Run with verbose output
  python -m ripple.main --config-file config.yaml --verbose
  
  # Show version information
  python -m ripple.main --version
        """
    )
    
    parser.add_argument(
        "--config-file",
        metavar="FILE",
        required=True,
        help="Path to the configuration YAML file"
    )
    
    parser.add_argument(
        "--generate-config",
        metavar="FILE NAME",
        help="Generate default configuration file"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version information"
    )
    
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Check environment setup only"
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Show version
    if args.version:
        Logger.header("RIPPLe (Rubin Image Preparation and Processing Lensing engine)")
        Logger.info("Version: 0.1.0")
        return 0
    
    # Check environment only
    if args.check_env:
        if check_lsst_environment() and validate_butler_command():
            Logger.success("✓ Environment check passed")
            Logger.info("  LSST Science Pipelines: Available")
            Logger.info("  Butler command: Available")
            return 0
        else:
            Logger.error("✗ Environment check failed")
            return 1
    
    # Generate configuration
    if args.generate_config:
        config = get_default_config()
        save_config(config, args.generate_config)
        Logger.success(f"Generated configuration file: {args.generate_config}")
        Logger.info("\nPlease edit the configuration file to:")
        Logger.info("  1. Set the correct data source path")
        Logger.info("  2. Choose the appropriate instrument")
        Logger.info("  3. Configure processing parameters")
        return 0
    
    # Run pipeline
    pipeline = RipplePipeline(args.config_file)
    return pipeline.run()


if __name__ == "__main__":
    sys.exit(main())