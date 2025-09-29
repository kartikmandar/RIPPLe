"""
Butler Repository Creation Module.

This module handles the creation and initialization of LSST Butler Gen3 repositories,
including verification of existing repositories.
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

from .config_handler import RepoConfig

logger = logging.getLogger(__name__)


def initialize_repository(config: RepoConfig, repo_path: str) -> bool:
    """
    Initialize a new Butler repository at the specified path.
    
    Parameters
    ----------
    config : RepoConfig
        Repository configuration object
    repo_path : str
        Path where the repository should be created
        
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    try:
        repo_path_obj = Path(repo_path)
        
        # Check if repository already exists
        if (repo_path_obj / "butler.yaml").exists():
            logger.info(f"Repository already exists at {repo_path}")
            return True
            
        # Create parent directory if it doesn't exist
        repo_path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        # Build butler create command
        cmd = ["butler", "create"]
        
        # Add configuration options
        if config.butler.dimension_config:
            cmd.extend(["--dimension-config", config.butler.dimension_config])
            
        if config.butler.seed_config:
            cmd.extend(["--seed-config", config.butler.seed_config])
            
        if config.butler.standalone:
            cmd.append("--standalone")
            
        if config.butler.override:
            cmd.append("--override")
            
        # Add registry configuration
        if config.butler.registry_db == "postgresql" and config.butler.postgres_url:
            logger.debug(f"Adding PostgreSQL configuration with URL: {config.butler.postgres_url}")
            cmd.extend(["--postgres", config.butler.postgres_url])
        elif config.butler.registry_db == "sqlite":
            # SQLite is default, no extra flags needed
            pass
            
        # Add output path
        cmd.append(str(repo_path_obj))
        
        logger.info(f"Creating Butler repository at {repo_path}")
        logger.debug(f"Command: {' '.join(cmd)}")
        
        # Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            logger.error(f"Failed to create repository: {result.stderr}")
            return False
            
        logger.info("Repository created successfully")
        if result.stdout:
            logger.debug(f"Command output: {result.stdout}")
        
        # Register instrument if specified
        if config.instrument.class_name:
            logger.info(f"Registering instrument: {config.instrument.class_name}")
            register_cmd = ["butler", "register-instrument", str(repo_path_obj), config.instrument.class_name]
            logger.debug(f"Register command: {' '.join(register_cmd)}")
            
            register_result = subprocess.run(
                register_cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            if register_result.returncode != 0:
                logger.error(f"Failed to register instrument: {register_result.stderr}")
                return False
                
            logger.info("Instrument registered successfully")
            if register_result.stdout:
                logger.debug(f"Register command output: {register_result.stdout}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error creating repository: {e}")
        return False


def verify_repository(repo_path: str) -> Dict[str, Any]:
    """
    Verify an existing Butler repository.
    
    Parameters
    ----------
    repo_path : str
        Path to the repository to verify
        
    Returns
    -------
    Dict[str, Any]
        Verification results with keys:
        - 'valid': bool, whether repository is valid
        - 'errors': List[str], list of errors found
        - 'warnings': List[str], list of warnings
        - 'info': Dict[str, Any], additional repository information
    """
    result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'info': {}
    }
    
    try:
        repo_path_obj = Path(repo_path)
        
        # Check if repository path exists
        if not repo_path_obj.exists():
            error_msg = f"Repository path does not exist: {repo_path}"
            logger.debug(f"Adding error message: {error_msg}")
            result['valid'] = False
            result['errors'].append(error_msg)
            return result
            
        # Check for butler.yaml file
        butler_yaml = repo_path_obj / "butler.yaml"
        if not butler_yaml.exists():
            error_msg = f"butler.yaml not found in {repo_path}"
            logger.debug(f"Adding error message: {error_msg}")
            result['valid'] = False
            result['errors'].append(error_msg)
            return result
            
        # Try to get repository information
        try:
            cmd = ["butler", "info", str(repo_path_obj)]
            info_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            if info_result.returncode == 0:
                # Parse basic info from output
                result['info']['raw_output'] = info_result.stdout
                
                # Extract key information
                for line in info_result.stdout.split('\n'):
                    if ':' in line:
                        key, value = line.split(':', 1)
                        key = key.strip()
                        value = value.strip()
                        if key and value:
                            result['info'][key.lower().replace(' ', '_')] = value
            else:
                warning_msg = f"Could not get repository info: {info_result.stderr}"
                logger.debug(f"Adding warning message: {warning_msg}")
                result['warnings'].append(warning_msg)
                
        except Exception as e:
            result['warnings'].append(f"Error getting repository info: {e}")
            
        # Check for collections
        try:
            cmd = ["butler", "query-collections", str(repo_path_obj)]
            collections_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            if collections_result.returncode == 0:
                collections = collections_result.stdout.strip().split('\n')
                if collections and collections[0]:  # Not empty
                    result['info']['collections'] = collections
                    result['info']['collection_count'] = len(collections)
                else:
                    result['warnings'].append("No collections found in repository")
            else:
                warning_msg = f"Could not query collections: {collections_result.stderr}"
                logger.debug(f"Adding warning message: {warning_msg}")
                result['warnings'].append(warning_msg)
                
        except Exception as e:
            result['warnings'].append(f"Error querying collections: {e}")
            
        # Check registry health
        try:
            cmd = ["butler", "query-datasets", "--collections", ".*", str(repo_path_obj)]
            datasets_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            if datasets_result.returncode == 0:
                # Count datasets (rough estimate)
                dataset_lines = [line for line in datasets_result.stdout.split('\n') if line.strip()]
                logger.debug(f"Dataset lines: {dataset_lines}, count: {len(dataset_lines)}")
                if dataset_lines:
                    result['info']['dataset_count'] = len(dataset_lines) - 1  # Subtract header
                    logger.debug(f"Dataset count after subtracting header: {result['info']['dataset_count']}")
            else:
                # This might fail if no datasets exist, which is normal for new repos
                if "No datasets found" not in datasets_result.stderr:
                    warning_msg = f"Could not query datasets: {datasets_result.stderr}"
                    logger.debug(f"Adding warning message: {warning_msg}")
                    result['warnings'].append(warning_msg)
                    
        except Exception as e:
            result['warnings'].append(f"Error querying datasets: {e}")
            
    except Exception as e:
        result['valid'] = False
        result['errors'].append(f"Verification failed: {e}")
        
    return result


def get_repository_info(repo_path: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a Butler repository.
    
    Parameters
    ----------
    repo_path : str
        Path to the repository
        
    Returns
    -------
    Optional[Dict[str, Any]]
        Repository information dictionary, or None if failed
    """
    try:
        repo_path_obj = Path(repo_path)
        
        if not (repo_path_obj / "butler.yaml").exists():
            return None
            
        info = {}
        
        # Get basic info
        cmd = ["butler", "info", str(repo_path_obj)]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse the output
        for line in result.stdout.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    info[key.lower().replace(' ', '_')] = value
                    
        # Get collections
        cmd = ["butler", "query-collections", str(repo_path_obj)]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        collections = result.stdout.strip().split('\n')
        if collections and collections[0]:  # Not empty
            info['collections'] = collections
            
        return info
        
    except Exception as e:
        logger.error(f"Error getting repository info: {e}")
        return None


def is_repository_empty(repo_path: str) -> bool:
    """
    Check if a repository is empty (no data collections).
    
    Parameters
    ----------
    repo_path : str
        Path to the repository
        
    Returns
    -------
    bool
        True if repository is empty, False otherwise
    """
    try:
        verification = verify_repository(repo_path)
        
        if not verification['valid']:
            return False
            
        # Check if there are any data collections
        collections = verification['info'].get('collections', [])
        
        # Look for data collections (not just instrument collections)
        data_collections = [
            c for c in collections 
            if any(x in c for x in ['/raw/', '/calib/', 'refcats', '/coadd/'])
        ]
        
        return len(data_collections) == 0
        
    except Exception as e:
        logger.error(f"Error checking if repository is empty: {e}")
        return False