#!/usr/bin/env python3
"""
Complete DP1 64x64 .npy Extraction Script

This script automatically generates coordinates for all DP1 fields
and extracts all possible 64x64 .npy images without requiring
manual coordinate input.

Usage:
    python extract_dp1_complete.py
"""

import os
import sys
import numpy as np
import yaml
import logging
from pathlib import Path

# Add RIPPLe to path
sys.path.insert(0, '/home/kartikmandar/RIPPLe')

from ripple.pipeline.pipeline_builder import PipelineBuilder

def setup_logging():
    """Setup logging for the extraction process."""
    log_dir = Path("/mnt/HDD2/dhruv/DP1_complete_64x64/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "dp1_extraction.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def generate_field_coordinates(field_config, spacing_arcmin=1.0):
    """
    Generate a grid of coordinates for a given field.

    Args:
        field_config (dict): Field configuration with center_ra, center_dec, radius
        spacing_arcmin (float): Spacing between grid points in arcminutes

    Returns:
        list: List of coordinate dictionaries
    """
    center_ra = float(field_config['center_ra'])
    center_dec = float(field_config['center_dec'])
    radius_deg = float(field_config['radius'])

    # Convert spacing to degrees
    spacing_deg = float(spacing_arcmin) / 60.0

    # Calculate grid bounds
    ra_min = center_ra - radius_deg + spacing_deg/2
    ra_max = center_ra + radius_deg - spacing_deg/2
    dec_min = center_dec - radius_deg + spacing_deg/2
    dec_max = center_dec + radius_deg - spacing_deg/2

    coordinates = []

    # Generate grid points using Python ranges, not numpy
    num_ra_points = int((ra_max - ra_min) / spacing_deg) + 1
    num_dec_points = int((dec_max - dec_min) / spacing_deg) + 1

    field_name = field_config['name'].replace(" ", "_").replace("-", "_")

    for i in range(num_ra_points):
        for j in range(num_dec_points):
            ra = ra_min + i * spacing_deg
            dec = dec_min + j * spacing_deg
            coordinates.append({
                'ra': round(ra, 6),
                'dec': round(dec, 6),
                'label': f"{field_name}_{i:02d}_{j:02d}"
            })

    return coordinates

def generate_all_dp1_coordinates(config_path):
    """
    Generate coordinates for all DP1 fields based on configuration.

    Args:
        config_path (str): Path to DP1 configuration file

    Returns:
        list: All coordinates for all fields
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    all_coordinates = []
    fields = config['data_source']['extraction']['fields']
    spacing = config['data_source']['extraction']['grid_sampling']['spacing_arcmin']
    max_per_field = config['advanced']['extraction_limits']['max_per_field']

    logger = logging.getLogger(__name__)
    logger.info(f"Generating coordinates for {len(fields)} DP1 fields...")

    for field in fields:
        logger.info(f"Processing field: {field['name']}")

        field_coords = generate_field_coordinates(field, spacing)

        # Limit per field if necessary
        if len(field_coords) > max_per_field:
            logger.warning(f"Limiting {field['name']} to {max_per_field} coordinates (was {len(field_coords)})")
            field_coords = field_coords[:max_per_field]

        # Add field name to each coordinate
        for coord in field_coords:
            coord['field'] = field['name']

        all_coordinates.extend(field_coords)
        logger.info(f"Generated {len(field_coords)} coordinates for {field['name']}")

    # Apply overall limit
    max_total = config['advanced']['extraction_limits']['max_total_cutouts']
    if len(all_coordinates) > max_total:
        logger.warning(f"Limiting total coordinates to {max_total} (was {len(all_coordinates)})")
        all_coordinates = all_coordinates[:max_total]

    logger.info(f"Total coordinates generated: {len(all_coordinates)}")
    return all_coordinates

def update_config_with_coordinates(config_path, coordinates):
    """
    Update the configuration file with generated coordinates.

    Args:
        config_path (str): Path to configuration file
        coordinates (list): List of coordinate dictionaries
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Update coordinates
    config['data_source']['extraction']['coordinates'] = coordinates
    config['data_source']['extraction']['auto_discover']['enabled'] = False

    # Save updated config
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    logger = logging.getLogger(__name__)
    logger.info(f"Updated configuration with {len(coordinates)} coordinates")

def run_extraction(config_path):
    """
    Run the RIPPLe pipeline for DP1 extraction.

    Args:
        config_path (str): Path to configuration file
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting DP1 64x64 extraction...")

    try:
        # Use the RIPPLe main pipeline
        from ripple.main import RipplePipeline

        # Initialize and run pipeline
        ripple_pipeline = RipplePipeline(config_path)
        exit_code = ripple_pipeline.run()

        if exit_code == 0:
            logger.info("DP1 extraction completed successfully!")
            return True
        else:
            logger.error(f"DP1 extraction failed with exit code: {exit_code}")
            return False

    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

    return True

def save_coordinate_summary(coordinates, output_path):
    """
    Save a summary of generated coordinates.

    Args:
        coordinates (list): List of coordinate dictionaries
        output_path (str): Path to save summary
    """
    summary = {
        'total_coordinates': len(coordinates),
        'fields': {}
    }

    for coord in coordinates:
        field = coord['field']
        if field not in summary['fields']:
            summary['fields'][field] = 0
        summary['fields'][field] += 1

    with open(output_path, 'w') as f:
        yaml.dump(summary, f, default_flow_style=False)

def main():
    """Main extraction process."""

    print("=" * 60)
    print("DP1 Complete 64x64 .npy Extraction")
    print("=" * 60)

    # Setup logging
    logger = setup_logging()

    # Configuration file path
    config_path = "/home/kartikmandar/RIPPLe/pipeline_configs/dp1_complete_extraction_64x64.yaml"

    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        return False

    try:
        # Step 1: Generate coordinates for all DP1 fields
        logger.info("Step 1: Generating coordinates for all DP1 fields...")
        coordinates = generate_all_dp1_coordinates(config_path)

        if not coordinates:
            logger.error("No coordinates generated!")
            return False

        # Step 2: Save coordinate summary
        summary_path = "/mnt/HDD2/dhruv/DP1_complete_64x64/coordinate_summary.yaml"
        save_coordinate_summary(coordinates, summary_path)
        logger.info(f"Coordinate summary saved to: {summary_path}")

        # Step 3: Update configuration with coordinates
        logger.info("Step 2: Updating configuration with coordinates...")
        update_config_with_coordinates(config_path, coordinates)

        # Step 4: Run extraction
        logger.info("Step 3: Running DP1 extraction...")
        success = run_extraction(config_path)

        if success:
            print("\n" + "=" * 60)
            print("✅ DP1 EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"📁 Output directory: /mnt/HDD2/dhruv/DP1_complete_64x64")
            print(f"📊 Total coordinates processed: {len(coordinates)}")
            print(f"📋 Coordinate summary: {summary_path}")
            print("🎯 Check the output directory for your .npy files!")
            print("=" * 60)
            return True
        else:
            print("\n" + "=" * 60)
            print("❌ DP1 EXTRACTION FAILED!")
            print("📋 Check the logs for error details")
            print("=" * 60)
            return False

    except Exception as e:
        logger.error(f"Extraction process failed: {e}")
        print(f"\n❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)