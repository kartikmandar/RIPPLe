#!/usr/bin/env python3
"""
Debug script to test TAP ObsCore queries and find available image data
"""

import os
import sys
sys.path.insert(0, '/home/kartikmandar/RIPPLe')

# Load environment variables from .env file manually
def load_env_file(env_file_path):
    """Load environment variables from .env file"""
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
                    if key.strip() == 'RSP_ACCESS_TOKEN':
                        print(f"✓ Loaded {key} from .env")

load_env_file('.env')

from ripple.data_access.rsp_tap_client import RSPTAPClient, create_rsp_client

def test_tap_connection():
    """Test TAP connection and explore ObsCore table"""
    print("=" * 60)
    print("TAP ObsCore Debug Script")
    print("=" * 60)

    try:
        # Initialize TAP client the same way the pipeline does
        client = create_rsp_client()
        print("✓ TAP client initialized")

        # Test 1: Check if ObsCore table exists and get basic info
        print("\n1. Testing ObsCore table access...")
        query = """
        SELECT COUNT(*) as total_images
        FROM ivoa.ObsCore
        WHERE dataproduct_type = 'image'
        """

        result = client.query_catalog(query)
        print(f"Total images in ObsCore: {result}")

        # Test 2: Check available collections
        print("\n2. Checking available collections...")
        query = """
        SELECT DISTINCT obs_collection, COUNT(*) as count
        FROM ivoa.ObsCore
        WHERE dataproduct_type = 'image'
        GROUP BY obs_collection
        ORDER BY count DESC
        """

        collections = client.query_catalog(query)
        print("Available collections:")
        for coll in collections:
            print(f"  {coll}")

        # Test 3: Check available bands
        print("\n3. Checking available bands...")
        query = """
        SELECT DISTINCT lsst_band, COUNT(*) as count
        FROM ivoa.ObsCore
        WHERE dataproduct_type = 'image' AND lsst_band IS NOT NULL
        GROUP BY lsst_band
        ORDER BY lsst_band
        """

        bands = client.query_catalog(query)
        print("Available bands:")
        for band in bands:
            print(f"  {band}")

        # Test 4: Check coordinate coverage - find any images in a larger area
        print("\n4. Testing coordinate coverage around test area...")
        query = """
        SELECT COUNT(*) as count,
               MIN(s_ra) as min_ra, MAX(s_ra) as max_ra,
               MIN(s_dec) as min_dec, MAX(s_dec) as max_dec
        FROM ivoa.ObsCore
        WHERE dataproduct_type = 'image'
          AND calib_level = 2
          AND s_ra BETWEEN 60 AND 64
          AND s_dec BETWEEN -39 AND -35
        """

        coverage = client.query_catalog(query)
        print(f"Coverage in test area (RA=60-64, Dec=-39 to -35): {coverage}")

        # Test 5: Find some actual images with relaxed constraints
        print("\n5. Finding sample images with relaxed constraints...")
        query = """
        SELECT TOP 5 obs_id, lsst_band, s_ra, s_dec, obs_collection,
               lsst_visit, access_url
        FROM ivoa.ObsCore
        WHERE dataproduct_type = 'image'
          AND calib_level = 2
          AND lsst_band IS NOT NULL
        ORDER BY lsst_visit DESC
        """

        samples = client.query_catalog(query)
        print("Sample images found:")
        for i, sample in enumerate(samples, 1):
            print(f"  {i}. {sample}")

        # Test 6: Test our NEW coordinates with data coverage
        print("\n6. Testing NEW coordinates in DP1 coverage area...")
        ra, dec = 106.68, -10.36
        query = f"""
        SELECT COUNT(*) as count
        FROM ivoa.ObsCore
        WHERE calib_level = 2
          AND dataproduct_type = 'image'
          AND CONTAINS(POINT('ICRS', {ra}, {dec}), s_region) = 1
        """

        result = client.query_catalog(query)
        print(f"Images at NEW RA={ra}, Dec={dec}: {result}")

        # Test 7: Test coordinates by band
        print("\n7. Testing by individual bands...")
        for band in ['g', 'r', 'i']:
            query = f"""
            SELECT COUNT(*) as count
            FROM ivoa.ObsCore
            WHERE calib_level = 2
              AND dataproduct_type = 'image'
              AND lsst_band = '{band}'
              AND CONTAINS(POINT('ICRS', {ra}, {dec}), s_region) = 1
            """
            result = client.query_catalog(query)
            print(f"  {band}-band images: {result}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_tap_connection()