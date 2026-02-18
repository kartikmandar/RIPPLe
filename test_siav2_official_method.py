#!/usr/bin/env python3
"""
Test script using the official Rubin Science Platform SIAv2 method
Based on the official DP1 tutorial: https://dp1.lsst.io/tutorials/notebook/103/notebook-103-2.html
"""

import os
import sys
from pathlib import Path

def setup_environment():
    """Setup Python path and imports."""
    print("🔧 Setting up environment...")

    sys.path.insert(0, '/home/kartikmandar/RIPPLe')

    # Try to import the official Rubin RSP libraries
    try:
        from lsst.rsp.service import get_siav2_service
        from lsst.rsp.utils import get_pyvo_auth
        print("✅ Official Rubin RSP libraries imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Rubin RSP libraries import failed: {e}")
        print("💡 This suggests the lsst.rsp packages are not available")
        print("   You may need to install the lsst-scipipe environment")
        return False

def test_official_siav2():
    """Test SIAv2 using the official Rubin method."""
    print("\n🔌 Testing official SIAv2 service method...")

    try:
        from lsst.rsp.service import get_siav2_service

        # Get the SIAv2 service for DP1
        print("   Initializing SIAv2 service for DP1...")
        sia_service = get_siav2_service("dp1")
        assert sia_service is not None
        print("✅ SIAv2 service initialized successfully")

        return sia_service

    except Exception as e:
        print(f"❌ SIAv2 service initialization failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        return None

def test_position_query(sia_service):
    """Test position-based query using official method."""
    print("\n🔍 Testing position query...")

    try:
        # Use coordinates near ECDFS as in the tutorial
        target_ra = 53.076
        target_dec = -28.110

        # Define a circle search region (ra, dec, radius in degrees)
        circle = (target_ra, target_dec, 0.05)

        print(f"   Search position: RA={target_ra}, Dec={target_dec}, Radius=0.05 deg")

        # Execute search
        import time
        start_time = time.time()
        results = sia_service.search(pos=circle, maxrec=3)
        search_time = time.time() - start_time

        print(f"✅ SIAv2 search completed in {search_time:.2f} seconds")
        print(f"   Found {len(results)} results")

        return results

    except Exception as e:
        print(f"❌ Position query failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        return []

def test_calibration_query(sia_service):
    """Test calibration level query."""
    print("\n🔍 Testing calibration level query...")

    try:
        from lsst.rsp.utils import get_pyvo_auth
        from pyvo.dal.adhoc import DatalinkResults

        # Use the same search parameters
        target_ra = 53.076
        target_dec = -28.110
        circle = (target_ra, target_dec, 0.05)

        print("   Querying for visit images (calib_level=2)...")
        results = sia_service.search(pos=circle, calib_level=2)
        print(f"   Found {len(results)} visit images")

        if len(results) > 0:
            print("   Testing DataLink access...")
            datalink_url = results[0].access_url
            print(f"   DataLink URL: {datalink_url[:80]}...")

            # Test DataLink access with proper authentication
            try:
                dl_result = DatalinkResults.from_result_url(
                    datalink_url,
                    session=get_pyvo_auth()
                )
                print(f"   ✅ DataLink access successful! Found {len(dl_result)} records")

                # Get the first image URL
                if len(dl_result) > 0:
                    image_url = dl_result.getrecord(0).get('access_url')
                    print(f"   Image URL: {image_url[:80]}...")
                    return results

            except Exception as e:
                print(f"   ❌ DataLink access failed: {e}")

        return results

    except Exception as e:
        print(f"❌ Calibration query failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        return []

def analyze_results(results):
    """Analyze the SIAv2 search results."""
    print(f"\n📊 Analyzing {len(results)} results...")

    if not results:
        print("   ❌ No results found")
        return

    # Convert to table for analysis
    try:
        table = results.to_table()
        print(f"   Table columns: {table.colnames}")

        # Show some basic info
        if 'dataproduct_subtype' in table.colnames:
            subtypes = {}
            for subtype in table['dataproduct_subtype']:
                subtypes[subtype] = subtypes.get(subtype, 0) + 1
            print(f"   Data product types: {dict(subtypes)}")

        if 'lsst_band' in table.colnames:
            bands = {}
            for band in table['lsst_band']:
                bands[band] = bands.get(band, 0) + 1
            print(f"   Bands: {dict(bands)}")

        # Show first few results
        print(f"   First 3 results:")
        for i in range(min(3, len(results))):
            result = results[i]
            print(f"     {i+1}. {getattr(result, 'obs_title', 'Unknown title')}")
            print(f"        Type: {getattr(result, 'dataproduct_subtype', 'Unknown')}")
            print(f"        Band: {getattr(result, 'lsst_band', 'Unknown')}")

    except Exception as e:
        print(f"   ❌ Analysis failed: {e}")

def main():
    """Main test function."""
    print("=" * 70)
    print("🔭 Official SIAv2 DP1 Access Test")
    print("=" * 70)

    # Step 1: Setup environment
    success = setup_environment()
    if not success:
        return 1

    # Step 2: Initialize SIAv2 service
    sia_service = test_official_siav2()
    if not sia_service:
        return 1

    # Step 3: Test basic position query
    results = test_position_query(sia_service)
    if not results:
        print("   ⚠️ No results from position query, trying other queries...")

    # Step 4: Test calibration level query with DataLink
    if not results:
        results = test_calibration_query(sia_service)

    # Step 5: Analyze results
    analyze_results(results)

    # Final summary
    print("\n" + "=" * 70)
    print("🎉 Official SIAv2 DP1 Access Test Completed!")
    print("=" * 70)

    if results:
        print(f"✅ Official SIAv2 method is working!")
        print(f"   Successfully found {len(results)} DP1 images")
        print(f"   Using the official Rubin Science Platform libraries")
        print(f"   ✅ This confirms SIAv2 is available when using the correct method")
        print(f"\n💡 Key Differences from our approach:")
        print(f"   • Uses get_siav2_service('dp1') for service discovery")
        print(f"   • Uses get_pyvo_auth() for authentication")
        print(f"   • Handles service capabilities and endpoints automatically")
        return 0
    else:
        print(f"❌ No results found with official method either")
        print(f"   This may indicate authentication or service access issues")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)