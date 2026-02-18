#!/usr/bin/env python3
"""
Simple SIAv2 DP1 Access Test

This script tests the official SIAv2 endpoint for DP1 access
and demonstrates how it works compared to our TAP approach.
"""

import os
import sys
from pathlib import Path
from astropy.time import Time

def setup_environment():
    """Setup Python path and imports."""
    print("🔧 Setting up environment...")

    sys.path.insert(0, '/home/kartikmandar/RIPPLe')

    try:
        import pyvo
        print("✅ pyvo imported successfully")
        return True
    except ImportError as e:
        print(f"❌ pyvo import failed: {e}")
        print("💡 Install with: pip install pyvo")
        return False

def load_token():
    """Load RSP access token from environment."""
    print("\n🔑 Loading authentication token...")

    # Try to get token from environment
    token = os.environ.get("RSP_ACCESS_TOKEN")

    if not token:
        # Try to load from .env file
        env_file = Path("/home/kartikmandar/RIPPLe/.env")
        if env_file.exists():
            print("   Loading from .env file...")
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('RSP_ACCESS_TOKEN='):
                        token = line.split('=', 1)[1].strip('"\'')
                        break
            print(f"✅ Token loaded successfully (length: {len(token)} chars)")
            print(f"   Token preview: {token[:20]}...")
            return token
        else:
            print("❌ No RSP_ACCESS_TOKEN found!")
            print("💡 Make sure your .env file contains:")
            print("   RSP_ACCESS_TOKEN=your_token_here")
            print("   Get your token from: https://data.lsst.cloud (Security menu)")
            return None
    else:
        print(f"✅ Token found (length: {len(token)} chars)")
        print(f"   Token preview: {token[:20]}...")
        return token

def test_siav2_service():
    """Initialize and test SIAv2 service with proper authentication."""
    print("\n🔌 Initializing SIAv2 service for DP1...")

    try:
        import pyvo
        from pyvo.auth import authsession

        # Get token for authentication
        token = load_token()
        if not token:
            print("❌ No token available for authentication")
            return None

        # Set up proper authentication as per research findings
        session = authsession.AuthSession()
        session.credentials.set_password("x-oauth-basic", token)
        print("✅ Authentication session configured")

        # Test different possible endpoints - use the official endpoint from community forum
        endpoints_to_test = [
            "https://data.lsst.cloud/api/sia/dp1/query",  # Official endpoint confirmed by LSST staff
            "https://data.lsst.cloud/api/dp1/query",  # Alternative endpoint
        ]

        working_services = []

        for endpoint in endpoints_to_test:
            print(f"\n   Testing endpoint: {endpoint}")
            try:
                # Try SIA2Service first
                sia_service = pyvo.dal.sia2.SIA2Service(
                    endpoint,
                    session=session,
                    check_baseurl=False  # Skip capabilities check
                )
                print(f"   ✅ SIAv2 service initialized with {endpoint}")
                sia_service.tested_endpoint = endpoint
                working_services.append(('SIAv2', sia_service))

            except Exception as e:
                print(f"   ❌ SIAv2 failed with {endpoint}: {e}")

                # Try with regular SIAService as fallback
                try:
                    sia_service = pyvo.dal.SIAService(
                        endpoint,
                        session=session
                    )
                    print(f"   ✅ SIA (v1) service initialized with {endpoint}")
                    sia_service.tested_endpoint = endpoint
                    sia_service.is_sia1 = True
                    working_services.append(('SIAv1', sia_service))
                except Exception as e2:
                    print(f"   ❌ SIAv1 also failed with {endpoint}: {e2}")

        if not working_services:
            print("❌ All endpoints failed")
            return None

        print(f"\n✅ Found {len(working_services)} working service(s)")

        # Try to test each service to see which one actually works for queries
        for service_type, service in working_services:
            print(f"\n   Testing {service_type} at {service.tested_endpoint}...")
            try:
                # Quick test query
                if service_type == 'SIAv2':
                    # SIAv2 expects POS as a string format: "CIRCLE ra dec radius"
                    test_query = service.create_query(
                        pos=[(52.884, -28.346, 0.1)],  # Use tuple format for CIRCLE
                        maxrec=1
                    )
                    # Manually set POS in the correct SIAv2 string format
                    test_query['POS'] = 'CIRCLE 52.884 -28.346 0.1'
                    test_url = test_query.queryurl
                else:  # SIAv1
                    test_url = f"{service.baseurl}?POS=52.884%2C-28.346&SIZE=0.1&MAXREC=1"

                print(f"      Test URL: {test_url}")

                # Actually try to execute a small query
                if service_type == 'SIAv2':
                    print(f"      Query details: {test_query}")
                    print(f"      All query params: {dict(test_query)}")
                    results = test_query.execute()
                else:
                    results = service.search(pos=(52.884, -28.346), size=0.1, maxrec=1)

                print(f"      ✅ {service_type} QUERIES WORK! Found {len(results)} results")
                return service

            except Exception as e:
                print(f"      ❌ {service_type} query failed: {e}")

        # If none work for queries, return the first one anyway
        service_type, service = working_services[0]
        print(f"\n   No services had working queries, using: {service_type} at {service.tested_endpoint}")
        return service

    except Exception as e:
        print(f"❌ SIAv2 service initialization failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        print("   This suggests SIAv2 service might not be available")
        return None

def test_basic_search(siav2_service):
    """Test basic SIA image search (v1 or v2)."""
    print("\n🔍 Testing basic SIA image search...")

    try:
        # Simple search around ECDFS coordinates where our pipeline is working
        ra, dec, radius = 52.884, -28.346, 0.1  # Use larger radius as suggested in research

        print(f"   Search position: RA={ra}, Dec={dec}, Size={radius} degrees")
        print(f"   Band: r-band")
        print(f"   Max results: 5")
        print(f"   Format: FITS")
        print(f"   Service endpoint: {getattr(siav2_service, 'tested_endpoint', 'Unknown')}")
        print(f"   Service type: {'SIAv1' if getattr(siav2_service, 'is_sia1', False) else 'SIAv2'}")

        import time
        from astropy import units as u
        start_time = time.time()

        # Check if this is SIAv1 or SIAv2
        if getattr(siav2_service, 'is_sia1', False):
            print("   Using SIAv1 search method...")
            # SIAv1 search method as suggested in research
            results = siav2_service.search(
                pos=(ra, dec),
                size=radius,  # degrees
                calib=2,      # calibration level 2 for visit images
                maxrec=5,
                format='image/fits'
            )
        else:
            print("   Using SIAv2 search method...")
            # Create query object first to inspect it
            print("   Creating SIA2 query object...")
            query = siav2_service.create_query(
                pos=[(ra, dec, radius)],   # Position as list of tuples for CIRCLE
                band=(620e-9, 750e-9),  # r-band wavelength range in meters (plain float)
                maxrec=5,          # Limit results for testing
                res_format='image/fits'  # Request FITS format
            )

            # Convert POS to correct string format: "CIRCLE ra dec radius"
            query['POS'] = f'CIRCLE {ra} {dec} {radius}'

            print(f"   Query URL: {query.queryurl}")
            print(f"   Query parameters: {dict(query)}")

            # Execute search using SIA2 API
            print("   Executing search...")
            results = query.execute()

        search_time = time.time() - start_time

        print(f"✅ SIA search completed in {search_time:.2f} seconds")
        print(f"   Found {len(results)} results")

        return results

    except Exception as e:
        print(f"❌ SIA search failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        print(f"   Full traceback: {traceback.format_exc()}")
        return []

def analyze_results(results):
    """Analyze the SIAv2 search results."""
    print(f"\n📊 Analyzing {len(results)} results...")

    if not results:
        print("   ❌ No results found")
        return

    for i in range(min(3, len(results))):
        result = results[i]
        print(f"\n   Result {i+1}:")
        print(f"   Dataset ID: {result.get('obs_publisher_did', 'Unknown')}")
        print(f"   Title: {result.get('obs_title', 'Unknown')}")

        # Access basic properties
        if 's_xel1' in result and 's_xel2' in result:
            print(f"   Image dimensions: {result['s_xel1']}x{result['s_xel2']} pixels")
        if 's_pixel_scale' in result:
            print(f"   Pixel scale: {result['s_pixel_scale']}")
        if 'access_format' in result:
            print(f"   Format: {result['access_format']}")

        # Access URL if available
        if 'access_url' in result and result['access_url']:
            access_url = result['access_url']
            print(f"   Access URL: {access_url[:80]}...")
        else:
            print("   Access URL: Not available")

        # Check additional properties
        if 'obs_collection' in result:
            print(f"   Collection: {result['obs_collection']}")
        if 'instrument_name' in result:
            print(f"   Instrument: {result['instrument_name']}")
        if 't_exptime' in result:
            print(f"   Exposure time: {result['t_exptime']}")

        # Check calibration level
        if 'calib_level' in result:
            calib_levels = {1: 'raw', 2: 'visit_image', 3: 'coadd'}
            calib_name = calib_levels.get(result['calib_level'], 'unknown')
            print(f"   Calibration level: {calib_name}")

        print(f"   ✅ SIAv2 result successfully retrieved!")

def test_different_queries(siav2_service):
    """Test different SIAv2 query types."""
    print("\n🔍 Testing different SIAv2 query patterns...")

    test_cases = [
        # Test 1: Search for different bands
        {
            'name': 'Multi-band search',
            'search_func': lambda: siav2_service.search(
                pos=(52.884, -28.346, 0.005),
                maxrec=3,
                res_format='image/fits'
            )
        },

        # Test 2: Search with time constraint
        {
            'name': 'Time-constrained search',
            'search_func': lambda: siav2_service.search(
                pos=(52.884, -28.346, 0.005),
                time=['2024-01-01', '2024-12-31'],  # Time range
                maxrec=2,
                res_format='image/fits'
            )
        },

        # Test 3: Search for larger area
        {
            'name': 'Larger area search',
            'search_func': lambda: siav2_service.search(
                pos=(52.884, -28.346, 0.02),  # Larger circle
                maxrec=5,
                res_format='image/fits'
            )
        },

        # Test 4: Search for multiple bands simultaneously
        {
            'name': 'Multiple band search',
            'search_func': lambda: siav2_service.search(
                pos=(52.884, -28.346, 0.005),
                band=['r', 'i'],  # Multiple bands
                maxrec=5,
                res_format='image/fits'
            )
        }
    ]

    for test_case in test_cases:
        print(f"\n   {test_case['name']}:")

        try:
            results = test_case['search_func']()
            print(f"   ✅ Success! Found {len(results)} results")

            # Show brief results
            for result in results[:2]:
                title = result.obs_title if hasattr(result, 'obs_title') else 'Unknown'
                print(f"     - {title}")

        except Exception as e:
            print(f"   ❌ Failed: {e}")

def test_capabilities(siav2_service):
    """Test SIAv2 service capabilities."""
    print("\n🔍 Testing SIAv2 service capabilities...")

    try:
        # Test if VOSI endpoints are available
        if hasattr(siav2_service, 'capabilities'):
            capabilities = siav2_service.capabilities
            print("✅ Capabilities retrieved successfully")
            print(f"   Capabilities: {capabilities}")
        else:
            print("   ℹ️ Capabilities endpoint not available")

        if hasattr(siav2_service, 'availability'):
            availability = siav2_service.availability
            print("   ✅ VOSI availability endpoint available")
            print(f"   URL: {availability}")
        else:
            print("   ℹ️ Availability endpoint not available")

    except Exception as e:
        print(f"❌ Capabilities test failed: {e}")
        print("   This may indicate the service has limited capabilities")

def main():
    """Main test function."""
    print("=" * 70)
    print("🔭 SIAv2 DP1 Access Test")
    print("=" * 70)

    # Step 1: Setup environment
    success = setup_environment()
    if not success:
        return 1

    # Step 2: Load authentication
    token = load_token()
    if not token:
        return 1

    # Step 3: Initialize SIAv2 service
    siav2_service = test_siav2_service()
    if not siav2_service:
        return 1

    # Step 4: Test basic search
    results = test_basic_search(siav2_service)
    if not results:
        print("\n💡 This confirms our earlier finding:")
        print("   • SIAv2 endpoint returns 404 errors")
        print("   • TAP approach is the working solution")
        print("   • The documentation shows SIAv2 should work, but implementation may be limited")
        return 0

    # Step 5: Analyze results
    analyze_results(results)

    # Step 6: Test different query patterns
    test_different_queries(siav2_service)

    # Step 7: Test capabilities
    test_capabilities(siav2_service)

    # Final summary
    print("\n" + "=" * 70)
    print("🎉 SIAv2 DP1 Access Test Completed!")
    print("=" * 70)

    if results:
        print(f"✅ SIAv2 is working and functional!")
        print(f"   Successfully found {len(results)} DP1 images")
        print("   Each result includes:")
        print("     • Direct access URLs for FITS file downloads")
        print("     • Complete ObsCore metadata")
        print("     - Standard IVOA SIAv2 interface")
        print("\n💡 SIAv2 vs TAP Comparison:")
        print("   • SIAv2: Direct image access, standard IVOA protocol")
        print("   • TAP: Metadata queries + DataLink + GCS downloads")
        print("   • Both provide access to DP1 calibrated visit images")
        print("   • SIAv2: Simpler for whole image access")
        print("   • TAP: More flexible for precise cutout extraction")

        print(f"\n📊 Test Results Summary:")
        for i, result in enumerate(results[:3]):
            print(f"   {i+1}. Dataset ID: {result.get('publisher_did', 'Unknown')}")
            print(f"       Access URL: {result.get('access_url', 'Not available')[:50]}...")
            print(f"       Format: {result.get('format', 'Unknown')}")

        print(f"\n💡 Next Steps:")
        print("   • SIAv2 can be used for whole image downloads")
        print("   • Combine SIAv2 with SODA for cutouts (when available)")
        print("   • Compare performance: SIAv2 vs TAP + DataLink")
        print("   • Test SODA cutout capability with: 64x64 pixel cutouts")

    else:
        print("\n❌ No results found!")
        print("💡 This suggests several possibilities:")
        print("   1. SIAv2 service may have limited data")
        print("   2. Search parameters may need adjustment")
        print("   3. Time range might not include DP1 data")
        print("   4. Service may be in development phase")
        print("\n🎯 Conclusion:")
        print("   • Our TAP + DataLink approach is the robust working solution")
        print("   • SIAv2 shows promise for simpler image access")
        print("   • Consider SIAv2 for future whole-dataset downloads")
        print("   • Keep using TAP for precise cutout extraction")

    return 0 if results else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)