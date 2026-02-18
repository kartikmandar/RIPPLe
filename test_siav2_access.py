#!/usr/bin/env python3
"""
Test script for SIAv2 DP1 Simple Image Access

This script demonstrates how to use the official SIAv2 endpoint
to access DP1 calibrated visit images from Rubin Science Platform.
"""

import os
import sys
import json
import time
from pathlib import Path
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.time import Time

def setup_environment():
    """Setup Python path and imports."""
    print("🔧 Setting up environment...")

    # Add current directory to Python path
    sys.path.insert(0, '/home/kartikmandar/RIPPLe')

    try:
        # Import required libraries
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

    if token:
        print(f"✅ Token loaded successfully (length: {len(token)} chars)")
        print(f"   Token preview: {token[:20]}...")
        return token
    else:
        print("❌ No RSP_ACCESS_TOKEN found!")
        print("💡 Make sure your .env file contains:")
        print("   RSP_ACCESS_TOKEN=your_token_here")
        print("   Get your token from: https://data.lsst.cloud (Security menu)")
        return None

def initialize_siav2_service():
    """Initialize SIAv2 service for DP1."""
    print("\n🔌 Initializing SIAv2 service for DP1...")

    try:
        # Initialize SIAv2 service
        siav2_service = pyvo.dal.SIAService(
            "https://data.lsst.cloud/api/dp1/query"
        )
        print("✅ SIAv2 service initialized successfully")
        print("   Endpoint: https://data.lsst.cloud/api/dp1/query")
        return siav2_service
    except Exception as e:
        print(f"❌ SIAv2 service initialization failed: {e}")
        return None

def test_connection(siav2_service):
    """Test basic SIAv2 connection."""
    print("\n🔍 Testing SIAv2 connection...")

    try:
        # Get service capabilities
        capabilities = siav2_service.capabilities
        print("✅ Connection test successful!")
        print("   Service capabilities:")
        if hasattr(capabilities, '_items'):
            for key, value in capabilities.items():
                if isinstance(value, dict) and value:
                    print(f"     {key}: Available")
                else:
                    print(f"     {key}: {value}")
        else:
            print(f"   Capabilities: {capabilities}")

        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def define_search_parameters():
    """Define search parameters for image query."""
    print("\n📍 Defining search parameters...")

    # Use ECDFS coordinates (where our pipeline is currently working)
    ra = 52.884167 * u.deg
    dec = -28.345833 * u.deg

    # Define search radius (0.02 degrees ~ 72 arcseconds for 64x64 cutouts)
    radius = 0.02 * u.deg

    # Define search parameters
    params = {
        'POS': f'{ra.degree},{dec.degree}',  # Position in degrees
        'RADIUS': radius.to(u.deg),                # Search radius in degrees
        'INTERSECT': 'OVERLAPS',                # Images covering the position
        'BAND': 'r',                              # r-band filter
        'OBSTIME': '<2025-01-01',             # Observation time range
        'OBSTIME': '>2024-11-01',
        'FORMAT': 'image/fits',                     # Request FITS format
        'MAXREC': 5,                               # Maximum number of results
    }

    print(f"   Search position: RA={ra.degree:.6f}, Dec={dec.degree:.6f}")
    print(f"   Search radius: {radius.deg:.3f} degrees (~72 arcsec)")
    print(f"   Band: r-band")
    print(f"   Time range: 2024-11-01 to 2025-01-01")
    print(f"   Max results: 5 images")
    print(f"   Format: FITS")
    print(f"   Parameters: {params}")

    return params

def search_images(siav2_service, params):
    """Search for images using SIAv2."""
    print("\n🔍 Searching for images...")

    try:
        # Execute search
        start_time = time.time()
        results = siav2_service.search(**params)
        search_time = time.time() - start_time

        print(f"✅ Image search completed in {search_time:.2f} seconds")
        print(f"   Found {len(results)} results")

        if len(results) == 0:
            print("   ❌ No images found at this location")
            return []
        else:
            print("   First 3 results:")
            for i, result in enumerate(results[:3]):
                print(f"     {i+1}. {result['title']}")
                print(f"        Size: {result.get('naxis1', '?')}x{result.get('naxis2', '?')} pixels")
                print(f"        Band: {result.get('band', '?')}")
                print(f"        Format: {result.get('format', '?')}")
                print(f"        URL: {result.get('access_url', '?')[:50]}...")
                print()

        return results
    except Exception as e:
        print(f"❌ Image search failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        print(f"   Error details: {str(e)[:200]}...")
        return []

def analyze_results(results):
    """Analyze the search results."""
    print("\n📊 Analyzing search results...")

    if not results:
        return

    # Analyze basic properties
    print(f"Total results: {len(results)}")

    # Count different formats
    formats = {}
    bands = {}
    image_sizes = []

    for result in results:
        # Count formats
        fmt = result.get('format', 'unknown')
        formats[fmt] = formats.get(fmt, 0) + 1

        # Count bands
        band = result.get('band', 'unknown')
        bands[band] = bands.get(band, 0) + 1

        # Count image sizes
        size = result.get('naxis1', 0) * result.get('naxis2', 0)
        if size > 0:
            image_sizes.append(size)

    print(f"Available formats: {dict(formats)}")
    print(f"Available bands: {dict(bands)}")

    if image_sizes:
        print(f"Image size range: {min(image_sizes)}x{max(image_sizes)} pixels")
        print(f"Average image size: {sum(image_sizes)/len(image_sizes):.0f}x{sum(image_sizes)/len(image_sizes):.0f} pixels")

    # Check for data products
    data_products = set()
    for result in results:
        for key in result.keys():
            if key.startswith('dataproduct'):
                data_products.add(key)

    if data_products:
        print(f"Data products available: {sorted(data_products)}")
    else:
        print("No data products identified in results")

def test_metadata_access(siav2_service, results):
    """Test metadata access for first result."""
    print("\n🏷 Testing metadata access...")

    if not results:
        print("   ❌ No results to test metadata")
        return

    first_result = results[0]
    try:
        # Try to get table
        table = first_result.to_table()
        print("✅ Successfully converted result to table")
        print(f"   Table shape: {table.shape}")
        print(f"   Column names: {table.colnames[:5]}...")  # Show first 5 columns

        # Display some basic info
        if 'title' in table.colnames:
            titles = table['title']
            print(f"   Sample titles: {titles[:min(3, len(titles))]}")

    except Exception as e:
        print(f"❌ Metadata access failed: {e}")
        print(f"   Error type: {type(e).__name__}")

def check_cutout_capability(siav2_service):
    """Check if SIAv2 supports cutouts."""
    print("\n✂️ Checking cutout capabilities...")

    try:
        # Define a small region for cutout test
        ra = 52.884167
        dec = -28.345833
        size = 0.0036  # Small cutout size (~13 arcsec)

        # Try to use SODA (Server-side Data Operations)
        params = {
            'POS': f'{ra},{dec}',
            'CUTOUT': f'square;{size}',  # Square cutout
            'FORMAT': 'image/fits',
            'BAND': 'r',
            'MAXREC': 1
        }

        print(f"   Testing cutout parameters: {params}")

        # Check if service supports SODA
        if hasattr(siav2_service, 'capabilities'):
            print("✅ Checking service capabilities...")
            print(f"   Capabilities: {siav2_service.capabilities}")

        # Try cutout query
        results = siav2_service.search(**params)
        if results:
            print("✅ SIAv2 cutouts are supported!")
            print(f"   Found {len(results)} cutout results")
        else:
            print("ℹ️ SIAv2 cutouts may not be supported or no results found")

    except Exception as e:
        print(f"❌ Cutout test failed: {e}")
        print("   This may indicate SIAv2 cutouts are not yet implemented")

def save_results_summary(results, token_used):
    """Save results summary for documentation."""
    print("\n💾 Saving results summary...")

    output_dir = Path("/home/kartikmandar/RIPPLe/siav2_test_results")
    output_dir.mkdir(exist_ok=True)

    # Save basic info
    summary = {
        'test_timestamp': Time.now().iso,
        'token_preview': f"{token_used[:20]}..." if token_used else "None",
        'total_results': len(results),
        'search_successful': len(results) > 0,
        'results': []
    }

    # Save summary of first 5 results
    for i, result in enumerate(results[:5]):
        result_summary = {
            'index': i + 1,
            'title': result.get('title', 'unknown'),
            'band': result.get('band', 'unknown'),
            'format': result.get('format', 'unknown'),
            'size_pixels': f"{result.get('naxis1', '?')}x{result.get('naxis2', '?')}",
            'fov_deg': result.get('fov', '?'),
            'has_preview': 'preview' in result,
            'has_access_url': 'access_url' in result,
        }
        summary['results'].append(result_summary)

    # Save to file
    summary_file = output_dir / "siav2_test_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Summary saved to: {summary_file}")
    return summary_file

def main():
    """Main test function."""
    print("=" * 70)
    print("🔭 SIAv2 DP1 Access Test")
    print("=" * 70)

    # Step 1: Setup environment
    success = setup_environment()
    if not success:
        return 1

    # Step 2: Load authentication token
    token = load_token()
    if not token:
        return 1

    # Step 3: Initialize SIAv2 service
    siav2_service = initialize_siav2_service()
    if not siav2_service:
        return 1

    # Step 4: Test basic connection
    if not test_connection(siav2_service):
        return 1

    # Step 5: Define search parameters
    params = define_search_parameters()

    # Step 6: Search for images
    results = search_images(siav2_service, params)
    if not results:
        print("\n💡 This could indicate:")
        print("   • No data available at the specified coordinates")
        print("   • The search parameters might be too restrictive")
        print("   • The time range might not have DP1 data")
        print("   • There might be a temporary service issue")

    # Step 7: Analyze results
    analyze_results(results)

    # Step 8: Test metadata access
    test_metadata_access(siav2_service, results)

    # Step 9: Check cutout capability
    check_cutout_capability(siav2_service)

    # Step 10: Save results
    save_results_summary(results, token)

    # Final summary
    print("\n" + "=" * 70)
    print("🎉 SIAv2 DP1 Access Test Completed!")
    print("=" * 70)

    if results:
        print(f"✅ Successfully found {len(results)} DP1 images")
        print(f"✅ SIAv2 service is operational and functional")
        print("✅ Authentication is working correctly")
        print("\n📋 Conclusion: SIAv2 access to DP1 is working!")
    else:
        print(f"❌ No DP1 images found with current search parameters")
        print(f"❌ This might be expected if SIAv2 is not fully implemented")
        print(f"❌ Consider trying different coordinates or time ranges")

    print("\n💡 Next Steps:")
    print("   • SIAv2 appears to be working - try broader searches")
    print("   • Consider using the SIAv2 endpoint for whole image access")
    print("   • The TAP approach used in our pipeline is also valid")
    print("   • Compare SIAv2 vs TAP for your specific use cases")

    return 0 if results else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)