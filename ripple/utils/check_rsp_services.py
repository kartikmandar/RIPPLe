#!/usr/bin/env python3
"""
RSP Service Status Checker

This script checks the status of RSP TAP and SIAv2 services
and provides recommendations for service usage.
"""

import os
import sys
from pathlib import Path

# Add the ripple module to Python path
sys.path.insert(0, str(Path(__file__).parent))

from ripple.data_access.service_monitor import create_service_monitor
from ripple.data_access.rsp_tap_client import create_rsp_client


def main():
    """Main function to check RSP service status."""
    print("="*70)
    print("RIPPLe - RSP Service Status Checker")
    print("="*70)

    # Check for access token
    access_token = os.environ.get("RSP_ACCESS_TOKEN")
    if not access_token:
        print("❌ RSP_ACCESS_TOKEN environment variable not set")
        print("Please set your RSP access token and try again")
        print("You can get a token from: https://data.lsst.cloud/")
        return 1

    print(f"✓ Found access token: {access_token[:20]}...")
    print()

    # Check service status with monitor
    print("Checking service availability...")
    monitor = create_service_monitor()
    monitor.print_service_status()

    # Get recommendations
    recommendations = monitor.get_service_recommendations()
    print("\nUsage Recommendations:")
    print("-" * 40)

    for i, rec in enumerate(recommendations['recommendations'], 1):
        print(f"{i}. {rec}")

    if recommendations['workarounds']:
        print("\nAvailable Workarounds:")
        for workaround in recommendations['workarounds']:
            print(f"• {workaround}")

    # Test RSP client initialization
    print("\n" + "="*70)
    print("Testing RSP Client Initialization")
    print("="*70)

    try:
        client = create_rsp_client(access_token=access_token)
        print("\n✓ RSP client created successfully")

        # Test TAP service
        if client.tap_service:
            print("✓ TAP service initialized and available")
            try:
                # Quick TAP test
                tables = client.list_available_tables()[:5]
                print(f"✓ TAP query successful - found {len(tables)} tables")
            except Exception as e:
                print(f"⚠️  TAP query test failed: {e}")
        else:
            print("✗ TAP service not available")

        # Test SIAv2 service
        if client.sia_service:
            print("✓ SIAv2 service initialized and available")
            try:
                # Quick SIAv2 test
                images = client.search_images(62.0, -37.0, max_results=1)
                print(f"✓ SIAv2 search successful - found {len(images)} images")
            except Exception as e:
                print(f"⚠️  SIAv2 search test failed: {e}")
        else:
            print("✗ SIAv2 service not available")

    except Exception as e:
        print(f"✗ Failed to create RSP client: {e}")
        return 1

    print("\n" + "="*70)
    print("Service Status Check Complete")
    print("="*70)

    # Return exit code based on service availability
    if client.tap_service or client.sia_service:
        print("✓ At least one service is available - pipeline should work")
        return 0
    else:
        print("✗ No services available - pipeline will have limited functionality")
        return 1


if __name__ == "__main__":
    sys.exit(main())