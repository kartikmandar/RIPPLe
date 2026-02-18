#!/usr/bin/env python3
"""
Debug script for Rubin SIAv2 endpoint
"""

import os
import requests
import json
from pathlib import Path

# Load environment variables from .env file
if Path('.env').exists():
    with open('.env') as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

def test_sia_endpoints():
    """Test different possible SIAv2 endpoints."""

    # Get access token
    access_token = os.environ.get("RSP_ACCESS_TOKEN")
    if not access_token:
        print("❌ No RSP_ACCESS_TOKEN found in environment")
        return

    print(f"✓ Found access token: {access_token[:20]}...")

    # Possible endpoints to test
    endpoints = [
        "https://data.lsst.cloud/api/dp02/query",
        "https://data.lsst.cloud/api/dp02/sync",
        "https://data.lsst.cloud/api/sia/query",
        "https://data.lsst.cloud/api/sia/v2/query",
        "https://data.lsst.cloud/sia/query",
        "https://data.lsst.cloud/sia/v2/query"
    ]

    # Headers for authentication
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json, application/xml, text/plain'
    }

    # Test parameters
    test_params = {
        'POS': '62.0,-37.0;0.1',
        'BAND': 'i',
        'MAXREC': '5',
        'INSTRUMENT': 'LSSTCam'
    }

    print("\n" + "="*60)
    print("Testing SIAv2 Endpoints")
    print("="*60)

    for endpoint in endpoints:
        print(f"\n🔍 Testing endpoint: {endpoint}")

        try:
            # Test GET request first
            response = requests.get(endpoint, headers=headers, timeout=10)
            print(f"   GET  Status: {response.status_code}")

            if response.status_code == 200:
                print(f"   ✓ GET works! Content-Type: {response.headers.get('content-type', 'unknown')}")
                print(f"   Response length: {len(response.text)} characters")

            elif response.status_code == 404:
                print(f"   ❌ GET - Not Found")

            else:
                print(f"   ⚠️  GET - {response.status_code}: {response.text[:100]}...")

            # Test POST request
            response = requests.post(endpoint, data=test_params, headers=headers, timeout=10)
            print(f"   POST Status: {response.status_code}")

            if response.status_code == 200:
                print(f"   ✓ POST works! Content-Type: {response.headers.get('content-type', 'unknown')}")
                print(f"   Response length: {len(response.text)} characters")

                # Try to parse response
                try:
                    if 'json' in response.headers.get('content-type', '').lower():
                        data = response.json()
                        print(f"   ✓ JSON response parsed successfully")
                        print(f"   Keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    elif 'xml' in response.headers.get('content-type', '').lower():
                        print(f"   ✓ XML response received")
                    else:
                        print(f"   Response preview: {response.text[:200]}...")
                except Exception as e:
                    print(f"   ⚠️  Response parsing failed: {e}")

            elif response.status_code == 404:
                print(f"   ❌ POST - Not Found")

            elif response.status_code == 405:
                print(f"   ⚠️  POST - Method Not Allowed")

            elif response.status_code == 400:
                print(f"   ⚠️  POST - Bad Request: {response.text[:100]}...")

            elif response.status_code == 401:
                print(f"   ❌ POST - Unauthorized")

            elif response.status_code == 403:
                print(f"   ❌ POST - Forbidden")

            else:
                print(f"   ⚠️  POST - {response.status_code}: {response.text[:100]}...")

        except requests.exceptions.Timeout:
            print(f"   ⏰ Request timed out")
        except requests.exceptions.ConnectionError:
            print(f"   🔌 Connection error")
        except Exception as e:
            print(f"   ❌ Error: {e}")

def test_alternative_formats():
    """Test alternative request formats."""

    access_token = os.environ.get("RSP_ACCESS_TOKEN")
    if not access_token:
        return

    print("\n" + "="*60)
    print("Testing Alternative Request Formats")
    print("="*60)

    endpoint = "https://data.lsst.cloud/api/dp02/query"

    # Test JSON format
    print(f"\n🔍 Testing JSON POST to: {endpoint}")

    headers_json = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    json_params = {
        'POS': '62.0,-37.0;0.1',
        'BAND': 'i',
        'MAXREC': 5,
        'INSTRUMENT': 'LSSTCam'
    }

    try:
        response = requests.post(endpoint, json=json_params, headers=headers_json, timeout=10)
        print(f"   JSON POST Status: {response.status_code}")

        if response.status_code == 200:
            print(f"   ✓ JSON POST works!")
        else:
            print(f"   ❌ JSON POST failed: {response.text[:100]}...")

    except Exception as e:
        print(f"   ❌ JSON POST error: {e}")

if __name__ == "__main__":
    test_sia_endpoints()
    test_alternative_formats()
    print("\n" + "="*60)
    print("Debug testing completed")
    print("="*60)