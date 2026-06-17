# SIAv2 DP1 Access Guide

This document provides a comprehensive guide to accessing LSST/Rubin Observatory DP1 data through the official SIAv2 (Simple Image Access version 2) endpoint, based on systematic research and debugging conducted in October 2025.

## Overview

SIAv2 is the IVOA standard for discovering and accessing astronomical image data. After extensive investigation, we successfully established working SIAv2 access to DP1 calibrated visit images using the official Rubin Science Platform endpoint.

## Key Findings

### 🔍 The Problem
- Initial attempts to access SIAv2 resulted in 404 errors
- Generic pyvo approaches failed with the RSP endpoint
- Documentation was inconsistent and scattered

### ✅ The Solution
- **Official SIAv2 Endpoint**: `https://data.lsst.cloud/api/sia/dp1/query`
- **Authentication**: Token-based using existing RSP access token
- **Parameter Format**: Specific string formatting required for POS and band parameters
- **Service Discovery**: Working with `check_baseurl=False` to bypass capabilities check

## Working Implementation

### 1. Authentication Setup

```python
import pyvo
from pyvo.auth import authsession
import os

def setup_siav2_service():
    """Initialize SIAv2 service with proper authentication"""

    # Load token from environment or .env file
    token = os.environ.get("RSP_ACCESS_TOKEN")
    if not token:
        # Load from .env file
        with open('.env', 'r') as f:
            for line in f:
                if line.startswith('RSP_ACCESS_TOKEN='):
                    token = line.split('=', 1)[1].strip('"\'')
                    break

    # Create authenticated session
    session = authsession.AuthSession()
    session.credentials.set_password("x-oauth-basic", token)

    # Initialize SIAv2 service
    sia_service = pyvo.dal.sia2.SIA2Service(
        "https://data.lsst.cloud/api/sia/dp1/query",
        session=session,
        check_baseurl=False  # Skip capabilities check
    )

    return sia_service
```

### 2. Critical Parameter Formatting

**The key breakthrough was discovering that SIAv2 requires specific parameter formats:**

```python
def create_siav2_query(siav2_service, ra, dec, radius):
    """Create a properly formatted SIAv2 query"""

    # Create query object first
    query = siav2_service.create_query(
        pos=[(ra, dec, radius)],           # Position as list of tuples for CIRCLE
        band=(620e-9, 750e-9),            # r-band wavelength range in meters (plain float)
        maxrec=5,                          # Limit results for testing
        res_format='image/fits'            # Request FITS format
    )

    # Convert POS to correct string format: "CIRCLE ra dec radius"
    query['POS'] = f'CIRCLE {ra} {dec} {radius}'

    return query
```

**Critical Parameter Requirements:**

1. **POS Parameter**: Must be a string in format `"CIRCLE ra dec radius"`
   - ❌ Wrong: `pos=[(52.884, -28.346, 0.1)]` (tuple format)
   - ✅ Right: `POS="CIRCLE 52.884 -28.346 0.1"` (string format)

2. **Band Parameter**: Must be plain float tuples (no astropy Quantity objects)
   - ❌ Wrong: `band=[520e-9*u.m, 750e-9*u.m]` (astropy units)
   - ✅ Right: `band=(520e-9, 750e-9)` (plain float in meters)

3. **Service Initialization**: Must disable capabilities check
   - ❌ Wrong: `SIA2Service(endpoint)` (default tries to check capabilities)
   - ✅ Right: `SIA2Service(endpoint, check_baseurl=False)` (bypass capabilities)

### 3. Complete Working Example

```python
#!/usr/bin/env python3
"""
Complete working SIAv2 DP1 access example
"""

import pyvo
from pyvo.auth import authsession
import os

def load_token():
    """Load RSP access token"""
    token = os.environ.get("RSP_ACCESS_TOKEN")
    if not token:
        with open('.env', 'r') as f:
            for line in f:
                if line.startswith('RSP_ACCESS_TOKEN='):
                    token = line.split('=', 1)[1].strip('"\'')
                    break
    return token

def test_siav2_access():
    """Test complete SIAv2 access workflow"""

    # 1. Setup authentication
    token = load_token()
    session = authsession.AuthSession()
    session.credentials.set_password("x-oauth-basic", token)

    # 2. Initialize service
    sia_service = pyvo.dal.sia2.SIA2Service(
        "https://data.lsst.cloud/api/sia/dp1/query",
        session=session,
        check_baseurl=False
    )
    print("✅ SIAv2 service initialized")

    # 3. Create properly formatted query
    ra, dec, radius = 52.884, -28.346, 0.1  # ECDFS coordinates

    query = sia_service.create_query(
        pos=[(ra, dec, radius)],
        band=(620e-9, 750e-9),  # r-band in meters
        maxrec=5,
        res_format='image/fits'
    )

    # 4. Fix POS parameter format
    query['POS'] = f'CIRCLE {ra} {dec} {radius}'

    # 5. Execute query
    print("Executing SIAv2 query...")
    results = query.execute()

    print(f"✅ Found {len(results)} results")

    # 6. Analyze results
    for i, result in enumerate(results[:3]):
        print(f"\nResult {i+1}:")
        print(f"  Dataset ID: {result.get('obs_publisher_did', 'Unknown')}")
        print(f"  Image size: {result.get('s_xel1', '?')}x{result.get('s_xel2', '?')} pixels")
        print(f"  Band: {result.get('band', 'Unknown')}")
        print(f"  Access URL: {result.get('access_url', 'Not available')[:50]}...")

    return results

if __name__ == "__main__":
    results = test_siav2_access()
```

## SIAv2 vs TAP + DataLink Comparison

### SIAv2 Advantages
- **Simpler Interface**: Direct image discovery and access
- **Standard IVOA Protocol**: Well-documented standard
- **Single Service**: One endpoint for both discovery and access
- **Whole Image Access**: Direct access to complete calibrated visit images

### TAP + DataLink Advantages
- **More Flexible**: Precise metadata queries and filtering
- **Cutout Support**: Server-side data extraction for smaller regions
- **Mature Implementation**: More thoroughly tested and documented
- **Additional Services**: Access to other DP1 data products

### Recommendation
- **Use SIAv2** for: Whole image downloads, simple positional searches
- **Use TAP + DataLink** for: Precise cutouts, complex metadata queries, custom region extraction

## Service Capabilities

### Working Endpoints
- **SIAv2 Query**: `https://data.lsst.cloud/api/sia/dp1/query` ✅
- **VOSI Capabilities**: Available through service
- **VOSI Availability**: Available through service

### Available Data Products
- **Calibration Level 2**: Visit-level calibrated images
- **Image Format**: FITS files (4608×4096 pixels from LSSTComCam)
- **Bands**: g, r, i filters available
- **Coverage**: DP1 survey areas

## Troubleshooting Guide

### Common Errors and Solutions

#### 1. "404 Not Found" Error
**Problem**: Using wrong endpoint
**Solution**: Use official endpoint `https://data.lsst.cloud/api/sia/dp1/query`

#### 2. "Unrecognized shape in POS string" Error
**Problem**: POS parameter in wrong format
**Solution**: Convert to string format: `POS="CIRCLE ra dec radius"`

#### 3. "could not convert string to float: 'm'" Error
**Problem**: Band parameter includes astropy units
**Solution**: Use plain float in meters: `band=(520e-9, 750e-9)`

#### 4. Service Initialization Fails
**Problem**: pyvo trying to check capabilities endpoint
**Solution**: Add `check_baseurl=False` parameter

#### 5. Authentication Issues
**Problem**: Missing or invalid token
**Solution**: Ensure valid RSP_ACCESS_TOKEN in environment or .env file

### Debugging Tips

1. **Enable Verbose Logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Inspect Query Parameters**:
   ```python
   print(f"Query URL: {query.queryurl}")
   print(f"Query parameters: {dict(query)}")
   ```

3. **Test Authentication**:
   ```python
   # Test token validity with a simple request
   test_url = "https://data.lsst.cloud/api/tap/sync"
   # Make authenticated request to verify token works
   ```

## Official Rubin Library Method

While our pyvo-based approach works perfectly, Rubin also provides official libraries:

```python
from lsst.rsp.service import get_siav2_service
from lsst.rsp.utils import get_pyvo_auth

# Official method (requires proper RSP environment)
sia_service = get_siav2_service("dp1")
```

**Note**: This method requires specific environment configuration (RSP_SITE_TYPE variable) and the full Rubin software stack installation.

## Key Technical Details

### Authentication
- **Method**: Token-based authentication
- **Token Source**: RSP_ACCESS_TOKEN environment variable or .env file
- **Format**: Bearer token in HTTP Authorization header

### Coordinate System
- **System**: ICRS (J2000)
- **Units**: Decimal degrees for RA/Dec
- **Search Region**: Circular search with radius in degrees

### Band Specifications
- **g-band**: ~400-550nm (400e-9 to 550e-9 meters)
- **r-band**: ~550-700nm (550e-9 to 700e-9 meters)
- **i-band**: ~700-850nm (700e-9 to 850e-9 meters)

### Image Properties
- **Dimensions**: 4608×4096 pixels (LSSTComCam)
- **Pixel Scale**: 0.2 arcseconds/pixel
- **Field of View**: ~15.4×13.6 arcminutes
- **Format**: FITS files with full WCS headers

## Testing and Validation

### Test Coordinates
We successfully tested SIAv2 access using ECDFS coordinates:
- **RA**: 52.884°
- **Dec**: -28.346°
- **Radius**: 0.1° (for testing)

### Expected Results
Successful SIAv2 queries should return:
- 5+ DP1 calibrated visit images
- Complete ObsCore metadata
- Direct access URLs for FITS downloads
- Image dimensions and band information

## Future Development

### Potential Enhancements
1. **SODA Integration**: Server-side cutouts when SODA service is available
2. **Batch Queries**: Efficient querying of multiple positions
3. **Caching**: Local caching of metadata and results
4. **Async Processing**: Background processing for large queries

### Integration with RIPPLe Pipeline
SIAv2 can be integrated into the existing RIPPLe pipeline as an alternative to the TAP + DataLink approach for certain use cases:

```python
# Example integration point in data_source_stage.py
def fetch_with_siav2(coordinates, bands):
    """Alternative data fetch method using SIAv2"""
    sia_service = setup_siav2_service()
    results = []

    for ra, dec in coordinates:
        query = create_siav2_query(sia_service, ra, dec, 0.01)
        images = query.execute()
        results.extend(images)

    return results
```

## References and Resources

### Official Documentation
- [Rubin DP1 Documentation](https://dp1.lsst.io)
- [IVOA SIAv2 Standard](http://www.ivoa.net/documents/SIA/)
- [pyvo Documentation](https://pyvo.readthedocs.io/)

### Community Resources
- [Rubin Community Forum](https://community.lsst.org)
- [Rubin Data Lab Documentation](https://datalab.lsst.cloud)

### Working Examples
- Complete test script: `/home/kartikmandar/RIPPLe/test_siav2_simple.py`
- Official method test: `/home/kartikmandar/RIPPLe/test_siav2_official_method.py`

## Summary

SIAv2 access to DP1 data is fully functional when using the correct:
1. **Endpoint**: `https://data.lsst.cloud/api/sia/dp1/query`
2. **Authentication**: Valid RSP access token
3. **Parameter Formatting**: Specific string and float formats
4. **Service Configuration**: `check_baseurl=False`

This implementation provides a robust, standard-compliant method for accessing DP1 calibrated visit images that complements the existing TAP + DataLink approach used in the RIPPLe pipeline.

---

**Last Updated**: 2025-10-25
**Status**: ✅ Fully Working Implementation
**Tested With**: Python 3.11, pyvo 1.5, RSP DP1 data