# Using RSP Token Authentication with RIPPLe

This guide explains how to use the enhanced ButlerClient with Rubin Science Platform (RSP) token authentication.

## Prerequisites

1. **RSP Account**: You need an account on the Rubin Science Platform (https://data.lsst.cloud/)
2. **Access Token**: Generate an access token from the RSP web interface

## Getting Your RSP Access Token

1. Log in to https://data.lsst.cloud/
2. Click on your user profile (upper right corner)
3. Select "Security tokens" from the dropdown menu
4. Click "Create Token"
5. Configure your token:
   - **Name**: Choose a descriptive name (e.g., "RIPPLe Pipeline")
   - **Scopes**: Select the appropriate scopes:
     - `read:image` - For image access
     - `read:tap` - For catalog queries
     - `exec:notebook` - For notebook access (if needed)
   - **Expiration**: Choose an appropriate expiration period
6. Click "Create" and **copy the token immediately** (it will only be shown once)

## Setting Up Your Environment

### Option 1: Using .env file (Recommended)

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit the `.env` file and add your token:
   ```
   RSP_ACCESS_TOKEN=your_actual_rsp_access_token_here
   ```

3. The test script will automatically load this token

### Option 2: Environment Variable

Set the environment variable directly:
```bash
export RSP_ACCESS_TOKEN=your_actual_rsp_access_token_here
```

## Usage Examples

### Basic RSP Connection

```python
from ripple.data_access import ButlerClient, ButlerConfig

# Method 1: Using environment variable
from ripple.data_access import get_rsp_config
config = get_rsp_config()
client = ButlerClient(config=config)

# Method 2: Manual configuration
config = ButlerConfig(
    server_url="https://data.lsst.cloud/api/butler/",
    access_token="your_token_here",
    token_username="x-oauth-basic",
    auth_method="token",
    collections=["2.2i/runs/DP0.2"],
    instrument="LSSTCam"
)
client = ButlerClient(config=config)

# Test the connection
if client.test_connection():
    print("✓ RSP connection successful!")
else:
    print("✗ RSP connection failed")
```

### Retrieving Data

```python
# Get deep coadded image
deep_coadd = client.get_deepCoadd(tract=4431, patch="2,3", band="i")
if deep_coadd:
    print(f"Retrieved deep coadd: {deep_coadd.getBBox()}")

# Get object catalog
obj_catalog = client.get_object_catalog(tract=4431, patch="2,3", band="i")
if obj_catalog:
    print(f"Retrieved object catalog with {len(obj_catalog)} objects")

# Get calibrated exposure (if you have visit/detector info)
calexp = client.get_calexp(visit=192350, detector=175)
if calexp:
    print(f"Retrieved calexp: {calexp.getBBox()}")
```

## Running the Test Script

1. Set up your token in `.env` file or environment variable
2. Run the test script:
   ```bash
   python test_enhanced_butler_client.py
   ```

The script will test both local and RSP configurations if a valid token is provided.

## Troubleshooting

### Common Issues

1. **"RSP_ACCESS_TOKEN environment variable is required"**
   - Make sure you've set the environment variable or created a `.env` file
   - Check that the `.env` file is in the same directory as the test script

2. **Connection/Auth Failures**
   - Verify your token is valid and hasn't expired
   - Ensure you have the correct scopes (`read:image`, `read:tap`)
   - Check that your RSP account is in good standing

3. **Dataset Not Found Errors**
   - Verify the collections and dataset types are correct for your data release
   - Check that the tract/patch combinations exist in the data

### Debug Mode

Enable debug logging to see detailed connection information:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Security Notes

- **Never commit your access token to version control**
- **Keep your `.env` file in `.gitignore`**
- **Use minimal necessary scopes**
- **Set reasonable expiration dates for tokens**
- **Rotate tokens regularly**

## Token Scopes Reference

- `read:tap` - Access catalog data via TAP service
- `read:image` - Access image data via Butler
- `exec:notebook` - Execute notebooks on RSP
- `write:files` - Write files to your RSP home directory
- `user:token` - Manage your tokens
- `exec:portal` - Use the Portal interface

For most RIPPLe use cases, you'll need at least `read:image` and `read:tap`.