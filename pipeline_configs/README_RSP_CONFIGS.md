# RSP Pipeline Configurations

This directory contains pipeline configuration files for connecting to the Rubin Science Platform (RSP) using the enhanced ButlerClient with token authentication.

## Available Configurations

### 1. `rsp_dp0_pipeline.yaml`
**Use Case**: DP0 (Data Preview 0) analysis with full image processing
- **Data Source**: DP0.2 collections on RSP
- **Instrument**: LSSTCam
- **Processing**: Full pipeline with image cutouts and ML inference
- **Optimization**: Balanced for remote data access

### 2. `rsp_dp1_pipeline.yaml`
**Use Case**: DP1 (Data Preview 1) analysis with ComCam data
- **Data Source**: DP1 collections on RSP
- **Instrument**: LSSTComCam
- **Processing**: Enhanced pipeline with quality control
- **Optimization**: Optimized for DP1 data characteristics

### 3. `rsp_catalog_pipeline.yaml`
**Use Case**: Catalog-only analysis using TAP service
- **Data Source**: DP0.2 and DP0.3 collections
- **Processing**: Catalog queries, crossmatching, feature extraction
- **Optimization**: Optimized for tabular data processing

## Setup Instructions

### 1. Environment Setup
```bash
# Set up LSST environment
source /path/to/lsst_stack/loadLSST.sh
setup lsst_distrib

# Activate Python environment
conda activate ripple
```

### 2. RSP Token Configuration
Create a `.env` file in the project root:
```bash
cp .env.example .env
```

Add your RSP access token to `.env`:
```
RSP_ACCESS_TOKEN=your_actual_rsp_access_token_here
```

### 3. Run Pipeline with RSP Config
```bash
# Using DP0 configuration
python -m ripple.main --config-file pipeline_configs/rsp_dp0_pipeline.yaml

# Using DP1 configuration
python -m ripple.main --config-file pipeline_configs/rsp_dp1_pipeline.yaml

# Using catalog-only configuration
python -m ripple.main --config-file pipeline_configs/rsp_catalog_pipeline.yaml
```

## Configuration Details

### Common RSP Settings
All RSP configurations include:

- **Token Authentication**: Uses `RSP_ACCESS_TOKEN` environment variable
- **Remote Butler**: Connects to `https://data.lsst.cloud/api/butler/`
- **Optimized for Remote Access**: Larger cache sizes, extended timeouts
- **Error Handling**: Retry logic for network issues

### Data Source Configuration
```yaml
data_source:
  type: butler_server
  server_url: "https://data.lsst.cloud/api/butler/"
  auth_method: token
  collections: ["2.2i/runs/DP0.2"]
```

### Authentication Setup
```yaml
butler:
  remote_auth:
    method: token
    username: x-oauth-basic
    token_env_var: RSP_ACCESS_TOKEN
```

## Customization Guide

### For Different Collections
Modify the `collections` section:
```yaml
data_source:
  collections:
    - "2.2i/runs/DP0.2"    # Change to your desired collection
```

### For Different Processing Parameters
Adjust the processing section:
```yaml
processing:
  cutout_size: 128          # Change cutout size
  batch_size: 16           # Adjust batch size
  max_workers: 2           # Set worker count
```

### For Different Models
Update the model configuration:
```yaml
model:
  operation: "prediction"
  type: "resnet50"         # Change model type
  path: "/path/to/model.pth"
  device: "cuda"
```

## Troubleshooting

### Common Issues

1. **"RSP_ACCESS_TOKEN not found"**
   - Ensure `.env` file exists in project root
   - Check token is correctly set in environment

2. **"Authentication failed"**
   - Verify token is valid and not expired
   - Check token has required scopes (`read:image`, `read:tap`)

3. **"Connection timeout"**
   - Increase `timeout` value in configuration
   - Check network connectivity to RSP

4. **"Collection not found"**
   - Verify collection name is correct
   - Check if you have access to the specified collection

### Debug Mode
Enable verbose logging:
```bash
python -m ripple.main --config-file pipeline_configs/rsp_dp0_pipeline.yaml --verbose
```

## Token Management

### Required Scopes
- `read:image`: For image data access
- `read:tap`: For catalog queries
- `exec:notebook`: For notebook access (if needed)

### Token Security
- Never commit tokens to version control
- Use `.env` file for local development
- Set appropriate expiration dates
- Rotate tokens regularly

## Performance Tips

1. **Cache Optimization**: Increase `cache_size` for frequently accessed data
2. **Batch Processing**: Adjust `batch_size` based on available memory
3. **Parallel Processing**: Tune `max_workers` for your system
4. **Network**: Use stable internet connection for RSP access
5. **Timeouts**: Set appropriate `timeout` values for your network

## Data Release Information

- **DP0.2**: Simulated LSST data for development and testing
- **DP0.3**: Solar system object simulations
- **DP1**: Real ComCam data from Rubin Observatory

Choose the appropriate configuration based on your data source and analysis needs.