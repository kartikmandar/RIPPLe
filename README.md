# RIPPLe: Rubin Image Preparation and Processing Lensing Engine

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GSoC 2025 Blog](https://img.shields.io/badge/GSoC%202025-Blog-orange)](https://gsoc2025.blogspot.com/)

RIPPLe is a high-performance data processing pipeline designed to bridge the gap between the massive datasets of the Vera C. Rubin Observatory's Legacy Survey of Space and Time (LSST) and the DeepLense machine learning framework. Its primary mission is to enable the efficient retrieval, preprocessing, and analysis of strong gravitational lensing candidates, automating the search for dark matter substructures.


## Architecture Overview

The RIPPLe pipeline follows a modular, sequential workflow that transforms raw astronomical coordinates into machine learning predictions.

```
 LSST Butler Repository
         │
         ▼
┌────────────────────┐
│  LsstDataFetcher   │ (Handles Butler queries, coordinate conversion)
└──────────┬─────────┘
           │ (Exposures, Catalogs)
           ▼
┌────────────────────┐
│    Preprocessor    │ (Creates normalized, multi-band cutouts)
└──────────┬─────────┘
           │ (PyTorch Tensors)
           ▼
┌────────────────────┐
│   ModelInterface   │ (Runs inference with a specified DeepLense model)
└──────────┬─────────┘
           │ (Predictions)
           ▼
   Output Handler (CSV, Catalog, etc.)
```

## Installation

RIPPLe requires a working LSST Science Pipelines environment and a set of Python dependencies.

### 1. Set up the LSST Environment

First, you must have the LSST Science Pipelines installed and activated in your shell.

```bash
# Example for a conda-based installation
source /path/to/lsst_stack/loadLSST.bash
setup lsst_distrib
```

### 2. Clone the RIPPLe Repository

```bash
git clone https://github.com/kartikmandar/RIPPLe
cd RIPPLe
```

---

## Configuration

The pipeline is controlled via a YAML configuration file. This file allows you to specify the data repository, collections, preprocessing steps, and model to use.

**Example `config.yaml`:**

```yaml
# Data Access Configuration
data:
  repo_path: "/path/to/lsst/repo"
  collections:
    - "LSSTCam/raw/all"
    - "LSSTCam/calib"
  dataset_type: "deepCoadd"

# Preprocessing Configuration
preprocessing:
  cutout_size: 128  # pixels
  bands: ['g', 'r', 'i']
  normalization: "asinh" # 'minmax', 'zscore', or 'asinh'

# Model Configuration
model:
  type: "classification" # or 'regression'
  path: "/path/to/models/deeplense_resnet50.pth"
  architecture: "resnet50"
  device: "cuda" # 'cuda' or 'cpu'

# Pipeline Execution Configuration
pipeline:
  batch_size: 64
  num_workers: 4
```

---

## Project Structure

```
/home/kartikmandar/RIPPLe/
├── README.md                    # This file
├── pipeline_configs/            # YAML configuration files for different demo datasets
│   ├── rc2_subset_pipeline.yaml
│   └── ...
├── ripple/                      # Core source code for the pipeline
│   ├── __init__.py
│   ├── main.py                  # Main executable script
│   ├── data_access/             # Module for Butler interaction and data fetching
│   ├── preprocessing/           # Image manipulation and normalization
│   ├── models/                  # ML model interfaces and loading logic
│   ├── pipeline/                # Core pipeline orchestration logic
│   ├── utils/                   # Utility functions
│   └── tests/                   # Unit and integration tests for the pipeline
├── notebooks/                   # Jupyter Notebooks with usage examples and tutorials (tutorials to be be added later)
│   └── 01_end_to_end_demo.ipynb
├── manual_tests/                # Manual test scripts
└── data/                        # Data storage for butler repos, etc.
```
---

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.