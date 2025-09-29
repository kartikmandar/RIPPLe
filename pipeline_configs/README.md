# RIPPLe Pipeline Configurations

This directory stores YAML configuration files for various RIPPLe pipelines. Each file defines a complete workflow, from data ingestion to model training and evaluation.

## Available Configurations

*   `default_pipeline.yaml`: A baseline configuration that runs a standard pipeline. This can be used as a template for creating new pipelines.

## Creating Custom Configurations

To create a new pipeline configuration, you can copy and modify an existing file. It is recommended to keep your custom configuration files in this directory and add them to the `.gitignore` to avoid committing them to the repository.

To run a specific pipeline, pass the path to the configuration file when executing the application:

```bash
python -m ripple.main pipeline_configs/your_custom_pipeline.yaml