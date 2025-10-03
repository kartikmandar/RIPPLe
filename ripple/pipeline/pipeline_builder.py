from typing import Any, List
import logging

from .pipeline import Pipeline
from .pipeline_stage import PipelineStage
from .stages.data_source_stage import DataSourceStage
from .stages.preprocessing_stage import PreprocessingStage
from .stages.model_stage import ModelStage
from .stages.ingestion_stage import IngestionStage

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class PipelineBuilder:
    """
    Constructs a Pipeline object based on a given configuration.
    """

    def __init__(self, config: Any):
        """
        Initializes the PipelineBuilder with a configuration object.

        Args:
            config: The configuration object that defines the pipeline structure.
                    The exact structure of this object is to be defined.
        """
        self.config = config

    def _parse_stages_from_config(self) -> List[PipelineStage]:
        """
        Parses the stage configurations from the main pipeline configuration
        by checking for specific configuration sections and instantiates
        the corresponding stage objects. The entire configuration is passed to each stage.

        Returns:
            A list of instantiated PipelineStage objects in logical order.
        """
        stages_list = []
        
        # Log the entire config object for debugging
        logger.debug(f"Config object type: {type(self.config)}")
        logger.debug(f"Config object content: {self.config}")

        # Convert the entire main config object to a dictionary
        if hasattr(self.config, '__dict__'):
            full_config_dict = self.config.__dict__
        elif isinstance(self.config, dict):
            full_config_dict = self.config
        else:
            logger.warning(f"Config is of unexpected type: {type(self.config)}. Attempting to convert to dict.")
            try:
                full_config_dict = dict(self.config)
            except TypeError:
                logger.error("Failed to convert config to dictionary. Stages may not receive configuration.")
                full_config_dict = {}

        # Check for ingestion stage
        if hasattr(self.config, "ingestion"):
            logger.debug("Found 'ingestion' attribute in config. Creating IngestionStage.")
            ingestion_config = getattr(self.config, "ingestion", {})
            stages_list.append(IngestionStage(config=ingestion_config))

        # Check for data_source stage
        if hasattr(self.config, "data_source"):
            logger.debug("Found 'data_source' attribute in config. Creating DataSourceStage.")
            data_source_config = getattr(self.config, "data_source", {})
            stages_list.append(DataSourceStage(config=data_source_config))
        
        # Check for processing stage
        if hasattr(self.config, "processing"):
            logger.debug("Found 'processing' attribute in config. Creating PreprocessingStage.")
            processing_config = getattr(self.config, "processing", {})
            stages_list.append(PreprocessingStage(config=processing_config))
        
        # Placeholder for model stage
        if hasattr(self.config, "model"):
            logger.debug("Found 'model' attribute in config. Creating ModelStage.")
            model_config = getattr(self.config, "model", {})
            stages_list.append(ModelStage(config=model_config))
            
        if not stages_list:
            logger.warning("No stage attributes ('ingestion', 'data_source', 'processing', 'model') found in configuration. Returning empty list.")
        
        logger.debug(f"Total stages parsed: {len(stages_list)}")
        return stages_list

    def build_pipeline(self) -> Pipeline:
        """
        Constructs and returns a Pipeline object.

        This method parses the configuration provided during initialization
        to create a sequence of stages and assembles them into a Pipeline.
        The pipeline name is derived from the 'name' key in the configuration,
        or defaults to 'default_pipeline' if not found.

        Returns:
            A Pipeline instance.
        """
        pipeline_name = "default_pipeline"
        if hasattr(self.config, "name") and isinstance(self.config.name, str):
            pipeline_name = self.config.name
        
        logger.debug(f"Config has name attribute for pipeline name: {hasattr(self.config, 'name')}")
        logger.debug(f"Pipeline name: {pipeline_name}")
        
        stages = self._parse_stages_from_config()
        logger.debug(f"Number of stages in pipeline: {len(stages)}")
        
        return Pipeline(name=pipeline_name, stages=stages, config=self.config)