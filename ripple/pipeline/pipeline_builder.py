from typing import Any, List

from .pipeline import Pipeline
from .pipeline_stage import PipelineStage
from .stages.data_source_stage import DataSourceStage
from .stages.preprocessing_stage import PreprocessingStage
from .stages.model_stage import ModelStage

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
        and instantiates the corresponding stage objects.

        Returns:
            A list of instantiated PipelineStage objects.
        """
        stages_list = []
        if not hasattr(self.config, "get") or "stages" not in self.config:
            # Return an empty list if no stages are defined in the config
            return stages_list

        pipeline_stages_config = self.config.get("stages", [])
        for stage_config in pipeline_stages_config:
            stage_type = stage_config.get("type")
            stage_params = stage_config.get("config", {})

            if stage_type == "butler_access":
                stages_list.append(DataSourceStage(config=stage_params))
            elif stage_type == "image_processing":
                stages_list.append(PreprocessingStage(config=stage_params))
            elif stage_type == "deeplense_model":
                stages_list.append(ModelStage(config=stage_params))
            else:
                # Optionally, log a warning or raise an error for unknown stage types
                # For now, we'll just ignore unknown types
                print(f"Warning: Unknown stage type '{stage_type}' found in configuration.")
        
        return stages_list

    def build_pipeline(self) -> Pipeline:
        """
        Constructs and returns a Pipeline object.

        This method parses the configuration provided during initialization
        to create a sequence of stages and assembles them into a Pipeline.

        Returns:
            A Pipeline instance.
        """
        pipeline_name = self.config.get("name", "default_pipeline") if hasattr(self.config, "get") else "default_pipeline"
        stages = self._parse_stages_from_config()
        
        return Pipeline(name=pipeline_name, stages=stages, config=self.config)