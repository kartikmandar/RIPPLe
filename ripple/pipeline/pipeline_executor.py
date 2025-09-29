import logging

from .pipeline import Pipeline

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineExecutor:
    """
    Executes a given Pipeline.
    """

    def __init__(self, pipeline: Pipeline):
        """
        Initializes the PipelineExecutor with a Pipeline object.

        Args:
            pipeline: The Pipeline instance to be executed.
        """
        self.pipeline = pipeline

    def execute(self) -> None:
        """
        Executes the pipeline.

        This method will iterate through the stages of the pipeline and call
        their execute method.
        """
        logger.info(f"Starting execution of pipeline: '{self.pipeline.name}'")
        data = None # Initial data
        for stage in self.pipeline.stages:
            logger.info(f"Executing stage: {stage.__class__.__name__}")
            data = stage.execute(data)
        logger.info(f"Pipeline '{self.pipeline.name}' execution finished.")