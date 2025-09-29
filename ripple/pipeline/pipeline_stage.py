from abc import ABC, abstractmethod

class PipelineStage(ABC):
    """
    Abstract base class for a pipeline stage.
    Each stage in the pipeline must inherit from this class and implement the execute method.
    """

    @abstractmethod
    def execute(self, data: any) -> any:
        """
        Execute the stage's specific logic.

        Args:
            data: The input data for the stage. The type and structure
                  of the data will depend on the specific stage implementation.

        Returns:
            The processed data after the stage has executed.
        """
        pass