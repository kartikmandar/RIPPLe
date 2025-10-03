from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class PipelineStage(ABC):
    """
    Abstract base class for a pipeline stage.
    Each stage in the pipeline must inherit from this class and implement the execute method.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline stage with configuration.

        Args:
            config (Optional[Dict[str, Any]]): Configuration dictionary for the stage.
                                               Defaults to None.
        """
        self.config = config if config is not None else {}

    @abstractmethod
    def execute(self, data: Any = None) -> Any:
        """
        Execute the stage's specific logic.

        Args:
            data (Any, optional): The input data for the stage. The type and structure
                                  of the data will depend on the specific stage implementation.
                                  Defaults to None.

        Returns:
            Any: The processed data after the stage has executed.
        """
        pass