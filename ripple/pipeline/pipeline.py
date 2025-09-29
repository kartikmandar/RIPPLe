from dataclasses import dataclass, field
from typing import List, Any

from .pipeline_stage import PipelineStage

@dataclass
class Pipeline:
    """
    Represents a data processing pipeline.
    A pipeline consists of a name and a sequence of stages to be executed.
    """
    name: str
    stages: List[PipelineStage] = field(default_factory=list)
    config: Any = None