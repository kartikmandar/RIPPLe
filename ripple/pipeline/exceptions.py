class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass

class PipelineExecutionError(PipelineError):
    """Exception for pipeline execution errors."""
    pass

class PipelineConfigError(PipelineError):
    """Exception for pipeline configuration errors."""
    pass