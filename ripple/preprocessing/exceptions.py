class PreprocessingError(Exception):
    """Base exception for preprocessing errors."""
    pass

class NormalizationError(PreprocessingError):
    """Exception for normalization errors."""
    pass

class CutoutError(PreprocessingError):
    """Exception for cutout errors."""
    pass