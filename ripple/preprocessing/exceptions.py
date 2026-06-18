class PreprocessingError(Exception):
    """Base exception for preprocessing errors."""
    pass

class NormalizationError(PreprocessingError):
    """Exception for normalization errors."""
    pass

class CutoutError(PreprocessingError):
    """Exception for cutout errors."""
    pass


class CleaningError(PreprocessingError):
    """Raised when data cleaning fails irrecoverably."""


class PSFMatchError(PreprocessingError):
    """Raised when PSF matching cannot be performed."""


class ManifestError(PreprocessingError):
    """Raised on malformed preprocessing manifests."""
