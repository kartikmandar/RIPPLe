class DataAccessError(Exception):
    """Base exception for data access errors."""
    pass

class ButlerConnectionError(DataAccessError):
    """Exception for Butler connection errors."""
    pass

class CoordinateConversionError(DataAccessError):
    """Exception for coordinate conversion errors."""
    pass