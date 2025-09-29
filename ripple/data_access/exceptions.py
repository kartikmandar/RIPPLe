class InvalidRepositoryError(Exception):
    """
    Raised when the Butler repository path is invalid.
    """
    pass


class DataAccessError(Exception):
    """
    Base class for data access related errors.
    """
    pass


class ButlerConnectionError(DataAccessError):
    """
    Raised when there's an error connecting to the Butler repository.
    """
    pass


class CoordinateConversionError(DataAccessError):
    """
    Raised when there's an error converting coordinates.
    """
    pass