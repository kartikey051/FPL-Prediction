class FPLBaseError(Exception):
    """Base class for FPL pipeline errors."""
    pass


class FPLNetworkError(FPLBaseError):
    """Raised when network or connection errors occur."""
    pass


class FPLServerError(FPLBaseError):
    """Raised when the FPL API returns 5xx."""
    pass


class FPLClientError(FPLBaseError):
    """Raised when the FPL API returns 4xx."""
    pass


class FPLSchemaError(FPLBaseError):
    """Raised when unexpected JSON structure is returned."""
    pass
