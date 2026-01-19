class APIRequestError(Exception):
    pass


class GameweekDiscoveryError(Exception):
    pass


class WriteFileError(Exception):
    pass


class DBConnectionError(Exception):
    """Raised when a database connection cannot be established."""
    pass


class DBWriteError(Exception):
    """Raised when a database write or schema operation fails."""
    pass