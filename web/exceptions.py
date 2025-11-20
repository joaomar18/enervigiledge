###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

#######################################

##########     S A F E T Y     E X C E P T I O N S     ##########


class TokenNotInRequest(Exception):
    """Raised when the safety token is not in the request"""


class TokenInRequestInvalid(Exception):
    """Raised when the safety token in the request is invalid or doesn't exist in the registered safety tokens"""


class UserConfigurationExists(Exception):
    """Raised when user configuration is trying to be created but the file already exists"""


class InvalidCredentials(Exception):
    """Raised when credentials (username and password) are invalid (not recognized)."""


class InvalidRequest(Exception):
    """Raised when a request is malformed or contains invalid data."""
