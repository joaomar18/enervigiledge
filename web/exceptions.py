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
    
##########     D B     E X C E P T I O N S     ##########

class DeviceCreationError(Exception):
    """Raised when a device cannot be created or persisted in the system."""
    pass

class DeviceUpdateError(Exception):
    """Raised when updating an existing device configuration or state fails."""
    pass

class DeviceDeleteError(Exception):
    """Raised when deletion of a device or its associated resources fails."""
    pass

##########     D E V I C E     E X C E P T I O N S     ##########

class DeviceNotFound(Exception):
    """Raised when a requested device does not exist in the system."""
    pass