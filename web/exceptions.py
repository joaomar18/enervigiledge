###########EXTERNAL IMPORTS############

from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

#######################################


@dataclass
class APIErrorDef:
    """Defines a canonical API error with status code, identifier, and default message."""
    
    status_code: int
    error_id: str
    default_message: str


class APIException(Exception):
    """Base exception for API errors constructed from a centralized error definition."""
    
    def __init__(self, error: APIErrorDef, message: str | None = None):
        
        self.status_code = error.status_code
        self.error_id = error.error_id
        self.message = message or error.default_message
        super().__init__(self.message)
    

##########     G E N E R A L     E X C E P T I O N S     ##########

class InvalidRequestPayload(APIException):
    """Raised when a request body is malformed, invalid, or missing required fields."""
    pass

##########     S A F E T Y     E X C E P T I O N S     ##########

class TokenNotInRequest(APIException):
    """Raised when the safety token is not in the request"""
    pass

class TokenInRequestInvalid(APIException):
    """Raised when the safety token in the request is invalid or doesn't exist in the registered safety tokens"""
    pass

class UserConfigurationExists(APIException):
    """Raised when user configuration is trying to be created but the file already exists"""
    pass

class UserConfigurationNotFound(APIException):
    """Raised when the user configuration does not exist or has not been initialized."""
    pass

class InvalidCredentials(APIException):
    """Raised when credentials (username and password) are invalid (not recognized)."""
    pass

class InvalidRequest(APIException):
    """Raised when a request is malformed or contains invalid data."""
    pass
    
##########     D B     E X C E P T I O N S     ##########

class DeviceCreationError(APIException):
    """Raised when a device cannot be created or persisted in the system."""
    pass

class DeviceUpdateError(APIException):
    """Raised when updating an existing device configuration or state fails."""
    pass

class DeviceDeleteError(APIException):
    """Raised when deletion of a device or its associated resources fails."""
    pass

##########     D E V I C E     E X C E P T I O N S     ##########

class DeviceNotFound(APIException):
    """Raised when a requested device does not exist in the system."""
    pass

##########     N O D E     E X C E P T I O N S     ##########

class NodeNotFound(APIException):
    """Raised when a requested node does not exist in the system."""
    pass

##########     C E N T R A L I Z E D     E R R O R S     O B J E C T     ##########

class Errors:
    INVALID_JSON = APIErrorDef(
        status_code=400,
        error_id="INVALID_JSON",
        default_message="Request body must be valid JSON.",
    )
    
    class AUTH:
        MISSING_USERNAME = APIErrorDef(
            status_code=400,
            error_id="AUTH.MISSING_USERNAME",
            default_message="The username is missing or in an invalid format.",
        )
        MISSING_PASSWORD = APIErrorDef(
            status_code=400,
            error_id="AUTH.MISSING_PASSWORD",
            default_message="The password is missing or in an invalid format.",
        )
        MISSING_OLD_PASSWORD = APIErrorDef(
            status_code=400,
            error_id="AUTH.MISSING_OLD_PASSWORD",
            default_message="The old password is missing or in an invalid format.",
        )
        MISSING_NEW_PASSWORD = APIErrorDef(
            status_code=400,
            error_id="AUTH.MISSING_NEW_PASSWORD",
            default_message="The new password is missing or in an invalid format.",
        )
        INVALID_PASSWORD = APIErrorDef(
            status_code=422,
            error_id="AUTH.INVALID_PASSWORD",
            default_message="Password must be at least 5 characters and not just whitespace."
        )
        USER_CONFIG_EXISTS = APIErrorDef(
            status_code=409, 
            error_id="AUTH.USER_CONFIG_EXISTS", 
            default_message="User configuration file already exists."
        )
        
        
        
        

