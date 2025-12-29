###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

#######################################


@dataclass
class APIErrorDef:
    """Defines a canonical API error with status code, identifier, and default message."""

    status_code: int
    error_section: str
    error_id: str
    default_message: str


class APIException(Exception):
    """Base exception for API errors constructed from a centralized error definition."""

    def __init__(
        self,
        error: APIErrorDef,
        message: str | None = None,
        details: Dict[str, Any] = {},
    ):

        self.status_code = error.status_code
        self.error_id = error.error_id
        self.error_section = error.error_section
        self.message = message or error.default_message
        self.details = details
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


class UserConfigCorrupted(APIException):
    """Raised when the user configuration is corrupted or cannot be parsed."""

    pass


class InvalidCredentials(APIException):
    """Raised when credentials (username and password) are invalid (not recognized)."""

    pass


class InvalidRequest(APIException):
    """Raised when a request is malformed or contains invalid data."""

    pass


class ToManyRequests(APIException):
    """Raised when a client exceeds the allowed number of non authorized requests (rate limit exceeded)."""

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
        error_section="GLOBAL",
        error_id="INVALID_JSON",
        default_message="Request body must be valid JSON.",
    )
    INVALID_FORM_DATA = APIErrorDef(
        status_code=400,
        error_section="GLOBAL",
        error_id="INVALID_FORM_DATA",
        default_message="Invalid or malformed form data.",
    )
    MISSING_IP = APIErrorDef(
        status_code=400,
        error_section="GLOBAL",
        error_id="INVALID_IP",
        default_message="Missing or invalid IP from request.",
    )
    INTERNAL_SERVER_ERROR = APIErrorDef(
        status_code=500,
        error_section="GLOBAL",
        error_id="INTERNAL_SERVER_ERROR",
        default_message="Got an unexpected internal server error."
    )

    class AUTH:
        MISSING_USERNAME = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_USERNAME",
            default_message="The username is missing or in an invalid format.",
        )
        MISSING_PASSWORD = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_PASSWORD",
            default_message="The password is missing or in an invalid format.",
        )
        MISSING_PASSWORD_CONFIRM = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_CONFIRM_PASSWORD",
            default_message="The password confirmation is missing or in an invalid format.",
        )
        MISSING_OLD_PASSWORD = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_OLD_PASSWORD",
            default_message="The old password is missing or in an invalid format.",
        )
        MISSING_NEW_PASSWORD = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_NEW_PASSWORD",
            default_message="The new password is missing or in an invalid format.",
        )
        MISSING_NEW_PASSWORD_CONFIRM = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="MISSING_NEW_PASSWORD_CONFIRM",
            default_message="The new password confirmation is missing or in an invalid format.",
        )
        INVALID_AUTO_LOGIN = APIErrorDef(
            status_code=400,
            error_section="AUTH",
            error_id="INVALID_AUTO_LOGIN",
            default_message="The auto_login field must be a boolean value.",
        )
        INVALID_PASSWORD = APIErrorDef(
            status_code=422,
            error_section="AUTH",
            error_id="INVALID_PASSWORD",
            default_message="Password must be at least 5 characters and not just whitespace.",
        )
        INVALID_USERNAME = APIErrorDef(
            status_code=422,
            error_section="AUTH",
            error_id="INVALID_USERNAME",
            default_message="Username must be at least 3 characters and not just whitespace.",
        )
        INVALID_NEW_PASSWORD = APIErrorDef(
            status_code=422,
            error_section="AUTH",
            error_id="INVALID_NEW_PASSWORD",
            default_message="The new password must be at least 5 characters and not just whitespace.",
        )
        PASSWORD_MISMATCH = APIErrorDef(
            status_code=422,
            error_section="AUTH",
            error_id="PASSWORD_MISMATCH",
            default_message="New password and password confirmation do not match.",
        )
        USER_CONFIG_EXISTS = APIErrorDef(
            status_code=409,
            error_section="AUTH",
            error_id="USER_CONFIG_EXISTS",
            default_message="User configuration file already exists.",
        )
        USER_CONFIG_NOT_FOUND = APIErrorDef(
            status_code=404,
            error_section="AUTH",
            error_id="USER_CONFIG_NOT_FOUND",
            default_message="User configuration does not exist.",
        )
        USER_CONFIG_CORRUPT = APIErrorDef(
            status_code=500,
            error_section="AUTH",
            error_id="USER_CONFIG_CORRUPT",
            default_message="User configuration is corrupted.",
        )
        INVALID_CREDENTIALS = APIErrorDef(
            status_code=401,
            error_section="AUTH",
            error_id="INVALID_CREDENTIALS",
            default_message="Invalid username or password.",
        )
        INVALID_TOKEN = APIErrorDef(
            status_code=401,
            error_section="AUTH",
            error_id="INVALID_TOKEN",
            default_message="Invalid or malformed authentication token",
        )
        TOKEN_MISSING = APIErrorDef(
            status_code=401,
            error_section="AUTH",
            error_id="TOKEN_MISSING",
            default_message="Authentication token is missing.",
        )
        BLOCKED_CLIENT = APIErrorDef(
            status_code=429,
            error_section="AUTH",
            error_id="BLOCKED_CLIENT",
            default_message="Too many attempts with failed authentication."
        )

    class DEVICE:
        MISSING_DEVICE_DATA = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_DEVICE_DATA",
            default_message="The device data is missing or in an invalid format.",
        )
        MISSING_NODES_DATA = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_NODES_DATA",
            default_message="The device nodes data is missing or in an invalid format.",
        )
        MISSING_UPLOADED_IMAGE = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_UPLOADED_IMAGE",
            default_message="The uploaded image is missing or in an invalid format.",
        )
        MISSING_DEVICE_NAME = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_DEVICE_NAME",
            default_message="The device name is missing or in an invalid format.",
        )
        MISSING_DEVICE_ID = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_DEVICE_ID",
            default_message="The device id is missing or in an invalid format.",
        )
        MISSING_DEVICE_FIELDS = APIErrorDef(
            status_code=400,
            error_section= "DEVICE",
            error_id="MISSING_DEVICE_FIELDS",
            default_message="There are missing fields in the device configuration.",
        )
        MISSING_DEVICE_OPTIONS = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_OPTIONS",
            default_message="Device options is missing or in an invalid format.",
        )
        MISSING_DEVICE_COMUNICATION = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_DEVICE_COMUNICATION",
            default_message="Device communication options is missing or in an invalid format.",
        )
        MISSING_DEVICE_OPTIONS_FIELDS = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_OPTIONS_FIELDS",
            default_message="There are missing fields in the device options.",
        )
        MISSING_DEVICE_COMUNICATION_FIELDS = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_DEVICE_COMUNICATION_FIELDS",
            default_message="There are missing fields in the device communication options.",
        )
        MISSING_PROTOCOL = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_PROTOCOL",
            default_message="Protocol is missing or in an invalid format.",
        )
        MISSING_TYPE = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="MISSING_TYPE",
            default_message="Device type is missing or in an invalid format.",
        )
        INVALID_DEVICE_DATA_JSON = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="INVALID_DEVICE_DATA_JSON",
            default_message="The device data is not in a valid JSON format.",
        )
        INVALID_DEVICE_NODES_JSON = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="INVALID_DEVICE_NODES_JSON",
            default_message="The device nodes data is not in a valid JSON format.",
        )
        INVALID_DEVICE_ID = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="INVALID_DEVICE_ID",
            default_message="Device ID must be a valid integer.",
        )
        INVALID_PROTOCOL = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="INVALID_PROTOCOL",
            default_message="Provided protocol is invalid or not supported.",
        )
        INVALID_TYPE = APIErrorDef(
            status_code=400,
            error_section="DEVICE",
            error_id="INVALID_TYPE",
            default_message="Provided device type is not supported.",
        )
        DEVICE_STORAGE_FAILED = APIErrorDef(
            status_code=500,
            error_section="DEVICE",
            error_id="DEVICE_STORAGE_FAILED",
            default_message="Failed to initialize device storage.",
        )
        UPDATE_STORAGE_FAILED = APIErrorDef(
            status_code=500,
            error_section="DEVICE",
            error_id="UPDATE_STORAGE_FAILED",
            default_message="Failed to update device storage.",
        )
        DELETE_STORAGE_FAILED = APIErrorDef(
            status_code=500,
            error_section="DEVICE",
            error_id="DELETE_STORAGE_FAILED",
            default_message="Failed to delete device.",
        )
        SAVE_IMAGE_FAILED = APIErrorDef(
            status_code=500,
            error_section="DEVICE",
            error_id="SAVE_IMAGE_FAILED",
            default_message="Failed to save the uploaded image.",
        )
        NOT_FOUND = APIErrorDef(
            status_code=404,
            error_section="DEVICE",
            error_id="NOT_FOUND",
            default_message="Device not found.",
        )

    class NODES:
        MISSING_START_TIME = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_START_TIME",
            default_message="The start time date is required for formatted queries.",
        )
        MISSING_NODE_NAME = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NAME",
            default_message="The node name is missing or in an invalid format.",
        )
        MISSING_PHASE = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_PHASE",
            default_message="The electrical phase is missing.",
        )
        MISSING_PROTOCOL = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_PROTOCOL",
            default_message="Protocol is missing or in an invalid format.",
        )
        MISSING_ENERGY_DIRECTION = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_ENERGY_DIRECTION",
            default_message="The energy direction is missing.",
        )
        MISSING_NODE_CONFIG = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_CONFIG",
            default_message="The node base configuration is missing or in an invalid format.",
        )
        MISSING_NODE_PROTOCOL_OPTIONS = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_PROTOCOL_OPTIONS",
            default_message="The node protocol options is missing or in an invalid format.",
        )
        MISSING_NODE_FIELDS = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_FIELDS",
            default_message="There are missing fields in the node record.",
        )
        MISSING_NODE_CONFIG_FIELDS = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_CONFIG_FIELDS",
            default_message="There are missing fields in the node base configuration.",
        )
        MISSING_NODE_PROTOCOL_OPTIONS_FIELDS = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_PROTOCOL_OPTIONS_FIELDS",
            default_message="There are missing fields in the node protocol options.",
        )
        MISSING_NODE_ATTRIBUTES_FIELDS = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="MISSING_NODE_ATTRIBUTES_FIELDS",
            default_message="There are missing fields in the node attributes.",
        )
        INVALID_NODE = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_NODE",
            default_message="Invalid format for node record.",
        )
        INVALID_START_TIME = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_START_TIME",
            default_message="The start time must be a valid ISO 8601 datetime.",
        )
        INVALID_END_TIME = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_END_TIME",
            default_message="The end time must be a valid ISO 8601 datetime.",
        )
        INVALID_PHASE = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_PHASE",
            default_message="The eletrical phase provided is not valid.",
        )
        INVALID_PROTOCOL = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_PROTOCOL",
            default_message="Provided protocol is not supported.",
        )
        INVALID_ENERGY_DIRECTION = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_ENERGY_DIRECTION",
            default_message="The energy direction provided is not valid.",
        )
        INVALID_TIME_ZONE = APIErrorDef(
            status_code=400,
            error_section="NODES",
            error_id="INVALID_TIME_ZONE",
            default_message="The provided time zone is not valid.",
        )
        NOT_FOUND = APIErrorDef(
            status_code=404,
            error_section="NODES",
            error_id="NODES.NOT_FOUND",
            default_message="The node was not found in the specified device.",
        )
        DELETE_LOGS_FAILED = APIErrorDef(
            status_code=500,
            error_section="NODES",
            error_id="DELETE_LOGS_FAILED",
            default_message="Failed to delete log points for the specified variable.",
        )
        DELETE_ALL_LOGS_FAILED = APIErrorDef(
            status_code=500,
            error_section="NODES",
            error_id="DELETE_ALL_LOGS_FAILED",
            default_message="Failed to delete all log points in the device.",
        )
