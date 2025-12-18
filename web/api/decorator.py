###########EXTERNAL IMPORTS############

from functools import wraps
from typing import Dict, Any, Callable, Awaitable, List, Type
from dataclasses import dataclass, field
from fastapi import Request
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.exceptions import (
    InvalidRequest,
    TokenNotInRequest,
    TokenInRequestInvalid,
    UserConfigurationExists,
    UserConfigurationNotFound,
    InvalidCredentials,
    APIException,
)
from web.safety import HTTPSafety
from util.debug import LoggerManager
import util.functions.web as web_util
import web.exceptions as api_exception

#######################################


EndpointFunc = Callable[[Request, HTTPSafety], Awaitable[JSONResponse]]
DEFAULT_INCREMENT_EXCEPTIONS = [
    InvalidRequest,
    TokenNotInRequest,
    TokenInRequestInvalid,
]  # Default exceptions that should increment failed requests


@dataclass
class APIMethodConfig:
    """
    Configuration for API endpoint behavior related to authentication and request-rate safety.

    Attributes:
        requires_auth: Whether the endpoint requires JWT-based authentication.
        increment_exceptions: APIException types that should count toward failed request tracking (e.g., for client blocking).
    """

    requires_auth: bool = True
    increment_exceptions: List[Type[APIException]] = field(default_factory=list)  # Exception types that increment failed requests


def auth_endpoint(config: APIMethodConfig):

    def decorator(func: EndpointFunc) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, safety: HTTPSafety, **kwargs) -> JSONResponse:

            logger = LoggerManager.get_logger(__name__)

            try:

                # Check if client is blocked
                if safety.is_blocked(request):
                    raise api_exception.ToManyRequests(
                        api_exception.Errors.AUTH.BLOCKED_CLIENT,
                        None,
                        details={"unlocked_date": safety.get_unlocked_date(request)},
                    )

                # Check authentication if required
                if config.requires_auth:
                    username, token, jwt_secret = safety.check_authorization_token(request)

                result = await func(request, safety, **kwargs)  # Call the core endpoint function
                safety.clean_failed_requests(request, web_util.get_api_url(request))  # Clean failed requests on success
                return result

            except APIException as e:
                all_increment_exceptions = DEFAULT_INCREMENT_EXCEPTIONS + config.increment_exceptions  # Merge default icrement exceptions with config-specific ones
                logger.warning(f"Failed {web_util.get_api_url(request)} API from IP: {web_util.get_ip_address(request)} due to error: {str(e.message)}")

                # Handle incrementing exceptions
                if any(isinstance(e, exc) for exc in all_increment_exceptions):
                    safety.increment_failed_requests(request, web_util.get_api_url(request))
                else:
                    safety.clean_failed_requests(request, web_util.get_api_url(request))  # Clean failed requests if the exception was not of incrementing type
                    return JSONResponse(status_code=401, content={"error": str(e)})

                content: Dict[str, Any] = {}
                content["message"] = e.message
                content["error_code"] = e.error_id
                content.update(e.details)
                return JSONResponse(status_code=e.status_code, content=content)

            except Exception as e:
                logger.exception(f"Failed {web_util.get_api_url(request)} API due to server error: {str(e)}")
                content: Dict[str, Any] = {}
                content["message"] = str(e)
                content["error_code"] = api_exception.Errors.INTERNAL_SERVER_ERROR.error_id
                return JSONResponse(status_code=api_exception.Errors.INTERNAL_SERVER_ERROR.status_code, content=content)

        return wrapper

    return decorator


# Simple preset configurations
class AuthConfigs:
    """Simple presets for common auth endpoint patterns."""

    # Standard login endpoint
    LOGIN = APIMethodConfig(
        requires_auth=False,
        increment_exceptions=[InvalidCredentials, UserConfigurationNotFound],
    )
    AUTO_LOGIN = APIMethodConfig(
        requires_auth=False,
        increment_exceptions=[InvalidCredentials, UserConfigurationNotFound],
    )
    LOGOUT = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    CREATE_LOGIN = APIMethodConfig(requires_auth=False, increment_exceptions=[UserConfigurationExists])
    CHANGE_PASSWORD = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    PROTECTED = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
