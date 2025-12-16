###########EXTERNAL IMPORTS############

from functools import wraps
from typing import Callable, List, Type
from dataclasses import dataclass, field
from fastapi import Request
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.exceptions import InvalidRequest, TokenNotInRequest, TokenInRequestInvalid, UserConfigurationExists, UserConfigurationNotFound, InvalidCredentials, APIException
from web.safety import HTTPSafety
from util.debug import LoggerManager
import util.functions.web as web_util

#######################################


DEFAULT_INCREMENT_EXCEPTIONS = [InvalidRequest, TokenNotInRequest, TokenInRequestInvalid]  # Default exceptions that should increment failed requests


@dataclass
class APIMethodConfig:
    """
    Configuration for API endpoint exception handling.

    Attributes:
        requires_auth: Whether the endpoint requires authentication (JWT Token)
        increment_exceptions: Exception types that should increment failed requests
    """

    requires_auth: bool = True
    increment_exceptions: List[Type[APIException]] = field(default_factory=list)  # Exception types that increment failed requests


def auth_endpoint(config: APIMethodConfig):
    """
    Decorator for authentication endpoints with proper exception handling.

    Args:
        increment_exceptions: Exception classes that should increment failed requests
        no_increment_exceptions: Exception classes that should NOT increment failed requests
        requires_auth: Whether endpoint requires existing authentication

    Returns:
        Decorator that wraps the endpoint with safety and exception handling
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, safety: HTTPSafety, **kwargs) -> JSONResponse:

            logger = LoggerManager.get_logger(__name__)

            try:

                # Check if client is blocked
                if safety.is_blocked(request):
                    return JSONResponse(
                        status_code=429,
                        content={"unlocked": safety.get_unlocked_date(request), "error": "Too many failed attempts. Try again later."},
                    )

                # Check authentication if required
                if config.requires_auth:
                    username, token, jwt_secret = safety.check_authorization_token(request)

                result = await func(request, safety, **kwargs)  # Call the core endpoint function
                safety.clean_failed_requests(request, web_util.get_api_url(request))  # Clean failed requests on success

                return result
            
            except APIException as e:
                exception_name = e.__class__.__name__

                all_increment_exceptions = (
                    DEFAULT_INCREMENT_EXCEPTIONS + config.increment_exceptions
                )  # Merge default icrement exceptions with config-specific ones

                # Handle incrementing exceptions
                if any(isinstance(e, exc) for exc in all_increment_exceptions):
                    logger.exception(
                        f"Failed {web_util.get_api_url(request)} API due to exception {exception_name} from IP: {web_util.get_ip_address(request)} due to error: {str(e)}"
                    )
                    safety.increment_failed_requests(request, web_util.get_api_url(request))

                    # Check if now blocked after incrementing
                    if safety.is_blocked(request):
                        return JSONResponse(
                            status_code=429,
                            content={"unlocked": safety.get_unlocked_date(request), "error": "Too many failed attempts. Try again later."},
                        )

                    return JSONResponse(status_code=401, content={"remaining": safety.get_remaining_requests(request), "error": str(e)})
                else:
                    safety.clean_failed_requests(request, web_util.get_api_url(request))  # Clean failed requests if the exception was not of incrementing type
                    

            except Exception as e:
                # Handle unexpected exceptions
                logger.exception(f"Failed {web_util.get_api_url(request)} API due to server error: {str(e)}")
                return JSONResponse(status_code=500, content={"error": str(e)})

        return wrapper

    return decorator


# Simple preset configurations
class AuthConfigs:
    """Simple presets for common auth endpoint patterns."""

    # Standard login endpoint
    LOGIN = APIMethodConfig(requires_auth=False, increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    AUTO_LOGIN = APIMethodConfig(requires_auth=False, increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    LOGOUT = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    CREATE_LOGIN = APIMethodConfig(requires_auth=False, increment_exceptions=[UserConfigurationExists])
    CHANGE_PASSWORD = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
    PROTECTED = APIMethodConfig(increment_exceptions=[InvalidCredentials, UserConfigurationNotFound])
