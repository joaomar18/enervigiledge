###########EXTERNAL IMPORTS############

from fastapi import Request

#######################################

#############LOCAL IMPORTS#############

from web.exceptions import InvalidRequest

#######################################


def get_ip_address(request: Request) -> str:
    """
    Returns the client's IP address from a request.

    Raises:
        InvalidRequest: If the request has no client information.
    """

    if request.client is None:
        raise InvalidRequest(f"Request doesn't contain valid client")

    return request.client.host


def get_api_url(request: Request) -> str:
    """
    Returns the path of the API URL from the given request.
    """

    return request.url.path
