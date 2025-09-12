###########EXTERNAL IMPORTS############

from typing import Dict, Any, Type, Tuple, List, TypeVar
import dataclasses
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
