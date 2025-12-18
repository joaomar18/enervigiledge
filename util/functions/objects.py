###########EXTERNAL IMPORTS############

from typing import (
    Dict,
    Any,
    Type,
    Union,
    Tuple,
    List,
    Optional,
    TypeVar,
    get_origin,
    get_args,
    cast,
)
from types import UnionType
from enum import Enum
import dataclasses
import os
from fastapi.datastructures import FormData, QueryParams

#######################################

#############LOCAL IMPORTS#############

#######################################

T = TypeVar("T")  # Generic type variable
E = TypeVar("E", bound=Enum)  # Generic Enum Variable


def require_env_variable(key: str) -> str:
    """
    Returns the value of the environment variable for the given key.
    Raises:
        KeyError: If the key is not found
    """

    value = os.getenv(key)
    if value is None:
        raise KeyError(f"Key {key} was not found in the environment")

    return value


def convert_str_to_enum(str_value: str, enum: Type[E]) -> E:
    """
    Converts a string to an enum value.

    Args:
        str_value (str): The string representation of the enum value.
        enum (Type[E]): The enum type to convert to.

    Returns:
        E: The corresponding enum value.

    Raises:
        ValueError: If the string does not match any enum name.
    """

    try:
        enum_value = enum[str_value]
        return enum_value
    except KeyError:
        raise ValueError(f"Invalid {enum.__name__}: {str_value}. Must be one of: {[e.name for e in enum]}")


def resolve_type(t: Any) -> Any:
    """
    Return a usable runtime type from a dataclass field.type"
    """

    origin = get_origin(t)
    if origin is not None:
        return origin or Any
    return t


def check_bool_str(string: Optional[str]) -> bool:
    """
    Convert string to boolean, case-insensitive check for "TRUE".

    Args:
        string: String to convert, or None.

    Returns:
        bool: True if string equals "TRUE" (case-insensitive), False otherwise.
    """

    if string is not None:
        return string.upper() == "TRUE"
    return False
