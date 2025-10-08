###########EXTERNAL IMPORTS############

from typing import Dict, Any, Type, Union, Tuple, List, Optional, TypeVar, get_origin, cast
from enum import Enum
import dataclasses
import os
from fastapi.datastructures import FormData, QueryParams

#######################################

#############LOCAL IMPORTS#############

#######################################

T = TypeVar("T")  # Generic type variable
E = TypeVar("E", bound=Enum)  # Generic Enum Variable


def check_required_keys(
    input_dict: Dict[str, Any], dataclass_type: Type, ignore_keys: Tuple[str] = tuple()
) -> Tuple[Tuple[dataclasses.Field, ...], List[str]]:
    """
    Validates that all required keys are present in the input dictionary.

    Args:
        input_dict (Dict[str, Any]): The dictionary to validate.
        dataclass_type (Type): The dataclass type to check against.
        ignore_keys (Tuple[str]): Keys to ignore during validation.

    Returns:
        Tuple[Tuple[dataclasses.Field, ...], List[str]]: Dataclass fields and optional fields.

    Raises:
        ValueError: If the input dictionary is missing.
        KeyError: If required fields are missing.
    """

    if not input_dict:
        raise ValueError(f"Missing input dataclass {dataclass_type.__name__}")

    # Get required fields from the class
    dataclass_fields = dataclasses.fields(dataclass_type)
    required_fields = []
    optional_fields = []

    for field in dataclass_fields:

        # Checks if field in type class doesn't have a default value (required field)
        if field.default == field.default_factory == dataclasses.MISSING:
            required_fields.append(field.name)

        # Checks if field in communication options has a default value (optional field)
        else:
            optional_fields.append(field.name)

    # Check for missing required fields
    missing_fields = [field for field in required_fields if field not in input_dict and field not in ignore_keys]
    if missing_fields:
        raise KeyError(f"Missing required fields: {', '.join(missing_fields)}")

    return (dataclass_fields, optional_fields)


def add_value_to_dict(dict: Dict[str, Any], field: dataclasses.Field, value: Union[int, float, str, bool]) -> None:
    """
    Adds a value to a dictionary after validating the field.

    Args:
        dict (Dict[str, Any]): The target dictionary to modify.
        field (dataclasses.Field): The dataclass field to process.
        value (Union[int, float, str, bool]): The value to add to the dictionary.

    Raises:
        ValueError: If the field validation fails or the value cannot be added.
    """

    try:
        dict[field.name] = value
        real_type = resolve_type(field.type)
        require_field(dict, field.name, real_type)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Failed to add value '{value}' of type '{field.type}' to dictionary: {e}")


def create_dict_from_fields(
    input_dict: Dict[str, Any], dataclass_fields: Tuple[dataclasses.Field, ...], optional_fields: List[str]
) -> Dict[str, Any]:
    """
    Creates a dictionary from dataclass fields, filling in defaults for optional fields.

    Args:
        input_dict (Dict[str, Any]): The source dictionary.
        dataclass_fields (Tuple[dataclasses.Field, ...]): Fields of the dataclass.
        optional_fields (List[str]): List of optional field names.

    Returns:
        Dict[str, Any]: A dictionary with processed fields.
    """

    output_dict: Dict[str, Any] = dict()

    # Process all fields
    for field in dataclass_fields:

        if field.name in input_dict:
            value = input_dict[field.name]
            add_value_to_dict(output_dict, field, value)

        elif field.name in optional_fields:
            # Use default value for optional fields if not provided
            if field.default != dataclasses.MISSING:
                output_dict[field.name] = field.default
            elif field.default_factory != dataclasses.MISSING:
                output_dict[field.name] = field.default_factory()
            else:
                output_dict[field.name] = None

    return output_dict


def require_field(data: dict[str, Any] | FormData | QueryParams, key: str, expected_type: Type[T]) -> T:
    """
    Retrieves and validates a required field from a dictionary, FormData or QueryParams.

    Args:
        data (dict[str, Any] | FormData | QueryParams): The source object to extract the field from.
        key (str): The field key to retrieve.
        expected_type (Type[T]): The expected type of the field value.

    Returns:
        T: The field value, cast to the expected type.

    Raises:
        ValueError: If the field is missing or not of the expected type.

    Note:
        The type check only validates against the provided expected_type if it is a class.
        The returned value is cast to the expected type for type checkers, but no runtime conversion is performed.
    """

    if key not in data:
        raise ValueError(f"Field '{key}' is required but is missing.")
    value = data[key]

    origin = get_origin(expected_type) or expected_type

    if isinstance(origin, type):
        if not isinstance(value, origin):
            raise ValueError(f"Field '{key}' must be of type {origin.__name__}, " f"got {type(value).__name__}")

    return cast(T, value)


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
