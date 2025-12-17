###########EXTERNAL IMPORTS############

from typing import Dict, Any, Type, Union, Tuple, List, Optional, TypeVar, get_origin, get_args, cast
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


def check_required_keys(
    input_dict: Dict[str, Any], dataclass_type: Type, ignore_keys: Tuple[str, ...] = tuple()
) -> Tuple[Tuple[dataclasses.Field, ...], List[str]]:
    """
    Validates that all required keys are present in the input dictionary.

    Checks the input dictionary against the required and optional fields of
    the provided dataclass type. Returns the list of all dataclass fields
    and optional fields. If any required fields are missing, raises a KeyError
    containing the missing field names as structured data.

    Args:
        input_dict (Dict[str, Any]): The dictionary to validate.
        dataclass_type (Type): The dataclass type to check against.
        ignore_keys (Tuple[str]): Keys to ignore during validation.

    Returns:
        Tuple[Tuple[dataclasses.Field, ...], List[str]]: A tuple containing:
            - A tuple of all dataclass fields.
            - A list of optional field names.

    Raises:
        KeyError: If required fields are missing. The exception argument contains
        a tuple of missing field names.
    """

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
        raise KeyError(tuple(missing_fields))

    return (dataclass_fields, optional_fields)


def add_value_to_dict(dict: Dict[str, Any], field: dataclasses.Field, value: Any) -> None:
    """
    Add a value to a dictionary and validate it against a dataclass field.

    Inserts the value using the dataclass field name as the key and validates
    the value against the resolved field type. If validation fails, raises a
    ValueError carrying the field name as structured exception data.

    Args:
        dict: Target dictionary to update.
        field: Dataclass field defining the expected key name and type.
        value: Value to assign to the field.

    Raises:
        ValueError: If the value cannot be validated. The exception argument
        contains the field name.
    """

    try:
        dict[field.name] = value
        if not validate_field_type(dict, field.name, field.type):
            raise ValueError(f"{field.name} with invalid type or missing.")
    except (TypeError, ValueError) as e:
        raise ValueError(field.name) from e


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


def validate_field_type(data: dict[str, Any] | FormData | QueryParams, key: str, expected_type: Any) -> bool:
    """
    Validate the runtime type of a value associated with a key in a mapping.

    Checks whether the given key exists in the provided mapping and whether
    its value conforms to the specified expected type. Supports basic runtime
    validation for concrete types and simple union types (e.g. ``str | None``).

    This function performs shallow runtime checks only and does not handle
    complex typing constructs such as nested generics, containers, or
    structural typing.

    Args:
        data: Mapping-like object containing input values (e.g. request data).
        key: Key whose associated value should be validated.
        expected_type: Expected runtime type or union of types.

    Returns:
        bool: ``True`` if the key exists and the value matches the expected
        type; ``False`` otherwise.
    """

    if key not in data:
        return False
    value = data[key]

    origin = get_origin(expected_type) or expected_type
    if origin in (Union, UnionType):
        if not isinstance(value, get_args(expected_type)):
            return False
    elif isinstance(expected_type, type):
        if not isinstance(value, expected_type):
            return False
    elif isinstance(expected_type, type) and issubclass(expected_type, Enum):
        if value not in [e.value for e in expected_type]:
            return False

    return True


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
