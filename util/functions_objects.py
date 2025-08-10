###########EXTERNAL IMPORTS############

from typing import Dict, Any, Type, Tuple, List
import dataclasses

#######################################

#############LOCAL IMPORTS#############


#######################################


def check_required_keys(input_dict: Dict[str, Any], type_class: Type) -> Tuple[Tuple[dataclasses.Field, ...], List[str], List[str]] | None:
    """
    Validates dictionary fields against a dataclass and returns field metadata.

    Uses dataclass field introspection to determine required fields (those without
    default values) and returns analysis of the dataclass structure and validation.

    Args:
        input_dict (Dict[str, Any]): Dictionary to validate.
        type_class (Type): Dataclass type to check against.

    Returns:
        Optional[Tuple]: Tuple of (fields, required_fields, optional_fields) or None if validation fails.
    """

    if not input_dict:
        return None

    # Get required fields from the class
    dataclass_fields = dataclasses.fields(type_class)
    required_fields = []
    optional_fields = []

    for field in dataclass_fields:

        # Checks if field in communication options doesn't have a default value (required field)
        if field.default == field.default_factory == dataclasses.MISSING:
            required_fields.append(field.name)

        # Checks if field in communication options has a default value (optional field)
        else:
            optional_fields.append(field.name)

    # Check for missing required fields
    missing_fields = [field for field in required_fields if field not in input_dict]
    if missing_fields:
        return None

    return (dataclass_fields, required_fields, optional_fields)


def add_value_to_dict(dict: Dict[str, Any], field_name: str, value: Any, value_type: str) -> None:
    """
    Adds a value to a dictionary with automatic type conversion.

    Converts the value to the specified type and adds it to the target dictionary.
    Modifies the dictionary in-place.

    Args:
        dict (Dict[str, Any]): Target dictionary to modify.
        field_name (str): Key name for the new entry.
        value (Any): Value to convert and add.
        value_type (str): Target type for conversion ('int', 'str', 'float', 'bool').
    """

    # Type conversion based on field type annotation
    if value_type == 'int':
        dict[field_name] = int(value)
    elif value_type == 'str':
        dict[field_name] = str(value) if value is not None else None
    elif value_type == 'float':
        dict[field_name] = float(value)
    elif value_type == 'bool':
        dict[field_name] = bool(value)
    else:
        dict[field_name] = value


def create_dict_from_fields(
    input_dict: Dict[str, Any], dataclass_fields: Tuple[dataclasses.Field, ...], required_fields: List[str], optional_fields: List[str]
) -> Dict[str, Any]:
    """
    Creates a new dictionary from dataclass fields with type conversion and defaults.

    Processes dataclass fields to build a dictionary with proper type conversion,
    applying default values for missing optional fields.

    Args:
        input_dict (Dict[str, Any]): Source dictionary with input values.
        dataclass_fields (Tuple[dataclasses.Field, ...]): Dataclass field definitions.
        required_fields (List[str]): List of required field names.
        optional_fields (List[str]): List of optional field names.

    Returns:
        Dict[str, Any]: New dictionary with converted values and defaults applied.
    """

    output_dict: Dict[str, Any] = dict()

    # Process all fields
    for field in dataclass_fields:
        field_name = field.name
        field_type = str(field.type)

        if field_name in input_dict:
            value = input_dict[field_name]
            add_value_to_dict(output_dict, field_name, value, field_type)

        elif field_name in optional_fields:
            # Use default value for optional fields if not provided
            if field.default != dataclasses.MISSING:
                output_dict[field_name] = field.default
            elif field.default_factory != dataclasses.MISSING:
                output_dict[field_name] = field.default_factory()
            else:
                output_dict[field_name] = None

    return output_dict
