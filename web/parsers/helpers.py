###########EXTERNAL IMPORTS############

from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

#######################################


def parse_bool_field_from_dict(data: Dict[str, Any], key: str, missing: list[str], optional: bool = False) -> bool | None:
    """
    Parse a boolean field from a dictionary.

    Retrieves the value associated with ``key`` if it is a boolean. If the field
    is required and missing or not a boolean, the key is added to ``missing`` and
    ``None`` is returned. If the field is optional and the value is explicitly
    ``None``, ``None`` is returned without marking the field as missing.
    """

    value = data.get(key)
    if isinstance(value, bool):
        return value
    elif value is None and optional:
        return None
    missing.append(key)
    return None


def parse_int_field_from_dict(data: Dict[str, Any], key: str, missing: list[str], optional: bool = False) -> int | None:
    """
    Parse an integer field from a dictionary.

    Retrieves and attempts to convert the value associated with ``key`` to an
    integer. If the field is required and missing or cannot be converted, the
    key is added to ``missing`` and ``None`` is returned. If the field is
    optional and the value is explicitly ``None``, ``None`` is returned
    without marking the field as missing.
    """

    try:
        value = data[key]
        if value is not None and isinstance(value, (int, str)):
            return int(value)
        elif value is None and optional:
            return None
        else:
            raise ValueError(f"Value can't be casted to int.")
    except Exception:
        missing.append(key)
        return None


def parse_float_field_from_dict(data: Dict[str, Any], key: str, missing: list[str], optional: bool = False) -> float | None:
    """
    Parse a float field from a dictionary.

    Retrieves and attempts to convert the value associated with ``key`` to a
    float. If the field is required and missing or cannot be converted, the key
    is added to ``missing`` and ``None`` is returned. If the field is optional
    and the value is explicitly ``None``, ``None`` is returned without marking
    the field as missing.
    """

    try:
        value = data[key]
        if value is not None and isinstance(value, (float, int, str)):
            return float(value)
        elif value is None and optional:
            return None
        else:
            raise ValueError(f"Value can't be casted to float.")
    except Exception:
        missing.append(key)
        return None


def parse_str_field_from_dict(data: Dict[str, Any], key: str, missing: list[str], optional: bool = False) -> str | None:
    """
    Parse a string field from a dictionary.

    Retrieves the value associated with ``key`` if it is a string. If the field
    is required and missing or not a string, the key is added to ``missing`` and
    ``None`` is returned. If the field is optional and the value is explicitly
    ``None``, ``None`` is returned without marking the field as missing.
    """

    value = data.get(key)
    if isinstance(value, str):
        return value
    elif value is None and optional:
        return None
    missing.append(key)
    return None
