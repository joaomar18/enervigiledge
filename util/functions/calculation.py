###########EXTERNAL IMPORTS############

from typing import Optional, Union

#######################################

#############LOCAL IMPORTS#############

#######################################


def get_unit_factor(unit_str: Optional[str]) -> float:
    """
    Get SI prefix factor from a unit string.

    Args:
        unit_str: Unit string (e.g., "kW", "MW"). If None, returns 1.0.

    Returns:
        Scaling factor to convert to base unit.
    """

    UNIT_PREFIXES = {"m": 1e-3, "": 1.0, "k": 1e3, "M": 1e6, "G": 1e9}

    if not unit_str:
        return 1.0
    return UNIT_PREFIXES.get(unit_str[0], 1.0)


def get_scaled_value(value: Union[int, float], unit: Optional[str]) -> Union[int, float]:
    """
    Returns the node value scaled by its unit prefix (if any).

    For example:
        - 'kW' → multiplies by 1e3
        - 'mA' → multiplies by 1e-3
        - 'V' → treated as no prefix (1.0)

    Args:
        value (Union[int, float]): The numeric value to scale.
        unit (Optional[str]): The unit string containing the prefix.

    Returns:
        Union[int, float]: Scaled numeric value based on unit prefix.
    """

    factor = get_unit_factor(unit)
    return value * factor


def apply_output_scaling(value: Union[int, float], unit: Optional[str]) -> Union[int, float]:
    """
    Scales a calculated value to match the unit prefix of the target node.

    This is typically used to convert values expressed in base units (e.g., W, Wh)
    to prefixed units (e.g., kW, kWh) according to the target node's unit.

    For example:
        - If unit is 'kWh', the value will be divided by 1e3.
        - If unit is 'MWh', the value will be divided by 1e6.

    Args:
        value (Union[int, float]): The calculated value to be scaled.
        unit (Optional[str]): The target unit whose prefix determines the scaling.

    Returns:
        Union[int, float]: The value scaled to the target unit prefix.
    """

    factor = get_unit_factor(unit)
    return value / factor
