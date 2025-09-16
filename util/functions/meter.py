###########EXTERNAL IMPORTS############

from typing import Dict, Tuple, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from controller.node.processor.processor import NodeProcessor
from controller.types.device import EnergyMeterType
from controller.types.node import NodePhase, NodeAttributes

#######################################


def get_node_prefix(node: Node) -> str:
    """
    Returns the phase-related prefix from a node's name.

    Checks if the name starts with a known prefix (e.g. "l1_", "l2_", "total_")
    and returns it, or an empty string if none match.
    """

    for prefix in ("l1_l2_", "l2_l3_", "l3_l1_", "l1_", "l2_", "l3_", "total_"):
        if node.config.name.startswith(prefix):
            return prefix
    return ""


def remove_phase_string(name: str) -> str:
    """
    Removes the phase or total prefix (e.g., 'l1_', 'l2_', 'l3_', 'total_', 'l1_l2_', etc.) from a node name if present.

    Args:
        name (str): The name of the node.

    Returns:
        str: The node name without the phase prefix.
    """

    parts = name.split("_")

    # Handle common prefixes
    if parts[0] in {"l1", "l2", "l3", "total"}:
        # Check for line-to-line voltages
        if len(parts) > 1 and parts[1] in {"l1", "l2", "l3"}:
            return "_".join(parts[2:])
        return "_".join(parts[1:])

    return name


def create_default_node_attributes(meter_type: EnergyMeterType) -> NodeAttributes:
    """
    Creates default node attributes based on the energy meter type.

    Args:
        meter_type (EnergyMeterType): The type of the energy meter.

    Returns:
        NodeAttributes: Default attributes for the node.

    Raises:
        ValueError: If the meter type is invalid.
    """

    if meter_type is EnergyMeterType.SINGLE_PHASE:
        node_phase = NodePhase.SINGLEPHASE
    elif meter_type is EnergyMeterType.THREE_PHASE:
        node_phase = NodePhase.GENERAL
    else:
        raise ValueError(f"Invalid meter type: {meter_type}. Must be one of: {[t.value for t in EnergyMeterType]}")

    return NodeAttributes(phase=node_phase)


def find_node(key: str, dictionary: Dict[str, Node]) -> Optional[Node]:
    """
    Retrieve a node from a dictionary by its key without raising errors.

    Returns:
        Optional[Node]: The node if found, otherwise None.
    """

    return dictionary.get(key)


def get_node(key: str, dictionary: Dict[str, Node]) -> Node:
    """
    Retrieve a node from a dictionary by its key.

    Raises:
        KeyError: If no node exists for the given key.

    Returns:
        Node: The node associated with the specified key.
    """

    node = dictionary.get(key)
    if node is None:
        raise KeyError(f"Could not obtain node with key: {key}")
    return node


def get_numeric_value(node: Node) -> Optional[int | float]:
    """
    Retrieve the numeric value of a node if available.

    Returns:
        Optional[int | float]: The node's value if the node exists and is numeric, otherwise None.
    """

    if node is None or not NodeProcessor.is_numeric_processor(node.processor):
        return None
    return node.processor.value


def get_numeric_node_with_value(key: str, dictionary: Dict[str, Node]) -> Tuple[Node, Optional[int | float]]:
    """
    Retrieve a node by key and return it along with its numeric value.

    Returns:
        Tuple[Node, Optional[int | float]]: The node and its numeric value,
        or None as the value if the node is not numeric or has no value.
    """

    node = get_node(key, dictionary)
    numeric_value = get_numeric_value(node)
    return (node, numeric_value)
