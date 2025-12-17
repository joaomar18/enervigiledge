###########EXTERNAL IMPORTS############

from typing import List, Dict, Tuple, Optional, Any

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from controller.node.processor.processor import NodeProcessor
from model.controller.device import EnergyMeterType
from model.date import TimeSpanParameters
import util.functions.date as date
from model.controller.node import NodePhase, NodePrefix, NodeDirection, NodeAttributes, NODE_PHASE_TO_PREFIX_MAP, NODE_DIRECTION_TO_STR_MAP

#######################################


def get_node_prefix(node: Optional[Node] = None, phase: Optional[NodePhase] = None) -> str:
    """Returns the phase-related prefix from a node's name or for a specific phase.

    Checks if the node's name starts with a known NodePrefix value (e.g. "l1_", "l2_",
    "total_") and returns it. If a phase is provided instead, returns the corresponding
    prefix for that phase using NODE_PHASE_TO_PREFIX_MAP.

    Args:
        node: Optional Node object to extract the prefix from by checking its name.
        phase: Optional NodePhase to get the corresponding prefix for directly.

    Returns:
        Phase-related prefix string (e.g., "l1_", "l2_", "total_") or empty string
        if no match is found or no arguments are provided.
    """

    if node:
        for prefix in NodePrefix:
            if node.config.name.startswith(prefix.value):
                return prefix.value

    if phase:
        return NODE_PHASE_TO_PREFIX_MAP[phase].value

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


def create_node_name(base_name: str, phase: NodePhase, direction: Optional[NodeDirection]) -> str:
    """Constructs a node name by combining phase, direction, and base name.

    Args:
        base_name: The base name of the node.
        phase: NodePhase enum for the phase prefix (e.g., "l1_", "total_").
        direction: Optional NodeDirection enum for direction prefix (e.g., "forward_").

    Returns:
        str: Node name like "l1_forward_active_energy" or "total_power".
    """

    return NODE_PHASE_TO_PREFIX_MAP[phase].value + (NODE_DIRECTION_TO_STR_MAP[direction] if direction else "") + base_name


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


def get_empty_log_points(numeric: bool, incremental: bool, time_span: TimeSpanParameters):
    """Generates empty placeholder log points with null values for aligned time buckets.

    Args:
        numeric: If True, uses numeric value fields; otherwise uses generic value field.
        incremental: If True with numeric, returns single 'value'; if False, returns
            'average_value', 'min_value', 'max_value'.
        time_span: Time range parameters. Returns empty list if formatted is False.

    Returns:
        List[Dict[str, Any]]: Empty points with null values and ISO-formatted timestamps.
    """

    empty_points: List[Dict[str, Any]] = []
    if time_span.formatted and time_span.start_time and time_span.end_time and time_span.time_step:

        for bucket_start, bucket_end in date.get_aligned_time_buckets(
            start_time=time_span.start_time, end_time=time_span.end_time, time_step=time_span.time_step, time_zone=time_span.time_zone
        ):

            if numeric and incremental:

                point = {
                    "start_time": date.to_iso_minutes(bucket_start),
                    "end_time": date.to_iso_minutes(bucket_end),
                    "value": None,
                }
            elif numeric:

                point = {
                    "start_time": date.to_iso_minutes(bucket_start),
                    "end_time": date.to_iso_minutes(bucket_end),
                    "average_value": None,
                    "min_value": None,
                    "max_value": None,
                }
            else:

                point = {
                    "start_time": date.to_iso_minutes(bucket_start),
                    "end_time": date.to_iso_minutes(bucket_end),
                    "value": None,
                }

            empty_points.append(point)

    return empty_points


def get_empty_log_global_metrics(numeric: bool, incremental: bool) -> Dict[str, Any]:
    """Generates an empty placeholder dictionary for global log metrics.

    Args:
        numeric: If True, uses numeric metric fields; otherwise uses a generic value field.
        incremental: If True with numeric, returns a single 'value' field; if False,
            returns aggregated numeric fields including averages and extrema.

    Returns:
        Dict[str, Any]: Dictionary containing global metric fields initialized to None.
    """

    if numeric and incremental:
        return {"value": None}

    elif numeric:
        return {
            "average_value": None,
            "min_value": None,
            "max_value": None,
            "min_value_start_time": None,
            "min_value_end_time": None,
            "max_value_start_time": None,
            "max_value_end_time": None,
        }

    else:
        return {"value": None}
