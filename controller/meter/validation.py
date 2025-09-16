###########EXTERNAL IMPORTS############

from typing import Dict, Set, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.types.device import EnergyMeterOptions, EnergyMeterType
from controller.types.node import NodeType
from controller.exceptions import UnitError, NodeUnknownError, NodeMissingError, LoggingPeriodError
from controller.node.node import Node
import util.functions.meter as meter_util

#######################################


def validate_node(node: Node, valid_nodes: Set[str], valid_units: Optional[Set[str]]) -> None:
    """
    Validates a node's name and unit against predefined valid options for energy meter nodes.

    Custom nodes bypass validation. For regular nodes, validates that the base name
    (without phase prefix) exists in valid_nodes and that units are appropriate for the node type.

    Args:
        node (Node): The node to validate
        valid_nodes (Set[str]): Set of valid base node names
        valid_units (Optional[Set[str]]): Set of valid units for this node type

    Raises:
        NodeUnknownError: If the node's base name is not recognized
        UnitError: If unit validation fails
    """

    if not node.config.custom:
        base_name = meter_util.remove_phase_string(node.config.name)

        if base_name not in valid_nodes:
            raise NodeUnknownError(f"Invalid node {node.config.name} with type {node.config.type}")

        if node.config.type is not NodeType.FLOAT and node.config.type is not NodeType.INT:
            if node.config.unit is None:
                return
            else:
                raise UnitError(f"Invalid unit '{node.config.unit}' for node '{node.config.name}'. Non numeric nodes can't have units")

        if valid_units is None:
            raise UnitError(f"Could not find valid units for node '{node.config.name}'")
        elif node.config.unit not in valid_units:
            raise UnitError(f"Invalid unit '{node.config.unit}' for node '{node.config.name}'. Expected one of {valid_units}")


def validate_logging_consistency(nodes: Dict[str, Node], node_to_check: Optional[Node] = None) -> None:
    """
    Validates that logging-enabled nodes in the same measurement category have consistent logging periods.

    Groups nodes by category (energy, power, voltage, current, frequency) and ensures
    all nodes within each category use the same logging period.

    Args:
        nodes (Dict[str, Node]): Dictionary of all nodes
        node_to_check (Optional[Node]): If provided, only validates this node's category

    Raises:
        LoggingPeriodError: If logging periods are inconsistent within a category
    """

    category_suffixes = {
        "energy": ("_energy",),
        "power": ("_power", "_power_factor", "power_factor_direction"),
        "voltage": ("_voltage",),
        "current": ("_current",),
        "frequency": ("_frequency",),
    }

    for category, suffixes in category_suffixes.items():

        if node_to_check is None:
            logging_nodes = [
                node
                for node in nodes.values()
                if any(node.config.name.endswith(suffix) for suffix in suffixes) and node.config.logging and not node.config.custom
            ]
        else:
            if not node_to_check.config.logging:
                return

            if not any(node_to_check.config.name.endswith(suffix) for suffix in suffixes):
                continue

            logging_nodes = [
                node
                for node in nodes.values()
                if any(node.config.name.endswith(suffix) for suffix in suffixes) and node.config.logging and not node.config.custom
            ]

        if not logging_nodes:
            continue

        expected_period = logging_nodes[0].config.logging_period
        mismatched = [node for node in logging_nodes if node.config.logging_period != expected_period]

        if mismatched:
            details = ", ".join(f"{n.config.name}={n.config.logging_period}min" for n in mismatched)
            raise LoggingPeriodError(
                f"Inconsistent logging periods in {category} group. Expected {expected_period}min in node {logging_nodes[0].config.name}. Got: {details}"
            )

        if node_to_check is not None:
            return


def validate_energy_nodes(
    phase: str, energy_type: str, nodes: Dict[str, Node], meter_type: EnergyMeterType, meter_options: EnergyMeterOptions
) -> None:
    """
    Validates that calculated energy nodes have required dependency nodes based on meter configuration.

    Checks different calculation methods: three-phase total summation, forward/reverse energy
    difference, or power-based energy integration.

    Args:
        phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "")
        energy_type (str): Energy type ("active" or "reactive")
        nodes (Dict[str, Node]): Dictionary of all available nodes
        meter_type (EnergyMeterType): Meter type (SINGLE_PHASE or THREE_PHASE)
        meter_options (EnergyMeterOptions): Configuration options

    Raises:
        NodeMissingError: If required dependency nodes are missing
    """

    node_name = f"{phase}{energy_type}_energy"
    node = nodes.get(node_name)

    if not node or not node.config.calculated or node.config.custom:
        return

    if meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
        missing_phases = []
        for p in ["l1_", "l2_", "l3_"]:
            sub_node = nodes.get(f"{p}{energy_type}_energy")
            if not sub_node:
                missing_phases.append(f"{p}{energy_type}_energy")
        if missing_phases:
            raise NodeMissingError(f"Missing phase energy nodes for {node_name} calculation: {', '.join(missing_phases)}.")
        return

    if meter_options.read_separate_forward_reverse_energy:
        forward = nodes.get(f"{phase}forward_{energy_type}_energy")
        reverse = nodes.get(f"{phase}reverse_{energy_type}_energy")

        if not (forward and reverse):
            raise NodeMissingError(f"Missing nodes for {node_name} calculation: expected forward and reverse energy nodes.")

    elif not meter_options.read_energy_from_meter:
        power_node = nodes.get(f"{phase}{energy_type}_power")
        if not power_node:
            raise NodeMissingError(f"Missing node for {node_name} calculation: expected {phase}{energy_type}_power.")


def validate_power_nodes(phase: str, power_type: str, nodes: Dict[str, Node], meter_type: EnergyMeterType) -> None:
    """
    Validates that calculated power nodes have sufficient input measurements for calculation.

    Checks different calculation paths based on available measurements:
    - Active power: (voltage + current + power_factor) or (apparent + reactive)
    - Reactive power: (voltage + current + power_factor) or (apparent + active)
    - Apparent power: (voltage + current) or (active + reactive)

    Args:
        phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "")
        power_type (str): Power type ("active", "reactive", or "apparent")
        nodes (Dict[str, Node]): Dictionary of all available nodes
        meter_type (EnergyMeterType): Meter type (SINGLE_PHASE or THREE_PHASE)

    Raises:
        NodeMissingError: If insufficient measurements are available for calculation
    """

    node_name = f"{phase}{power_type}_power"
    node = nodes.get(node_name)

    if not node or not node.config.calculated or node.config.custom:
        return

    if meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
        missing_phases = [f"{p}{power_type}_power" for p in ["l1_", "l2_", "l3_"] if not nodes.get(f"{p}{power_type}_power")]
        if missing_phases:
            raise NodeMissingError(f"Missing phase power nodes for {node_name} calculation: {', '.join(missing_phases)}.")
        return

    v = nodes.get(f"{phase}voltage")
    i = nodes.get(f"{phase}current")
    pf = nodes.get(f"{phase}power_factor")
    p = nodes.get(f"{phase}active_power")
    q = nodes.get(f"{phase}reactive_power")
    s = nodes.get(f"{phase}apparent_power")

    if power_type == "active":
        if (v and i and pf) or (s and q):
            return

    elif power_type == "reactive":
        if (v and i and pf) or (s and p):
            return

    elif power_type == "apparent":
        if (v and i) or (p and q):
            return

    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check dependencies.")


def validate_pf_nodes(phase: str, nodes: Dict[str, Node], meter_type: EnergyMeterType) -> None:
    """
    Validates that calculated power factor nodes have required active and reactive power measurements.

    For three-phase total, requires all individual phase power measurements.
    For individual phases, requires phase-specific active and reactive power.

    Args:
        phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "")
        nodes (Dict[str, Node]): Dictionary of all available nodes
        meter_type (EnergyMeterType): Meter type (SINGLE_PHASE or THREE_PHASE)

    Raises:
        NodeMissingError: If required power measurement nodes are missing
    """

    node_name = f"{phase}power_factor"
    node = nodes.get(node_name)

    if not node or not node.config.calculated or node.config.custom:
        return

    if meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
        missing = []
        for p in ["l1_", "l2_", "l3_"]:
            active = nodes.get(f"{p}active_power")
            reactive = nodes.get(f"{p}reactive_power")
            if not active:
                missing.append(f"{p}active_power")
            if not reactive:
                missing.append(f"{p}reactive_power")
        if missing:
            raise NodeMissingError(f"Missing nodes for {node_name} calculation: {', '.join(missing)}.")
    else:
        active_power = nodes.get(f"{phase}active_power")
        reactive_power = nodes.get(f"{phase}reactive_power")

        if not active_power or not reactive_power:
            raise NodeMissingError(f"Missing nodes for {node_name} calculation: expected {phase}active_power and {phase}reactive_power.")


def validate_pf_direction_nodes(phase: str, nodes: Dict[str, Node], meter_type: EnergyMeterType, meter_options: EnergyMeterOptions) -> None:
    """
    Validates that calculated power factor direction nodes have required measurements based on meter configuration.

    Different meter options require different measurements:
    - Negative reactive power mode: requires power_factor + reactive_power
    - Forward/reverse energy mode: requires power_factor + reactive_energy
    - Default mode: requires power_factor only

    Args:
        phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "")
        nodes (Dict[str, Node]): Dictionary of all available nodes
        meter_type (EnergyMeterType): Meter type (SINGLE_PHASE or THREE_PHASE)
        meter_options (EnergyMeterOptions): Configuration options affecting direction method

    Raises:
        NodeMissingError: If required dependency nodes are missing for the configured method
    """

    node_name = f"{phase}power_factor_direction"
    node = nodes.get(node_name)

    if not node or not node.config.calculated or node.config.custom:
        return

    if meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
        missing = []
        for p in ["l1_", "l2_", "l3_"]:
            pf = nodes.get(f"{p}power_factor")

            if meter_options.negative_reactive_power:
                q = nodes.get(f"{p}reactive_power")
                if not (pf and q):
                    if not pf:
                        missing.append(f"{p}power_factor")
                    if not q:
                        missing.append(f"{p}reactive_power")

            elif meter_options.read_separate_forward_reverse_energy:
                er = nodes.get(f"{p}reactive_energy")
                if not (pf and er):
                    if not pf:
                        missing.append(f"{p}power_factor")
                    if not er:
                        missing.append(f"{p}reactive_energy")

            else:
                if not pf:
                    missing.append(f"{p}power_factor")

        if missing:
            raise NodeMissingError(f"Missing nodes for {node_name} calculation: {', '.join(missing)}.")
        return

    pf = nodes.get(f"{phase}power_factor")

    if meter_options.negative_reactive_power:
        q = nodes.get(f"{phase}reactive_power")
        if pf and q:
            return

    elif meter_options.read_separate_forward_reverse_energy:
        er = nodes.get(f"{phase}reactive_energy")
        if pf and er:
            return

    elif pf:
        return

    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check dependencies.")
