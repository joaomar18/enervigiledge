###########EXERTNAL IMPORTS############

from typing import Dict, Set, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.node import Node
from controller.types import EnergyMeterType, EnergyMeterOptions
from controller.exceptions import *
import controller.meter_validation as validation
import util.functions.generic as generic

#######################################


class EnergyMeterNodes:
    """
    Validates and manages energy meter node configurations and relationships.

    This class ensures all required nodes are defined and logically valid depending
    on the type of meter and its configuration options.
    """

    VALID_NODES: Set[str] = {
        "voltage",
        "current",
        "active_power",
        "reactive_power",
        "apparent_power",
        "power_factor",
        "power_factor_direction",
        "frequency",
        "active_energy",
        "reactive_energy",
        "forward_active_energy",
        "reverse_active_energy",
        "forward_reactive_energy",
        "reverse_reactive_energy",
    }

    VALID_UNITS: Dict[str, Set[str]] = {
        "voltage": {"V"},
        "current": {"mA", "A"},
        "active_power": {"W", "kW"},
        "reactive_power": {"VAr", "kVAr"},
        "apparent_power": {"VA", "kVA"},
        "power_factor": {""},
        "power_factor_direction": {""},
        "frequency": {"Hz"},
        "active_energy": {"Wh", "kWh"},
        "reactive_energy": {"VArh", "kVArh"},
        "forward_active_energy": {"Wh", "kWh"},
        "reverse_active_energy": {"Wh", "kWh"},
        "forward_reactive_energy": {"VArh", "kVArh"},
        "reverse_reactive_energy": {"VArh", "kVArh"},
    }

    UNIT_PREFIXES = {'m': 1e-3, '': 1.0, 'k': 1e3, 'M': 1e6, 'G': 1e9}

    @staticmethod
    def validate_node(node: Node) -> None:
        """
        Validates a node's name and unit against energy meter standards.
        """
        validation.validate_node(node, EnergyMeterNodes.VALID_NODES, EnergyMeterNodes.VALID_UNITS.get(generic.remove_phase_string(node.name)))

    @staticmethod
    def validate_logging_consistency(nodes: Dict[str, Node], node_to_check: Optional[Node] = None) -> None:
        """
        Validates that nodes in the same measurement category have consistent logging periods.
        """

        validation.validate_logging_consistency(nodes, node_to_check)

    @staticmethod
    def get_scaled_value(node: Optional[Node]) -> Optional[float]:
        """
        Returns the node value scaled by its unit prefix (if any).

        For example:
            - 'kW' → multiplies by 1e3
            - 'mA' → multiplies by 1e-3
            - 'V' → treated as no prefix (1.0)

        Args:
            node (Optional[Node]): The node with a value and unit.

        Returns:
            Optional[float]: Scaled numeric value, or None if not applicable.
        """

        if not node or node.value is None:
            return None

        unit = getattr(node, 'unit', '')
        if not unit or not unit[0].isalpha():
            factor = 1.0
        else:
            factor = EnergyMeterNodes.UNIT_PREFIXES.get(unit[0], 1.0)

        return node.value * factor

    @staticmethod
    def apply_output_scaling(value: float, node: Node) -> float:
        """
        Scales a calculated value to match the unit prefix of the target node.

        This is typically used to convert values expressed in base units (e.g., W, Wh)
        to prefixed units (e.g., kW, kWh) according to the target node's unit.

        For example:
            - If node.unit is 'kWh', the value will be divided by 1e3.
            - If node.unit is 'MWh', the value will be divided by 1e6.

        Args:
            value (float): The calculated value to be scaled.
            node (Node): The target node whose unit prefix determines the scaling.

        Returns:
            float: The value scaled to the target node's unit prefix.
        """

        prefix = node.unit[0] if hasattr(node, 'unit') and node.unit and node.unit[0].isalpha() else ''
        factor = EnergyMeterNodes.UNIT_PREFIXES.get(prefix, 1.0)
        return value / factor

    def __init__(self, meter_type: EnergyMeterType, meter_options: EnergyMeterOptions, nodes: set[Node]):
        """
        Initializes the energy meter node manager with configuration and node definitions.

        Args:
            meter_type (EnergyMeterType): Specifies whether the meter is single-phase or three-phase.
            meter_options (EnergyMeterOptions): Configuration options controlling how data is read and processed.
            nodes (set[Node]): A set of Node objects representing all measurable parameters.

        Notes:
            - Nodes are stored internally as a dictionary for fast access using their `name`.
            - No validation is performed here; call `validate_nodes()` after construction.
        """

        self.meter_type = meter_type
        self.meter_options = meter_options
        self.nodes: Dict[str, Node] = {node.name: node for node in nodes}

    def validate_nodes(self) -> None:
        """
        Performs comprehensive validation of all nodes configured for the energy meter.

        Validates node names, units, dependencies, and logging consistency based on
        meter type (single-phase or three-phase) and configuration options.

        Raises:
            NodeUnknownError: If any node name is not recognized.
            UnitError: If any node has an invalid unit.
            NodeMissingError: If required dependency nodes are missing.
            LoggingPeriodError: If nodes have inconsistent logging periods.
            MeterError: If the meter type is invalid.
        """

        for node in self.nodes.values():
            EnergyMeterNodes.validate_node(node)

        if self.meter_type is EnergyMeterType.SINGLE_PHASE:
            for energy_type in ("active", "reactive"):
                self.validate_energy_nodes("", energy_type)

            for power_type in ("active", "reactive", "apparent"):
                self.validate_power_nodes("", power_type)

            self.validate_pf_nodes("")
            self.validate_pf_direction_nodes("")

        elif self.meter_type is EnergyMeterType.THREE_PHASE:
            for phase in ("l1_", "l2_", "l3_", "total_"):
                for energy_type in ("active", "reactive"):
                    self.validate_energy_nodes(phase, energy_type)

                for power_type in ("active", "reactive", "apparent"):
                    self.validate_power_nodes(phase, power_type)

                self.validate_pf_nodes(phase)
                self.validate_pf_direction_nodes(phase)
        else:
            raise MeterError(f"Meter type is not valid")

        EnergyMeterNodes.validate_logging_consistency(self.nodes)

    def validate_energy_nodes(self, phase: str, energy_type: EnergyMeterType) -> None:
        """
        Validates energy nodes for the specified phase and energy type configuration.
        """

        validation.validate_energy_nodes(phase, energy_type, self.nodes, self.meter_type, self.meter_options)

    def validate_power_nodes(self, phase: str, power_type: str) -> None:
        """
        Validates power nodes for the specified phase and power type configuration.
        """

        validation.validate_power_nodes(phase, power_type, self.nodes, self.meter_type)

    def validate_pf_nodes(self, phase: str) -> None:
        """
        Validates power factor nodes for the specified phase configuration.
        """

        validation.validate_pf_nodes(phase, self.nodes, self.meter_type)

    def validate_pf_direction_nodes(self, phase: str) -> None:
        """
        Validates power factor direction nodes for the specified phase configuration.
        """

        validation.validate_pf_direction_nodes(phase, self.nodes, self.meter_type, self.meter_options)

    def set_energy_nodes_incremental(self):
        """
        Marks all energy-related nodes as incremental.
        It applies to all nodes whose names contain the substring 'energy'.
        """

        for node in self.nodes.values():

            if "energy" in node.name:
                node.set_incremental_node(True)
