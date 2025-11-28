###########EXERTNAL IMPORTS############

from typing import Dict, Set, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from model.controller.device import EnergyMeterType, EnergyMeterOptions
from model.controller.node import NodeDirection
from controller.exceptions import *
import controller.meter.validation as validation
import util.functions.meter as meter_util

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
        "frequency": {"Hz"},
        "active_energy": {"Wh", "kWh"},
        "reactive_energy": {"VArh", "kVArh"},
        "forward_active_energy": {"Wh", "kWh"},
        "reverse_active_energy": {"Wh", "kWh"},
        "forward_reactive_energy": {"VArh", "kVArh"},
        "reverse_reactive_energy": {"VArh", "kVArh"},
    }

    @staticmethod
    def validate_node(node: Node) -> None:
        """
        Validates a node's name and unit against energy meter standards.

        Args:
            node (Node): The node to validate for name and unit compliance.

        Raises:
            NodeUnknownError: If the node name is not recognized.
            UnitError: If the node unit is invalid for the node type.
        """
        validation.validate_node(
            node, EnergyMeterNodes.VALID_NODES, EnergyMeterNodes.VALID_UNITS.get(meter_util.remove_phase_string(node.config.name))
        )

    @staticmethod
    def validate_logging_consistency(nodes: Dict[str, Node], node_to_check: Optional[Node] = None) -> None:
        """
        Validates that nodes in the same measurement category have consistent logging periods.

        Args:
            nodes (Dict[str, Node]): Dictionary of all nodes to validate.
            node_to_check (Optional[Node]): Specific node to focus validation on, if any.

        Raises:
            LoggingPeriodError: If nodes have inconsistent logging periods within the same category.
        """

        validation.validate_logging_consistency(nodes, node_to_check)

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
        self.nodes: Dict[str, Node] = {node.config.name: node for node in nodes}

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
                for direction in NodeDirection:
                    self.validate_energy_nodes("", energy_type, direction)

            for power_type in ("active", "reactive", "apparent"):
                self.validate_power_nodes("", power_type)

            self.validate_pf_nodes("")

        elif self.meter_type is EnergyMeterType.THREE_PHASE:
            for phase in ("l1_", "l2_", "l3_", "total_"):
                for energy_type in ("active", "reactive"):
                    for direction in NodeDirection:
                        self.validate_energy_nodes(phase, energy_type, direction)

                for power_type in ("active", "reactive", "apparent"):
                    self.validate_power_nodes(phase, power_type)

                self.validate_pf_nodes(phase)
        else:
            raise MeterError(f"Meter type {self.meter_type} is not valid")

        EnergyMeterNodes.validate_logging_consistency(self.nodes)

    def validate_energy_nodes(self, phase: str, energy_type: str, energy_direction: NodeDirection) -> None:
        """
        Validates energy nodes for the specified phase and energy type configuration.

        Args:
            phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "" for single-phase).
            energy_type (str): Type of energy to validate ("active" or "reactive").
            energy_direction (NodeDirection): Direction to validate ("forward", "reverse" or "total").

        Raises:
            NodeMissingError: If required energy nodes are missing for the configuration.
        """

        validation.validate_energy_nodes(phase, energy_type, energy_direction, self.nodes, self.meter_type, self.meter_options)

    def validate_power_nodes(self, phase: str, power_type: str) -> None:
        """
        Validates power nodes for the specified phase and power type configuration.

        Args:
            phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "" for single-phase).
            power_type (str): Type of power to validate ("active", "reactive", or "apparent").

        Raises:
            NodeMissingError: If required power nodes are missing for the configuration.
        """

        validation.validate_power_nodes(phase, power_type, self.nodes, self.meter_type)

    def validate_pf_nodes(self, phase: str) -> None:
        """
        Validates power factor nodes for the specified phase configuration.

        Args:
            phase (str): Phase prefix ("l1_", "l2_", "l3_", "total_", or "" for single-phase).

        Raises:
            NodeMissingError: If required power factor nodes are missing for the configuration.
        """

        validation.validate_pf_nodes(phase, self.nodes, self.meter_type)
