###########EXERTNAL IMPORTS############

from typing import  Dict, Set, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.node import NodeType, Node
from controller.types import EnergyMeterType, EnergyMeterOptions
from controller.exceptions import *
import util.functions as functions

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
        Validates a node's name and unit according to predefined valid options for energy meter nodes.

        This method performs comprehensive validation for energy meter nodes with the following logic:

        1. **Custom Node Exemption**: Custom nodes (node.custom=True) bypass all validation
        2. **Base Name Extraction**: Removes phase prefixes from node names (e.g., "l1_voltage" → "voltage")
        3. **Node Name Validation**: Verifies the base name exists in the VALID_NODES set
        4. **Unit Validation by Node Type**:
           - Non-numeric nodes (BOOL, STRING): Must have unit=None (no units allowed)
           - Numeric nodes (FLOAT, INT): Must have a unit from the predefined valid units for that node type

        The validation ensures that only recognized energy meter parameters are used and that
        units are appropriate for the data type and measurement category.

        Args:
            node (Node): The node to validate containing name, type, unit, and custom flag.

        Raises:
            NodeUnknownError: If the node's base name is not found in VALID_NODES.
            UnitError: If unit validation fails for any of these scenarios:
                - Non-numeric nodes (BOOL/STRING) have a non-None unit
                - Numeric nodes (FLOAT/INT) have units not in VALID_UNITS for that node type
                - Numeric nodes have None units when valid units are defined for that node type

        Note:
            Custom nodes are completely exempt from validation and always considered valid.
            Phase prefixes like 'l1_', 'l2_', 'l3_', 'total_' are automatically stripped
            before validation to allow phase-specific variations of base measurements.
        """

        if node.custom:
            # Custom nodes are not validated against the predefined sets
            return

        base_name = functions.remove_phase_string(node.name)

        if base_name not in EnergyMeterNodes.VALID_NODES:
            raise NodeUnknownError(f"Invalid node {node.name} with type {node.type}")

        if node.type is not NodeType.FLOAT and node.type is not NodeType.INT:
            if node.unit is None:
                return
            else:
                raise UnitError(f"Invalid unit '{node.unit}' for node '{node.name}'. Non numeric nodes can't have units")

        valid_units = EnergyMeterNodes.VALID_UNITS.get(base_name)

        if valid_units is not None and node.unit not in valid_units:
            raise UnitError(f"Invalid unit '{node.unit}' for node '{node.name}'. Expected one of {valid_units}")

    @staticmethod
    def validate_logging_consistency(nodes: Dict[str, Node], node_to_check: Optional[Node] = None) -> None:
        """
        Validates that all logging-enabled nodes in the same measurement category
        (e.g., all energy nodes) have a consistent logging period.

        Categories:
            - Energy: nodes with names ending in '_energy'
            - Power:  nodes with names ending in '_power', '_power_factor' or _'power_factor_direction'
            - Voltage: nodes with names ending in '_voltage'
            - Current: nodes with names ending in '_current'
            - Frequency: nodes with names ending in '_frequency'

        Args:
            nodes (Dict[str, Node]): All nodes.
            node_to_check (Optional[Node]): If just one node needs to check

        Raises:
            LoggingPeriodError: If any mismatch is found across types or phases within a category.
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
                logging_nodes = [node for node in nodes.values() if any(node.name.endswith(suffix) for suffix in suffixes) and node.logging and not node.custom]
            else:
                if not node_to_check.logging:
                    return

                if not any(node_to_check.name.endswith(suffix) for suffix in suffixes):
                    continue

                logging_nodes = [node for node in nodes.values() if any(node.name.endswith(suffix) for suffix in suffixes) and node.logging and not node.custom]

            if not logging_nodes:
                continue

            expected_period = logging_nodes[0].logging_period
            mismatched = [node for node in logging_nodes if node.logging_period != expected_period]

            if mismatched:
                details = ", ".join(f"{n.name}={n.logging_period}min" for n in mismatched)
                raise LoggingPeriodError(
                    f"Inconsistent logging periods in {category} group. Expected {expected_period}min in node {logging_nodes[0].name}. Got: {details}"
                )

            if node_to_check is not None:
                return

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

    def validate_energy_nodes(self, phase: str, energy_type: str) -> None:
        """
        Validates whether the required dependencies are present for a calculated energy node.

        Depending on the meter's configuration, this function checks for:
        - Forward and reverse energy nodes (if configured to read them separately).
        - Corresponding power node (if energy is to be calculated from power).
        - For total energy nodes in 3F meters, validates that all three phase nodes exist.

        Args:
            phase (str): Phase prefix (e.g., "l1_", "l2_", "l3_", "total_", or "" for 1F meters).
            energy_type (str): Type of energy (e.g., "active", "reactive").

        Raises:
            NodeMissingError: If any required supporting nodes are missing for a calculated energy node.
        """

        node_name = f"{phase}{energy_type}_energy"
        node = self.nodes.get(node_name)

        if not node or not node.calculated or node.custom:
            return

        if self.meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
            missing_phases = []
            for p in ["l1_", "l2_", "l3_"]:
                sub_node = self.nodes.get(f"{p}{energy_type}_energy")
                if not sub_node:
                    missing_phases.append(f"{p}{energy_type}_energy")
            if missing_phases:
                raise NodeMissingError(f"Missing phase energy nodes for {node_name} calculation: {', '.join(missing_phases)}.")
            return

        if self.meter_options.read_separate_forward_reverse_energy:
            forward = self.nodes.get(f"{phase}forward_{energy_type}_energy")
            reverse = self.nodes.get(f"{phase}reverse_{energy_type}_energy")

            if not (forward and reverse):
                raise NodeMissingError(f"Missing nodes for {node_name} calculation: expected forward and reverse energy nodes.")

        elif not self.meter_options.read_energy_from_meter:
            power_node = self.nodes.get(f"{phase}{energy_type}_power")
            if not power_node:
                raise NodeMissingError(f"Missing node for {node_name} calculation: expected {phase}{energy_type}_power.")

    def validate_power_nodes(self, phase: str, power_type: str) -> None:
        """
        Validates that all required dependencies are present for calculating a given type of power node.

        Supports multiple calculation paths:
        - "active": (V, I, PF) OR (S and Q)
        - "reactive": (V, I, PF) OR (S and P)
        - "apparent": (V, I) OR (P and Q)
        - For total power nodes on 3F meters: validates that all three phases exist.

        Args:
            phase (str): Phase prefix (e.g., "l1_", "l2_", "l3_", "total_", or "" for 1F).
            power_type (str): Type of power ("active", "reactive", or "apparent").

        Raises:
            NodeMissingError: If required nodes for power calculation are missing.
        """
        node_name = f"{phase}{power_type}_power"
        node = self.nodes.get(node_name)

        if not node or not node.calculated or node.custom:
            return

        if self.meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
            missing_phases = [f"{p}{power_type}_power" for p in ["l1_", "l2_", "l3_"] if not self.nodes.get(f"{p}{power_type}_power")]
            if missing_phases:
                raise NodeMissingError(f"Missing phase power nodes for {node_name} calculation: {', '.join(missing_phases)}.")
            return

        v = self.nodes.get(f"{phase}voltage")
        i = self.nodes.get(f"{phase}current")
        pf = self.nodes.get(f"{phase}power_factor")
        p = self.nodes.get(f"{phase}active_power")
        q = self.nodes.get(f"{phase}reactive_power")
        s = self.nodes.get(f"{phase}apparent_power")

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

    def validate_pf_nodes(self, phase: str) -> None:
        """
        Validates that required nodes exist for calculating the power factor (PF) of a given phase.

        A calculated power factor requires:
            - Active power node
            - Reactive power node

        Args:
            phase (str): Phase prefix (e.g., "l1_", "l2_", "l3_", "total_", or "" for 1F).

        Raises:
            NodeMissingError: If any of the required nodes for PF calculation are missing.
        """

        node_name = f"{phase}power_factor"
        node = self.nodes.get(node_name)

        if not node or not node.calculated or node.custom:
            return

        if self.meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
            missing = []
            for p in ["l1_", "l2_", "l3_"]:
                active = self.nodes.get(f"{p}active_power")
                reactive = self.nodes.get(f"{p}reactive_power")
                if not active:
                    missing.append(f"{p}active_power")
                if not reactive:
                    missing.append(f"{p}reactive_power")
            if missing:
                raise NodeMissingError(f"Missing nodes for {node_name} calculation: {', '.join(missing)}.")
        else:
            active_power = self.nodes.get(f"{phase}active_power")
            reactive_power = self.nodes.get(f"{phase}reactive_power")

            if not active_power or not reactive_power:
                raise NodeMissingError(f"Missing nodes for {node_name} calculation: expected {phase}active_power and {phase}reactive_power.")

    def validate_pf_direction_nodes(self, phase: str):
        """
        Validates required dependencies for calculating power factor direction of a given phase or total.

        For individual phases:
            - If `negative_reactive_power` is enabled: requires power factor and reactive power.
            - If `read_separate_forward_reverse_energy` is enabled: requires power factor and reactive energy.
            - Otherwise: requires only power factor.

        For 3F total:
            - Validates that all three phases meet the same dependency criteria.

        Args:
            phase (str): Phase prefix (e.g., "l1_", "l2_", "l3_", "total_", or "" for 1F).

        Raises:
            NodeMissingError: If any required dependent node is missing for PF direction calculation.
        """

        node_name = f"{phase}power_factor_direction"
        node = self.nodes.get(node_name)

        if not node or not node.calculated or node.custom:
            return

        if self.meter_type is EnergyMeterType.THREE_PHASE and phase == "total_":
            missing = []
            for p in ["l1_", "l2_", "l3_"]:
                pf = self.nodes.get(f"{p}power_factor")

                if self.meter_options.negative_reactive_power:
                    q = self.nodes.get(f"{p}reactive_power")
                    if not (pf and q):
                        if not pf:
                            missing.append(f"{p}power_factor")
                        if not q:
                            missing.append(f"{p}reactive_power")

                elif self.meter_options.read_separate_forward_reverse_energy:
                    er = self.nodes.get(f"{p}reactive_energy")
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

        pf = self.nodes.get(f"{phase}power_factor")

        if self.meter_options.negative_reactive_power:
            q = self.nodes.get(f"{phase}reactive_power")
            if pf and q:
                return

        elif self.meter_options.read_separate_forward_reverse_energy:
            er = self.nodes.get(f"{phase}reactive_energy")
            if pf and er:
                return

        elif pf:
            return

        raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check dependencies.")

    def set_energy_nodes_incremental(self):
        """
        Marks all energy-related nodes as incremental.
        It applies to all nodes whose names contain the substring 'energy'.
        """

        for node in self.nodes.values():

            if "energy" in node.name:
                node.set_incremental_node(True)
