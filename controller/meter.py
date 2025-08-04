###########EXERTNAL IMPORTS############

from datetime import datetime
import asyncio
import math
import traceback
from typing import List, Dict, Set, Optional, Any
from enum import Enum
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node import NodeType, Node
from mqtt.client import MQTTMessage
from db.timedb import Measurement
from db.db import EnergyMeterRecord
from util.debug import LoggerManager
import util.functions as functions

#######################################


###############EXCEPTIONS##############


class UnitError(Exception):
    """Raised when a unit configuration or value is invalid."""

    pass


class NodeUnknownError(Exception):
    """Raised when a requested node does not exist or is unknown."""

    pass


class NodeMissingError(Exception):
    """Raised when a required node is missing during validation or runtime."""

    pass


class LoggingPeriodError(Exception):
    """Raised when at least two nodes of the same type have different logging periods."""

    pass


#############ENUMERATIONS##############


class EnergyMeterType(Enum):
    """
    Enumeration of supported energy meter configurations.

    Attributes:
        SINGLE_PHASE (str): Single-phase meter.
        THREE_PHASE (str): Three-phase meter.
    """

    SINGLE_PHASE = "SINGLE_PHASE"
    THREE_PHASE = "THREE_PHASE"


class PowerFactorDirection(Enum):
    """
    Enumeration representing the direction of power factor.

    Attributes:
        UNKNOWN (str): Power factor direction is unknown.
        UNITARY (str): Power factor is unitary (1.0).
        LAGGING (str): Power factor is lagging (inductive load).
        LEADING (str): Power factor is leading (capacitive load).
    """

    UNKNOWN = "UNKNOWN"
    UNITARY = "UNITARY"
    LAGGING = "LAGGING"
    LEADING = "LEADING"


################CLASSES################


@dataclass
class EnergyMeterOptions:
    """
    Configuration options for how an energy meter should behave.

    Attributes:
        read_energy_from_meter (bool): Whether to read energy directly from the meter.
        read_separate_forward_reverse_energy (bool): Whether to track forward and reverse energy separately.
        negative_reactive_power (bool): Whether the meter reads negative (leading) reactive power.
        frequency_reading (bool): Whether the meter provides frequency readings.
    """

    read_energy_from_meter: bool
    read_separate_forward_reverse_energy: bool
    negative_reactive_power: bool
    frequency_reading: bool

    def get_meter_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current energy meter options.

        Returns:
            Dict[str, Any]: A dictionary with all configuration flags and their values.
        """
        return asdict(self)


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
        Performs full validation of all nodes configured for the energy meter.

        This includes:
        - Validating node names and units against predefined valid options.
        - Ensuring calculated nodes have all required dependencies based on their type and phase.
        - Checking that power factor and power direction nodes are properly supported.
        - Verifying logging consistency across related nodes.
        """

        for node in self.nodes.values():
            EnergyMeterNodes.validate_node(node)

        if self.meter_type == EnergyMeterType.SINGLE_PHASE:
            for energy_type in ("active", "reactive"):
                self.validate_energy_nodes("", energy_type)

            for power_type in ("active", "reactive", "apparent"):
                self.validate_power_nodes("", power_type)

            self.validate_pf_nodes("")
            self.validate_pf_direction_nodes("")

        elif self.meter_type == EnergyMeterType.THREE_PHASE:
            for phase in ("l1_", "l2_", "l3_", "total_"):
                for energy_type in ("active", "reactive"):
                    self.validate_energy_nodes(phase, energy_type)

                for power_type in ("active", "reactive", "apparent"):
                    self.validate_power_nodes(phase, power_type)

                self.validate_pf_nodes(phase)
                self.validate_pf_direction_nodes(phase)

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

        if self.meter_type == EnergyMeterType.THREE_PHASE and phase == "total_":
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

        if self.meter_type == EnergyMeterType.THREE_PHASE and phase == "total_":
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

        if self.meter_type == EnergyMeterType.THREE_PHASE and phase == "total_":
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

        if self.meter_type == EnergyMeterType.THREE_PHASE and phase == "total_":
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


class EnergyMeter(Device):
    """
    Specialized device class representing an energy meter.

    This class extends the generic `Device` class to handle energy meter-specific logic,
    including real-time measurement, node calculation, logging, and MQTT publishing.

    Key Features:
    - Supports single-phase and three-phase configurations.
    - Automatically calculates derived values such as power, energy, power factor, and PF direction.
    - Integrates configurable options for reading raw or calculated energy values.
    - Handles logging of measurements and statistical data.
    - Publishes node values over MQTT in a structured format.

    Attributes:
        meter_type (EnergyMeterType): Type of the energy meter (single or three phase).
        meter_options (EnergyMeterOptions): Configuration flags controlling how energy and power are interpreted.
        meter_nodes (EnergyMeterNodes): Manager for validating and handling node configurations and relationships.
        calculation_methods (Dict[str, Tuple[Callable, Dict[str, Any]]]): Map of suffixes to calculation methods.
        disconnected_calculation (bool): Flag to make the device make one and only calculation of nodes on disconnection
    """

    @staticmethod
    def get_node_phase(node: Node) -> str:
        """
        Returns the phase prefix of a node based on its name.

        Identifies whether a node belongs to a specific phase (e.g., "l1_", "l2_", "l3_"),
        a line-to-line voltage (e.g., "l1_l2_"), or represents a totalized value ("total_").

        Args:
            node (Node): The node whose name is to be analyzed.

        Returns:
            str: The phase prefix ("l1_", "l2_", "l3_", "l1_l2_", etc.), or an empty string if none match.
        """
        for phase in ("l1_l2_", "l2_l3_", "l3_l1_", "l1_", "l2_", "l3_", "total_"):
            if node.name.startswith(phase):
                return phase
        return ""

    def __init__(
        self,
        id: int,
        name: str,
        protocol: str,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        meter_nodes: set[Node],
    ):

        super().__init__(id=id, name=name, protocol=protocol, publish_queue=publish_queue, measurements_queue=measurements_queue, nodes=meter_nodes)
        self.meter_type = meter_type
        self.meter_options = meter_options

        try:
            self.meter_nodes = EnergyMeterNodes(meter_type=self.meter_type, meter_options=self.meter_options, nodes=meter_nodes)
            self.meter_nodes.set_energy_nodes_incremental()
            self.meter_nodes.validate_nodes()
        except Exception as e:
            raise Exception(f"Failed to initialize EnergyMeter '{name}' with id {id}: {e}")

        self.calculation_methods = {
            "_reactive_energy": (self.calculate_energy, {"energy_type": "reactive"}),
            "_active_energy": (self.calculate_energy, {"energy_type": "active"}),
            "_active_power": (self.calculate_power, {"power_type": "active"}),
            "_reactive_power": (self.calculate_power, {"power_type": "reactive"}),
            "_apparent_power": (self.calculate_power, {"power_type": "apparent"}),
            "_power_factor_direction": (self.calculate_pf_direction, {}),
            "_power_factor": (self.calculate_pf, {}),
        }

        self.disconnected_calculation = False

    @abstractmethod
    def start(self) -> None:
        """
        Starts the energy meter device operations.

        This method should be implemented by subclasses to initialize and start
        the communication protocol, begin data acquisition, and set up any
        necessary background tasks for the specific meter type.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stops the energy meter device operations.

        This method should be implemented by subclasses to gracefully shutdown
        the communication protocol, stop data acquisition, and clean up any
        resources or background tasks associated with the specific meter type.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    async def process_nodes(self) -> None:
        """
        Executes the full data processing cycle for the energy meter.

        If the meter is connected:
            - Clears disconnected flag
            - Calculates all nodes
            - Logs and publishes values concurrently

        If the meter is disconnected and hasn't been processed since disconnection:
            - Runs one calculation pass
            - Sets disconnected flag to avoid repeated unnecessary processing
        """

        if self.connected:
            self.disconnected_calculation = False
            await self.calculate_nodes()
            await asyncio.gather(self.log_nodes(), self.publish_nodes())
        elif not self.disconnected_calculation:
            await self.calculate_nodes()
            self.disconnected_calculation = True

    async def log_nodes(self) -> None:
        """
        Logs node data for nodes with logging enabled if their logging period has elapsed.

        For each node:
            - Initializes logging timestamp on the first run.
            - Submits a log entry if the configured logging period has passed.
            - Resets directional energy for energy-related nodes not independently logged.
        """

        current_time = datetime.now()

        for node in self.meter_nodes.nodes.values():
            if not node.logging or not node.enabled:
                continue

            if node.last_log_datetime is None:
                node.last_log_datetime = current_time
                continue

            elapsed_time = functions.subtract_datetime_mins(current_time, node.last_log_datetime)

            if elapsed_time >= node.logging_period:
                log_data = [node.submit_log(current_time)]
                log_db = f"{self.name}_{self.id}"
                await self.measurements_queue.put(Measurement(db=log_db, data=log_data))
                self.reset_directional_energy(node)

    def reset_directional_energy(self, node: Node):
        """
        Resets the values of directional and incremental energy nodes if they are not independently logged.
        Also resets corresponding phase nodes if the parent node is a total node.

        Args:
            node (Node): The node used to identify related energy nodes.
        """

        prefix = EnergyMeter.get_node_phase(node)

        for energy_type in ("reactive", "active"):
            if f"_{energy_type}_energy" not in node.name:
                continue

            # Reset directional nodes (forward, reverse)
            for direction in ("forward", "reverse"):
                key = f"{prefix}{direction}_{energy_type}_energy"
                energy_node = self.meter_nodes.nodes.get(key)
                if energy_node and not energy_node.logging:
                    energy_node.reset_value()

            # If total_, also reset all phase equivalents
            if prefix == "total_":
                for p in ("l1_", "l2_", "l3_"):
                    # Directional
                    for direction in ("forward", "reverse"):
                        phase_dir_key = f"{p}{direction}_{energy_type}_energy"
                        phase_dir_node = self.meter_nodes.nodes.get(phase_dir_key)
                        if phase_dir_node and not phase_dir_node.logging:
                            phase_dir_node.reset_value()

                    # Normal incremental
                    phase_norm_key = f"{p}{energy_type}_energy"
                    phase_norm_node = self.meter_nodes.nodes.get(phase_norm_key)
                    if phase_norm_node and not phase_norm_node.logging:
                        phase_norm_node.reset_value()

    async def calculate_nodes(self) -> None:
        """
        Asynchronously calculates values for all nodes marked as 'calculated'.

        For each calculated node:
            - Determines its phase prefix.
            - Finds the appropriate calculation method based on its name.
            - Executes the calculation in a background thread to avoid blocking.

        Notes:
            Uses `asyncio.to_thread` to offload CPU-bound work.
        """

        logger = LoggerManager.get_logger(__name__)

        calculated_nodes: Dict[str, Node] = {name: node for name, node in self.meter_nodes.nodes.items() if node.calculated and node.enabled}
        tasks = []

        for node in calculated_nodes.values():

            prefix = EnergyMeter.get_node_phase(node)

            for key, (func, kwargs) in self.calculation_methods.items():
                if key in node.name:
                    tasks.append(asyncio.to_thread(func, node=node, prefix=prefix, **kwargs))
                    break

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_nodes = []

        for node, result in zip(calculated_nodes.values(), results):
            if isinstance(result, Exception):
                failed_nodes.append((node.name, result))
                continue

        if failed_nodes:
            for name, e in failed_nodes:
                tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                logger.exception(f"Failed to calculate node {name} from device {self.name} with id {self.id}:\n{tb}")

    def calculate_energy(self, prefix: str, energy_type: str, node: Node) -> None:
        """
        Calculates energy value based on the device configuration.

        For total nodes, the value is the sum of the three phase values.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            energy_type (str): "active" or "reactive".
            node (Node): The node that will receive the calculated value.
        """

        if prefix == "total_":
            total = 0.0
            for p in ("l1_", "l2_", "l3_"):
                phase_key = f"{p}{energy_type}_energy"
                phase_node = self.meter_nodes.nodes.get(phase_key)

                if phase_node is None or phase_node.value is None:
                    node.set_value(None)
                    return

                total += EnergyMeterNodes.get_scaled_value(phase_node)

            scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
            node.set_value(scaled_total)
            return

        if self.meter_options.read_separate_forward_reverse_energy:
            forward_key = f"{prefix}forward_{energy_type}_energy"
            reverse_key = f"{prefix}reverse_{energy_type}_energy"
            forward = self.meter_nodes.nodes.get(forward_key)
            reverse = self.meter_nodes.nodes.get(reverse_key)

            if forward and reverse and forward.value is not None and reverse.value is not None:
                scaled_forward = EnergyMeterNodes.get_scaled_value(forward)
                scaled_reverse = EnergyMeterNodes.get_scaled_value(reverse)
                scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_forward - scaled_reverse, node)
                node.set_value(scaled_value)

        elif not self.meter_options.read_energy_from_meter:
            power_key = f"{prefix}{energy_type}_power"
            power_node = self.meter_nodes.nodes.get(power_key)

            if power_node and power_node.value is not None and power_node.elapsed_time is not None:
                elapsed_hours = power_node.elapsed_time / 3600.0
                scaled_power = EnergyMeterNodes.get_scaled_value(power_node)
                scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_power * elapsed_hours, node)
                node.set_value(scaled_value)

    def calculate_power(self, prefix: str, power_type: str, node: Node):
        """
        Calculates the specified type of power (active, reactive, or apparent) for a given phase or total.

        Power calculations prioritize using known power values (e.g., active + reactive)
        over raw voltage/current inputs, which may be less reliable.

        For total nodes, the result is the sum of all corresponding phase power values.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            power_type (str): One of "active", "reactive", or "apparent".
            node (Node): The node to assign the calculated value to.
        """

        if prefix == "total_":
            total = 0.0
            for p in ("l1_", "l2_", "l3_"):
                phase_key = f"{p}{power_type}_power"
                phase_node = self.meter_nodes.nodes.get(phase_key)

                if phase_node is None or phase_node.value is None:
                    node.set_value(None)
                    return

                total += EnergyMeterNodes.get_scaled_value(phase_node)

            scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
            node.set_value(scaled_total)
            return

        # Individual phase
        v = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}voltage"))
        i = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}current"))
        pf_node = self.meter_nodes.nodes.get(f"{prefix}power_factor")
        pf = pf_node.value if pf_node else None

        p = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}active_power"))
        q = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}reactive_power"))
        s = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}apparent_power"))

        val = None

        if power_type == "apparent":
            if p is not None and q is not None:
                val = math.sqrt(p**2 + q**2)
            elif v is not None and i is not None:
                val = v * i

        elif power_type == "reactive":
            if s is not None and p is not None and s >= p:
                val = math.sqrt(s**2 - p**2)
            elif v is not None and i is not None and pf is not None and -1.0 <= pf <= 1.0:
                val = v * i * math.sin(math.acos(pf))

        elif power_type == "active":
            if s is not None and q is not None and s >= q:
                val = math.sqrt(s**2 - q**2)
            elif v is not None and i is not None and pf is not None:
                val = v * i * pf

        scaled_value = EnergyMeterNodes.apply_output_scaling(val, node) if val is not None else None
        node.set_value(scaled_value)

    def calculate_pf(self, prefix: str, node: Node) -> None:
        """
        Calculates the power factor (PF) for a given phase or total.

        Power factor is computed using the formula:
            PF = cos(atan(Q / P))
        where:
            P = active power
            Q = reactive power

        If P is zero, sets PF to 0.0 to avoid division by zero.
        If any required input is missing, sets PF to None.

        For total power factor, uses the sum of P and Q across all three phases.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            node (Node): The node that will receive the calculated PF value.
        """

        if prefix == "total_":
            total_p = 0.0
            total_q = 0.0

            for p in ("l1_", "l2_", "l3_"):
                p_node = self.meter_nodes.nodes.get(f"{p}active_power")
                q_node = self.meter_nodes.nodes.get(f"{p}reactive_power")

                p_val = EnergyMeterNodes.get_scaled_value(p_node) if p_node else None
                q_val = EnergyMeterNodes.get_scaled_value(q_node) if q_node else None

                if p_val is None or q_val is None:
                    node.set_value(None)
                    return

                total_p += p_val
                total_q += q_val

            if total_p == 0:
                node.set_value(0.0)
            else:
                node.set_value(math.cos(math.atan(total_q / total_p)))

            return

        # Per-phase PF
        p_node = self.meter_nodes.nodes.get(f"{prefix}active_power")
        q_node = self.meter_nodes.nodes.get(f"{prefix}reactive_power")

        p = EnergyMeterNodes.get_scaled_value(p_node) if p_node else None
        q = EnergyMeterNodes.get_scaled_value(q_node) if q_node else None

        if p is None or q is None:
            node.set_value(None)
            return

        if p == 0:
            node.set_value(0.0)
        else:
            node.set_value(math.cos(math.atan(q / p)))

    def calculate_pf_direction(self, prefix: str, node: Node) -> None:
        """
        Calculates the direction of the power factor for a given phase.

        The direction can be:
            - UNITARY: If PF is 1.0.
            - LAGGING or LEADING: Based on reactive power or reactive energy direction.
            - UNKNOWN: If direction cannot be determined.

        Logic flow:
            1. If PF >= 0.99 → UNTIARY and reset energy direction (if applicable).
            2. If using `negative_reactive_power`, sign of Q determines direction.
            3. If using `read_separate_forward_reverse_energy`, direction is taken from reactive energy trend.
            4. Otherwise, defaults to UNKNOWN.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_").
            node (Node): The node that will receive the calculated direction.
        """

        pf_node = self.meter_nodes.nodes.get(f"{prefix}power_factor")
        pf = pf_node.value if pf_node else None
        er_node = self.meter_nodes.nodes.get(f"{prefix}reactive_energy")

        if pf is not None and pf == 1.00:
            node.set_value(PowerFactorDirection.UNITARY.value)
            if er_node:
                er_node.reset_direction()
            return

        if self.meter_options.negative_reactive_power:
            q_node = self.meter_nodes.nodes.get(f"{prefix}reactive_power")

            if q_node and q_node.value is not None:
                node.set_value(PowerFactorDirection.LAGGING.value if q_node.value > 0.0 else PowerFactorDirection.LEADING.value)
            else:
                node.set_value(PowerFactorDirection.UNKNOWN.value)
            return

        elif self.meter_options.read_separate_forward_reverse_energy:
            if er_node:
                if er_node.positive_direction:
                    node.set_value(PowerFactorDirection.LAGGING.value)
                elif er_node.negative_direction:
                    node.set_value(PowerFactorDirection.LEADING.value)
                else:
                    node.set_value(PowerFactorDirection.UNKNOWN.value)
            else:
                node.set_value(PowerFactorDirection.UNKNOWN.value)
            return

        node.set_value(PowerFactorDirection.UNKNOWN.value)

    async def publish_nodes(self):
        """
        Publishes the current values of all nodes marked for publishing via MQTT.

        For each node:
            - Checks if the node is marked as `publish=True` and has a non-null value.
            - Serializes the node's data using `get_publish_format()`.

        The result is sent as a single MQTT message to a topic formatted as:
            "<device_name>_<device_id>_nodes"

        Raises:
            Exception: If `get_publish_format()` raises due to missing value.
        """

        publish_nodes: Dict[str, Node] = {name: node for name, node in self.meter_nodes.nodes.items() if node.publish and node.value is not None}

        topic = f"{self.name}_{self.id}_nodes"
        payload: Dict[str, Any] = {}

        for node in publish_nodes.values():
            payload[node.name] = node.get_publish_format()

        if payload:
            await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))

    def get_device_state(self) -> Dict[str, Any]:
        """
        Returns the current state of the energy meter device, including metadata and configuration.

        Returns:
            Dict[str, Any]: A dictionary containing the device's:
                - ID
                - Name
                - Protocol
                - Connection status
                - Meter options
                - Meter type
        """

        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
            "options": self.meter_options.get_meter_options(),
            "type": self.meter_type,
        }

    @abstractmethod
    def get_meter_record(self) -> EnergyMeterRecord:
        """
        Creates a database record representation of the energy meter configuration.

        Returns:
            EnergyMeterRecord: Record containing meter configuration and all associated nodes.
        """
        pass
