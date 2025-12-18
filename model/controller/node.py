###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol
from model.date import FormattedTimeStep

#######################################


class NodePhase(str, Enum):
    """
    Enumeration of supported electrical phases.

    Attributes:
        L1 (str): Phase L1.
        L2 (str): Phase L2.
        L3 (str): Phase L3.
        TOTAL (str): Total of all phases.
        GENERAL (str): Non-phase-specific.
        SINGLEPHASE (str): Single-phase system.
    """

    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    TOTAL = "Total"
    GENERAL = "General"
    SINGLEPHASE = "Singlephase"


class NodePrefix(str, Enum):
    """Enumeration of phase-related prefixes for node names.

    Attributes:
        L1 (str): Prefix for phase L1.
        L2 (str): Prefix for phase L2.
        L3 (str): Prefix for phase L3.
        L1_L2 (str): Prefix for line-to-line phases L1-L2.
        L1_L3 (str): Prefix for line-to-line phases L1-L3.
        L2_L1 (str): Prefix for line-to-line phases L2-L1.
        L2_L3 (str): Prefix for line-to-line phases L2-L3.
        L3_L1 (str): Prefix for line-to-line phases L3-L1.
        L3_L2 (str): Prefix for line-to-line phases L3-L2.
        TOTAL (str): Prefix for total/aggregated values.
        GENERAL (str): Prefix for general non-phase-specific nodes.
        SINGLEPHASE (str): Prefix for single-phase system nodes.
    """

    L1 = "l1_"
    L2 = "l2_"
    L3 = "l3_"
    L1_L2 = "l1_l2_"
    L1_L3 = "l1_l3_"
    L2_L1 = "l2_l1_"
    L2_L3 = "l2_l3_"
    L3_L1 = "l3_l1_"
    L3_L2 = "l3_l2_"
    TOTAL = "total_"
    GENERAL = ""
    SINGLEPHASE = ""


class NodeDirection(str, Enum):
    """
    Enumeration of supported directional measurements.

    Attributes:
        FORWARD (str): Forward direction measurement.
        REVERSE (str): Reverse direction measurement.
        TOTAL (str): Total of both directions.
    """

    FORWARD = "Forward"
    REVERSE = "Reverse"
    TOTAL = "Total"


class NodeType(str, Enum):
    """
    Internal enumeration of protocol-agnostic node data types.

    These types represent the normalized data categories used by the system
    after protocol-specific values (Modbus registers, OPC UA typed nodes, etc.)
    are decoded and mapped into a unified internal model.

    Attributes:
        INT (str): Integer value (signed or unsigned, any bit-width).
        FLOAT (str): Floating-point number (32-bit or 64-bit).
        BOOL (str): Boolean value.
        STRING (str): Text/string value.
    """

    INT = "INT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"
    STRING = "STRING"


class CounterMode(str, Enum):
    """
    Enumeration of supported counter processing modes.

    Attributes:
        DIRECT (str): Uses the incoming value as-is with no additional processing.
        DELTA (str): Treats the incoming value as an incremental change to be accumulated.
        CUMULATIVE (str): Treats the incoming value as a growing total from which deltas are derived.
    """

    DIRECT = "DIRECT"
    DELTA = "DELTA"
    CUMULATIVE = "CUMULATIVE"


"""Mapping from NodePhase enum values to their corresponding NodePrefix values."""
NODE_PHASE_TO_PREFIX_MAP = {
    NodePhase.L1: NodePrefix.L1,
    NodePhase.L2: NodePrefix.L2,
    NodePhase.L3: NodePrefix.L3,
    NodePhase.TOTAL: NodePrefix.TOTAL,
    NodePhase.GENERAL: NodePrefix.GENERAL,
    NodePhase.SINGLEPHASE: NodePrefix.SINGLEPHASE,
}


"""Mapping from NodeDirection enum values to their corresponding string prefixes."""
NODE_DIRECTION_TO_STR_MAP = {
    NodeDirection.FORWARD: "forward_",
    NodeDirection.REVERSE: "reverse_",
    NodeDirection.TOTAL: "",
}


@dataclass
class BaseNodeProtocolOptions:
    """
    Base class for protocol-specific node communication options.

    Serves as a common parent for all protocol option classes
    (e.g., ModbusRTUNodeOptions, OPCUANodeOptions), allowing the
    system to treat protocol-specific configurations in a unified way.

    This class intentionally contains no attributes and acts solely as a
    structural and typing anchor for protocol option subclasses.
    """

    pass

    def get_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current node protocol options

        Returns:
            Dict[str, Any]: A dictionary with all protocol options and it's values
        """

        return asdict(self)


@dataclass
class BaseNodeRecordConfig:
    """
    Base configuration for node records containing common attributes shared across all protocols.

    Defines core node behavior including publishing, alarms, logging, and counter features.
    Extended by protocol-specific configurations to add protocol-dependent attributes.
    """

    enabled: bool
    unit: str | None
    publish: bool
    calculated: bool
    custom: bool
    decimal_places: int | None
    logging: bool
    logging_period: int
    min_alarm: bool
    max_alarm: bool
    min_alarm_value: float | None
    max_alarm_value: float | None
    min_warning: bool
    max_warning: bool
    min_warning_value: float | None
    max_warning_value: float | None
    is_counter: bool
    counter_mode: CounterMode | None = None

    def get_config(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current node base configuration

        Returns:
            Dict[str, Any]: A dictionary with all base configurations and it's values
        """

        return asdict(self)

    @staticmethod
    def cast_from_dict(config_dict: Dict[str, Any]) -> "BaseNodeRecordConfig":
        """
        Construct BaseNodeRecordConfig from a persisted configuration dictionary.

        Converts stored primitive values into strongly typed base node configuration
        attributes, including counters, logging, and alarm thresholds. Assumes the
        input dictionary has already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into a valid base node
            configuration (e.g. due to corrupted or incompatible data).
        """

        try:
            enabled = bool(config_dict["enabled"])
            unit = str(config_dict["unit"]) if config_dict["unit"] is not None else None
            publish = bool(config_dict["publish"])
            calculated = bool(config_dict["calculated"])
            custom = bool(config_dict["custom"])
            decimal_places = int(config_dict["decimal_places"]) if config_dict["decimal_places"] is not None else None
            logging = bool(config_dict["logging"])
            logging_period = int(config_dict["logging_period"])
            min_alarm = bool(config_dict["min_alarm"])
            max_alarm = bool(config_dict["max_alarm"])
            min_alarm_value = float(config_dict["min_alarm_value"]) if config_dict["min_alarm_value"] is not None else None
            max_alarm_value = float(config_dict["max_alarm_value"]) if config_dict["max_alarm_value"] is not None else None
            min_warning = bool(config_dict["min_warning"])
            max_warning = bool(config_dict["max_warning"])
            min_warning_value = float(config_dict["min_warning_value"]) if config_dict["min_warning_value"] is not None else None
            max_warning_value = float(config_dict["max_warning_value"]) if config_dict["max_warning_value"] is not None else None
            is_counter = bool(config_dict["is_counter"])
            counter_mode = CounterMode(config_dict["counter_mode"]) if config_dict["counter_mode"] is not None else None
            return BaseNodeRecordConfig(
                enabled,
                unit,
                publish,
                calculated,
                custom,
                decimal_places,
                logging,
                logging_period,
                min_alarm,
                max_alarm,
                min_alarm_value,
                max_alarm_value,
                min_warning,
                max_warning,
                min_warning_value,
                max_warning_value,
                is_counter,
                counter_mode,
            )

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into Node Record Base Configuration: {e}.")


@dataclass
class NodeAttributes:
    """
    Holds domain-specific attributes for a node.

    Currently includes the node's electrical phase.
    Defaults to NodePhase.GENERAL if not specified.
    """

    phase: NodePhase = NodePhase.GENERAL

    def get_attributes(self) -> Dict[str, Any]:
        """
        Returns the node attributes as a dictionary.

        Returns:
            Dict[str, Any]: Dictionary containing all node attributes.
        """

        return asdict(self)

    @staticmethod
    def cast_from_dict(attributes_dict: Dict[str, Any]) -> "NodeAttributes":
        """
        Construct NodeAttributes from a persisted attributes dictionary.

        Converts stored primitive values into strongly typed node attribute
        representations. Assumes the input dictionary has already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid node
            attributes (e.g. due to corrupted or incompatible data).
        """

        try:
            phase = NodePhase(attributes_dict["phase"])
            return NodeAttributes(phase)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into Node Attributes: {e}.")


@dataclass
class NodeRecord:
    """
    Persistent representation of a device node configuration.

    Encapsulates both protocol-agnostic node settings and protocol-specific
    communication options. This record is used for database persistence and
    as an intermediate representation when reconstructing runtime Node
    instances via protocol-specific factories.

    Attributes:
        name: Unique node identifier within a device.
        protocol: Communication protocol associated with the node
            (e.g. "MODBUS_RTU", "OPC_UA", "NONE").
        config: Protocol-independent node configuration (type, units, logging,
            alarms, counters, display options, etc.).
        protocol_options: Protocol-specific communication configuration
            (e.g. Modbus register details or OPC UA NodeId).
        attributes: Domain-level metadata associated with the node
            (e.g. phase, direction).
        device_id: Identifier of the parent device in the database, if assigned.
    """

    name: str
    protocol: Protocol
    config: BaseNodeRecordConfig
    protocol_options: BaseNodeProtocolOptions
    attributes: NodeAttributes
    device_id: Optional[int] = None

    def __eq__(self, other):
        """
        Compares two NodeRecord instances for equality based on device_id and name.

        Args:
            other: Another object to compare with.

        Returns:
            bool: True if both objects are NodeRecord instances with same device_id and name.
        """

        if not isinstance(other, NodeRecord):
            return False
        return (self.device_id, self.name) == (other.device_id, other.name)

    def __hash__(self):
        """
        Returns hash value based on device_id and name for use in sets and dictionaries.

        Returns:
            int: Hash value of the (device_id, name) tuple.
        """

        return hash((self.device_id, self.name))

    def get_attributes(self) -> Dict[str, Any]:
        """
        Return a dictionary representation of the node record for persistence or serialization.
        """

        return {
            "name": self.name,
            "protocol": self.protocol,
            "config": self.config.get_config(),
            "protocol_options": self.protocol_options.get_options(),
            "attributes": self.attributes.get_attributes(),
            "device_id": self.device_id,
        }


@dataclass
class NodeLogs:
    """Container for node log data with metadata, time-series points, and computed metrics.

    Encapsulates the complete result of a node logs query, including raw data points,
    formatting configuration, and aggregate statistics. Used for both raw time-series
    data and time-bucketed (formatted) results with gap-filling.

    Attributes:
        unit: Measurement unit for the node values (e.g., "kWh", "V", "A", "°C").
        decimal_places: Number of decimal places for value precision, or None for no rounding.
        type: NodeType indicating the data type classification of the node.
        counter: Whether the node represents a counter value that increase over time.
        points: List of time-series data points, each containing timestamps and measured values.
        time_step: Time interval for bucketed data (e.g., hourly, daily), None for raw data.
        global_metrics: Computed statistics across all points (min/max values, averages, totals).
    """

    unit: Optional[str]
    decimal_places: Optional[int]
    type: NodeType
    is_counter: Optional[bool]
    points: List[Dict[str, Any]]
    time_step: Optional[FormattedTimeStep]
    global_metrics: Optional[Dict[str, Any]]

    def get_logs(self) -> Dict[str, Any]:
        """
        Returns the node logs as a dictionary.

        Returns:
            Dict[str, Any]: Dictionary containing all node logs attributes.
        """

        return asdict(self)


@dataclass
class NodeConfig:
    """
    Runtime configuration for a node, defining how its value is interpreted,
    displayed, logged, published, and monitored during device operation.

    Contains all internal settings required by the node processor, including
    type information, alarms, logging behavior, counter modes, and
    domain-specific attributes.

    Attributes:
        name: Node identifier within the device.
        type: Internal node type (INT, FLOAT, BOOL, STRING).
        unit: Measurement unit or None.
        protocol: Communication protocol used to read the node.
        enabled: Whether the node is active.
        is_counter: Marks the node as a counter-type measurement.
        counter_mode: Interpretation mode for counter values.
        publish: Whether the node's value should be published externally.
        calculated: True if the node is computed internally.
        custom: True if defined by the user.
        logging: Enables periodic logging.
        logging_period: Logging interval in seconds.
        min_alarm: Enable minimum-value alarm.
        max_alarm: Enable maximum-value alarm.
        min_alarm_value: Minimum threshold for alarms.
        max_alarm_value: Maximum threshold for alarms.
        min_warning_value: Minimum threshold for warnings.
        max_warning_value: Maximum threshold for warnings.
        decimal_places: Number of decimals for FLOAT display.
        attributes: Domain-specific attributes (e.g., electrical phase).
    """

    name: str
    type: NodeType
    unit: str | None
    protocol: Protocol = Protocol.NONE
    enabled: bool = True
    is_counter: bool = False
    counter_mode: CounterMode | None = None
    publish: bool = True
    calculated: bool = False
    custom: bool = False
    logging: bool = False
    logging_period: int = 15
    min_alarm: bool = False
    max_alarm: bool = False
    min_alarm_value: float | None = None
    max_alarm_value: float | None = None
    min_warning: bool = False
    max_warning: bool = False
    min_warning_value: float | None = None
    max_warning_value: float | None = None
    decimal_places: int | None = 3
    attributes: NodeAttributes = field(default_factory=NodeAttributes)

    @staticmethod
    def create_config_from_record(record: NodeRecord, internal_type: NodeType) -> "NodeConfig":
        """
        Builds a runtime NodeConfig from a stored NodeRecord.

        Applies field filtering, restores enums, assigns the inferred internal type,
        and reconstructs node attributes for use at runtime.

        Args:
            record: Persisted node record loaded from the database.
            internal_type: Internal node type inferred from protocol-specific options.

        Returns:
            NodeConfig: Fully initialized runtime configuration.
        """

        config = record.config.get_config()
        valid_fields = set(NodeConfig.__dataclass_fields__.keys())
        filtered_config = {
            k: v for k, v in config.items() if k in valid_fields and k not in ["unit", "name", "attributes", "protocol", "counter_mode"]
        }

        return NodeConfig(
            name=record.name,
            type=internal_type,
            unit=config["unit"],
            protocol=Protocol(record.protocol),
            counter_mode=CounterMode(config["counter_mode"]) if config["counter_mode"] else None,
            attributes=record.attributes,
            **filtered_config,
        )
    

    def validate(self) -> None:
        """
        Validates the node configuration and applies type-specific auto-fixes.

        Performs comprehensive validation including protocol validation, type-dependent
        attribute restrictions, alarm configuration validation, and logging period checks.
        Automatically fixes incompatible settings for non-numeric types.

        Raises:
            ValueError: If protocol is invalid, alarm settings are inconsistent,
                       logging period is invalid, or type-specific constraints are violated.
        """

        if self.protocol not in Protocol.valid_protocols():
            raise ValueError(f"Protocol {self.protocol} is not valid.")

        # Auto-fix for non numeric types
        if self.type in {NodeType.BOOL, NodeType.STRING}:
            self.is_counter = False
            self.counter_mode = None
            self.min_alarm = False
            self.max_alarm = False
            self.min_alarm_value = None
            self.max_alarm_value = None
            self.min_warning = False
            self.max_warning = False
            self.min_warning_value = None
            self.max_warning_value = None
            self.unit = None

        if self.type in {NodeType.BOOL, NodeType.STRING}:
            if self.is_counter:
                raise ValueError(f"counter node is not valid for {self.type.name} nodes.")
            if self.counter_mode is not None:
                raise ValueError("Counter mode is not applicable to non counter nodes.")
            if (
                self.min_alarm
                or self.max_alarm
                or self.min_warning
                or self.max_warning
                or self.min_alarm_value is not None
                or self.min_warning_value is not None
                or self.max_alarm_value is not None
                or self.max_warning_value is not None
            ):
                raise ValueError(f"Alarms and Warnings are not supported for {self.type.name} nodes.")
            if self.unit is not None:
                raise ValueError(f"Non null unit is not applicable to {self.type.name} nodes.")

        if self.is_counter:
            if self.min_alarm or self.min_warning or self.max_alarm or self.max_warning:
                raise ValueError("Alarms and Warnings are not applicable to counter nodes.")

        if self.min_alarm and self.min_alarm_value is None:
            raise ValueError("min_alarm is enabled but min_alarm_value is None.")

        if self.max_alarm and self.max_alarm_value is None:
            raise ValueError("max_alarm is enabled but max_alarm_value is None.")

        if self.min_warning and self.min_warning_value is None:
            raise ValueError("min_warning is enabled but min_warning_value is None.")

        if self.max_warning and self.max_warning_value is None:
            raise ValueError("max_warning is enabled but max_warning_value is None.")

        if self.logging and (not isinstance(self.logging_period, int) or self.logging_period <= 0):
            raise ValueError(f"Invalid logging period '{self.logging_period}' for node '{self.name}'. Must be a positive integer.")

        # Auto-fix for non decimal places types
        if self.type is not NodeType.FLOAT:
            self.decimal_places = None

        if self.type is NodeType.FLOAT and not isinstance(self.decimal_places, int):
            raise ValueError(f"decimal_places must be an int for FLOAT nodes, got {type(self.decimal_places).__name__}")
