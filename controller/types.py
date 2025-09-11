###########EXTERNAL IMPORTS############

from enum import Enum
import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any, Set, Callable

#######################################

#############LOCAL IMPORTS#############

#######################################


##########     A B S T R A C T     E N U M S     &     D A T A     C L A S S E S     ##########


class Protocol(str, Enum):
    """
    Enumeration of supported device communication protocols.
    """

    NONE = "NONE"
    OPC_UA = "OPC_UA"
    MQTT = "MQTT"
    MODBUS_TCP = "MODBUS_TCP"
    MODBUS_RTU = "MODBUS_RTU"

    @classmethod
    def valid_protocols(cls) -> set[str]:
        """Returns a set of all valid protocol string values."""
        return {p.value for p in cls}


##########     N O D E     E N U M S     &     D A T A     C L A S S E S     ##########


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


class NodeType(str, Enum):
    """
    Enumeration of supported node data types.

    Attributes:
        INT (str): Integer data type.
        FLOAT (str): Floating point number.
        BOOL (str): Boolean value.
        STRING (str): String/text value.
    """

    INT = "INT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"
    STRING = "STRING"


@dataclass
class BaseNodeRecordConfig:
    """
    Base configuration for node records containing common attributes shared across all protocols.

    Defines core node behavior including data type, publishing, alarms, logging, and incremental features.
    Extended by protocol-specific configurations to add protocol-dependent attributes.
    """

    enabled: bool
    type: NodeType
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
    incremental_node: bool | None
    positive_incremental: bool | None
    calculate_increment: bool | None

    def get_config(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current node base configuration

        Returns:
            Dict[str, Any]: A dictionary with all base configurations and it's values
        """
        return asdict(self)


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
        """
        return asdict(self)


@dataclass
class NodeRecord:
    """
    Database record for a node configuration with protocol-specific attributes.

    Stores all configuration data needed to recreate a Node instance, including
    base configuration (BaseNodeRecordConfig) and protocol-specific attributes.
    Used for database persistence and Node factory methods.

    Attributes:
        name (str): Unique node identifier within the device.
        protocol (str): Communication protocol (MODBUS_RTU, OPC_UA, NONE, etc.).
        config (Dict[str, Any]): Complete configuration dictionary containing:
        attributes (Dict[str, Any]): Domain-specific attributes (e.g. {"phase": "L1"}).
            - Base attributes: type, unit, enabled, publish, alarms, logging, etc.
            - Protocol-specific: register (Modbus), node_id (OPC UA), etc.
        device_id (Optional[int]): Database ID of the parent device.
    """

    name: str
    protocol: str
    config: Dict[str, Any]
    attributes: Dict[str, Any]
    device_id: Optional[int] = None

    def __eq__(self, other):
        if not isinstance(other, NodeRecord):
            return False
        return (self.device_id, self.name) == (other.device_id, other.name)

    def __hash__(self):
        return hash((self.device_id, self.name))


@dataclass
class NodeConfig:
    """
    Configuration for a node within a device.

    Defines both functional behavior (type, unit, protocol, alarms, logging) and
    domain-specific attributes (e.g. phase) used to manage and validate node data.
    This configuration is used when creating and initializing Node instances.

    Attributes:
        name (str): Unique node identifier within the device.
        type (NodeType): The data type of the node (FLOAT, BOOL, STRING, etc.).
        unit (str | None): Measurement unit (e.g. "V", "A", "W") or None.
        protocol (Protocol): Communication protocol for the node (default NONE).
        enabled (bool): Whether the node is active and should be polled.
        incremental_node (bool | None): Marks if the node represents a cumulative counter.
        positive_incremental (bool | None): If True, only positive increments are allowed.
        calculate_increment (bool | None): Whether to calculate incremental deltas from values.
        publish (bool): Whether to publish the node's values externally (e.g. MQTT).
        calculated (bool): True if this node is derived from other nodes instead of hardware.
        custom (bool): Whether the node is user-defined.
        logging (bool): Whether to log values periodically to the database.
        logging_period (int): Period in seconds for logging when enabled.
        min_alarm (bool): Whether to trigger an alarm if value drops below min_alarm_value.
        max_alarm (bool): Whether to trigger an alarm if value exceeds max_alarm_value.
        min_alarm_value (float | None): Minimum threshold for alarm triggering.
        max_alarm_value (float | None): Maximum threshold for alarm triggering.
        decimal_places (int | None): Number of decimal places to display (FLOAT only).
        attributes (NodeAttributes): Domain-specific metadata (e.g. phase).
        on_value_change (Callable | None): Optional callback when node value changes.

    Methods:
        validate(): Validates configuration values and enforces type-dependent rules.
    """

    name: str
    type: NodeType
    unit: str | None
    protocol: Protocol = Protocol.NONE
    enabled: bool = True
    incremental_node: bool | None = False
    positive_incremental: bool | None = False
    calculate_increment: bool | None = True
    publish: bool = True
    calculated: bool = False
    custom: bool = False
    logging: bool = False
    logging_period: int = 15
    min_alarm: bool = False
    max_alarm: bool = False
    min_alarm_value: float | None = 0.0
    max_alarm_value: float | None = 0.0
    decimal_places: int | None = 3
    attributes: NodeAttributes = field(default_factory=NodeAttributes)
    on_value_change: Callable[[], None] | None = None

    def validate(self) -> None:
        if self.protocol not in Protocol.valid_protocols():
            raise ValueError(f"Protocol {self.protocol} is not valid.")

        # Auto-fix for non numeric types
        if self.type in {NodeType.BOOL, NodeType.STRING}:
            self.incremental_node = False
            self.positive_incremental = None
            self.calculate_increment = None
            self.min_alarm = False
            self.max_alarm = False
            self.min_alarm_value = None
            self.max_alarm_value = None
            self.unit = None

        if self.type in {NodeType.BOOL, NodeType.STRING}:
            if self.incremental_node:
                raise ValueError(f"incremental_node is not valid for {self.type.name} nodes.")
            if self.positive_incremental is not None or self.calculate_increment is not None:
                raise ValueError("Incremental node options are not applicable to non incremental nodes.")
            if self.min_alarm or self.max_alarm or self.min_alarm_value is not None or self.max_alarm_value is not None:
                raise ValueError(f"Alarms are not supported for {self.type.name} nodes.")
            if self.unit is not None:
                raise ValueError(f"Non null unit is not applicable to {self.type.name} nodes.")

        if self.incremental_node:
            if self.min_alarm or self.max_alarm:
                raise ValueError("Alarms are not applicable to incremental nodes.")

        if self.min_alarm and self.min_alarm_value is None:
            raise ValueError("min_alarm is enabled but min_alarm_value is None.")

        if self.max_alarm and self.max_alarm_value is None:
            raise ValueError("max_alarm is enabled but max_alarm_value is None.")

        if self.logging and (not isinstance(self.logging_period, int) or self.logging_period <= 0):
            raise ValueError(f"Invalid logging period '{self.logging_period}' for node '{self.name}'. Must be a positive integer.")

        # Auto-fix for non decimal places types
        if self.type is not NodeType.FLOAT:
            self.decimal_places = None

        if self.type is NodeType.FLOAT and not isinstance(self.decimal_places, int):
            raise ValueError(f"decimal_places must be an int for FLOAT nodes, got {type(self.decimal_places).__name__}")


##########     D E V I C E     A N D     E N E R G Y     M E T E R     E N U M S     &     D A T A     C L A S S E S     ##########


class EnergyMeterType(str, Enum):
    """
    Enumeration of supported energy meter configurations.

    Attributes:
        SINGLE_PHASE (str): Single-phase meter.
        THREE_PHASE (str): Three-phase meter.
    """

    SINGLE_PHASE = "SINGLE_PHASE"
    THREE_PHASE = "THREE_PHASE"


class PowerFactorDirection(str, Enum):
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


@dataclass
class EnergyMeterRecord:
    """
    Represents the full configuration of an energy meter for persistence in SQLite.

    Attributes:
        id (int | None): id of the device, when inserting leave None
        name (str): Human-readable name of the energy meter (e.g., "OR-WE-516 Energy Meter").
        protocol (Protocol): Communication protocol used by the meter (e.g., "modbus_rtu", "opcua").
        type (EnergyMeterType): Type of the meter, typically based on electrical configuration (e.g., "three_phase", "single_phase").
        options (Dict[str, Any]): Configuration options related to what the meter should read or expose (e.g., frequency, energy direction).
        communication_options (Dict[str, Any]): Protocol-specific connection settings (e.g., slave ID, port, URL, baudrate).
    """

    name: str
    protocol: Protocol
    type: EnergyMeterType
    options: Dict[str, Any]
    communication_options: Dict[str, Any]
    nodes: Set[NodeRecord]
    id: Optional[int] = None


@dataclass
class DeviceHistoryStatus:
    """
    Represents the operational status and connection history of a device.

    Tracks connection events and record lifecycle timestamps for monitoring
    device availability and operational status over time.

    Attributes:
        connection_on_datetime (Optional[datetime.datetime]): Timestamp when was the device last connection.
        connection_off_datetime (Optional[datetime.datetime]): Timestamp when was the device last disconnection.
        created_at (Optional[datetime.datetime]): Timestamp when the device was first created.
        updated_at (Optional[datetime.datetime]): Timestamp when the device was last modified.
    """

    connection_on_datetime: Optional[datetime.datetime]
    connection_off_datetime: Optional[datetime.datetime]
    created_at: Optional[datetime.datetime]
    updated_at: Optional[datetime.datetime]

    def get_status(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the device history status.

        Returns:
            Dict[str, Any]: All status attributes as key-value pairs.
        """
        return asdict(self)
