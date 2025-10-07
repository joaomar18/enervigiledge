###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol

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

        Returns:
            Dict[str, Any]: Dictionary containing all node attributes.
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
        min_warning_value (float | None): Minimum treshold for warning triggering.
        max_warning_value (float | None): Maximum treshold for warning triggering.
        decimal_places (int | None): Number of decimal places to display (FLOAT only).
        attributes (NodeAttributes): Domain-specific metadata (e.g. phase).

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
    min_warning_value: float | None = None
    max_warning_value: float | None = None
    decimal_places: int | None = 3
    attributes: NodeAttributes = field(default_factory=NodeAttributes)

    @staticmethod
    def create_from_node_record(record: NodeRecord) -> "NodeConfig":
        """
        Creates a NodeConfig instance from a NodeRecord database record.

        Extracts configuration data from the record and filters valid fields
        to construct a properly typed NodeConfig instance with enum conversions.

        Args:
            record: Database record containing node configuration data.

        Returns:
            NodeConfig: Configured node instance ready for use.
        """

        config = record.config
        valid_fields = set(NodeConfig.__dataclass_fields__.keys())
        filtered_config = {k: v for k, v in config.items() if k in valid_fields and k not in ["type", "unit", "name", "attributes", "protocol"]}

        return NodeConfig(
            name=record.name,
            type=NodeType(config["type"]),
            unit=config["unit"],
            protocol=Protocol(record.protocol),
            attributes=NodeAttributes(**record.attributes),
            **filtered_config,
        )

    def __post_init__(self):

        DEFAULT_WARNING_PERCENT = 0.02

        if self.min_warning_value is None and self.min_alarm_value is not None:
            self.min_warning_value = self.min_alarm_value * (1 + DEFAULT_WARNING_PERCENT)
        if self.max_warning_value is None and self.max_alarm_value is not None:
            self.max_warning_value = self.max_alarm_value * (1 - DEFAULT_WARNING_PERCENT)

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
