###########EXTERNAL IMPORTS############

from datetime import datetime
import time
from typing import Optional, Callable, Dict, Union, Any
from enum import Enum

#######################################

#############LOCAL IMPORTS#############

from controller.enums import Protocol
from db.db import NodeRecord

#######################################


class NodeType(Enum):
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


class Node:
    """
    Represents a data point in a device, such as a sensor reading or measurement value.

    Supports various data types (e.g., INT, FLOAT, BOOL, STRING) and features like:
    - Incremental value handling (for energy counters, etc.)
    - Alarms based on min/max thresholds
    - Logging and tracking of min/max/mean values
    - Detection of direction of value changes (positive/negative trend)
    - Callback execution on value change

    Attributes:
        name (str): Unique name of the node.
        type (NodeType): Type of the node value (e.g., FLOAT, BOOL).
        unit (str | None): Measurement unit (e.g., 'V', 'kWh'). Automatically set to None for BOOL/STRING nodes.
        protocol (Protocol): Protocol of the node (NONE if virtual or calculated node, MODBUS_RTU, OPC_UA...).
        enabled (bool): Whether the node is enabled for data collection and processing.
        incremental_node (bool | None): Whether the node represents an incremental counter. None for BOOL/STRING nodes.
        positive_incremental (bool | None): Whether to increment positively (used with incremental_node). None for non-incremental nodes.
        calculate_increment (bool | None): Whether to compute increment from the initial value (used with incremental_node). None for non-incremental nodes.
        publish (bool): Whether to publish this node over MQTT.
        calculated (bool): If the value is computed rather than read from hardware.
        custom (bool): If the node is a custom node (custom name and unit).
        logging (bool): Whether to log historical values.
        logging_period (int): Time in minutes between log entries.
        min_alarm (bool): Enables alarm for values below `min_alarm_value`.
        max_alarm (bool): Enables alarm for values above `max_alarm_value`.
        min_alarm_value (float | None): Lower threshold for minimum value alarm triggering. Automatically set to None for BOOL/STRING nodes.
        max_alarm_value (float | None): Upper threshold for maximum value alarm triggering. Automatically set to None for BOOL/STRING nodes.
        decimal_places (int | None): Number of decimal places to round values to. Automatically set to None for non-FLOAT nodes.
        on_value_change (Optional[Callable[["Node"], None]]): Optional callback triggered when value updates.
        last_log_datetime (Optional[datetime]): Timestamp of last logged entry.
        min_alarm_state (bool): True if a value dropped below the minimum threshold. Resets only on value reset.
        max_alarm_state (bool): True if a value exceeded the maximum threshold. Resets only on value reset.

        value (Union[float, int, str, bool] | None): Current value of the node.
        timestamp (Optional[float]): Timestamp of the last value update.
        elapsed_time (Optional[float]): Time difference (seconds) between last and current update.
        initial_value (Union[float, int] | None): First observed value (for incremental nodes).
        positive_direction (bool): True if value increased since last change.
        negative_direction (bool): True if value decreased since last change.
        min_value (Union[float, int] | None): Lowest observed value since last reset.
        max_value (Union[float, int] | None): Highest observed value since last reset.
        mean_value (Union[float, int] | None): Average value since last reset.
        mean_sum (float): Cumulative value used to calculate mean.
        mean_count (int): Number of values used to calculate mean.
    """

    def __init__(
        self,
        name: str,
        type: NodeType,
        unit: str | None,
        protocol: Protocol = Protocol.NONE,
        enabled: bool = True,
        incremental_node: bool | None = False,
        positive_incremental: bool | None = False,
        calculate_increment: bool | None = True,
        publish: bool = True,
        calculated: bool = False,
        custom: bool = False,
        logging: bool = False,
        logging_period: int = 15,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float | None = 0.0,
        max_alarm_value: float | None = 0.0,
        decimal_places: int | None = 3,
        on_value_change: Optional[Callable[["Node"], None]] = None,
    ):
        # Configuration
        self.name = name
        self.type = type
        self.unit = unit if (self.type is NodeType.FLOAT or self.type is NodeType.INT) else None
        self.protocol = protocol
        self.enabled = enabled
        self.publish = publish
        self.calculated = calculated
        self.custom = custom
        self.decimal_places = decimal_places if (self.type is NodeType.FLOAT) else None
        self.on_value_change = on_value_change

        # Logging
        self.logging = logging
        self.logging_period = logging_period
        self.last_log_datetime: Optional[datetime] = None

        # Alarms
        self.min_alarm = min_alarm
        self.max_alarm = max_alarm
        self.min_alarm_value = min_alarm_value if (self.type is NodeType.FLOAT or self.type is NodeType.INT) else None
        self.max_alarm_value = max_alarm_value if (self.type is NodeType.FLOAT or self.type is NodeType.INT) else None
        self.min_alarm_state = False
        self.max_alarm_state = False

        # Incremental logic
        self.incremental_node = incremental_node if (self.type is NodeType.FLOAT or self.type is NodeType.INT) else None
        self.positive_incremental = positive_incremental if (self.incremental_node is not None) else None
        self.calculate_increment = calculate_increment if (self.incremental_node is not None) else None
        self.initial_value: Union[float, int] = None

        # Value and tracking
        self.value: Union[float, int, str, bool] = None
        self.timestamp: Optional[float] = None
        self.elapsed_time: Optional[float] = None
        self.positive_direction = False
        self.negative_direction = False
        self.min_value: Union[float, int] = None
        self.max_value: Union[float, int] = None
        self.mean_value: Union[float, int] = None
        self.mean_sum: float = 0.0
        self.mean_count: int = 0

        # Validate Node
        self.validate_node()

    def validate_node(self) -> None:
        """
        Validates the current Node configuration to ensure it is logically consistent
        based on its type and enabled features.

        Raises:
            ValueError: If the configuration includes unsupported or inconsistent combinations such as:
                - Invalid protocol defined.
                - Using `incremental_node` with a BOOL or STRING node.
                - Enabling min or max alarms for BOOL or STRING nodes.
                - Enabling alarms on incremental nodes.
                - Setting alarm thresholds when alarms are disabled.
                - Units should be empty for BOOL or STRING nodes.
                - Enabling logging with a non-positive logging period.
        """

        # Protocol validation
        if self.protocol not in Protocol.valid_protocols():
            raise ValueError(f"Protocol {self.protocol} is not valid.")

        # Basic type restrictions
        if self.type in {NodeType.BOOL, NodeType.STRING}:
            if self.incremental_node:
                raise ValueError(f"incremental_node is not valid for {self.type.name} nodes.")

            if self.min_alarm or self.max_alarm:
                raise ValueError(f"Alarms are not supported for {self.type.name} nodes.")

            if self.unit:
                raise ValueError(f"Non-empty unit is not applicable to {self.type.name} nodes.")

        # Incremental restrictions
        if self.incremental_node:
            if self.min_alarm or self.max_alarm:
                raise ValueError("Alarms are not applicable to incremental nodes.")

        # Alarm threshold without enable flags
        if self.min_alarm and self.min_alarm_value is None:
            raise ValueError("min_alarm is enabled but min_alarm_value is None.")

        if self.max_alarm and self.max_alarm_value is None:
            raise ValueError("max_alarm is enabled but max_alarm_value is None.")

        # Logging period must be valid if logging is enabled
        if self.logging and (not isinstance(self.logging_period, int) or self.logging_period <= 0):
            raise ValueError(f"Invalid logging period '{self.logging_period}' for node '{self.name}'. Must be a positive integer.")

    def set_unit(self, unit: str) -> None:
        """
        Sets the unit for the node if applicable.

        Raises:
            ValueError: If the node type is BOOL or STRING.
        """

        if self.type in {NodeType.BOOL, NodeType.STRING} and unit:
            raise ValueError(f"Non-empty unit is not applicable to {self.type.name} nodes.")

        self.unit = unit

    def set_incremental_node(self, incremental_node: bool) -> None:
        """
        Enables or disables incremental behavior for the node.

        Args:
            incremental_node (bool): Whether to enable incremental logic.

        Raises:
            ValueError: If enabling incremental behavior on an unsupported node type or configuration:
                - Node type is BOOL or STRING.
                - Alarms are enabled, which are not supported on incremental nodes.
                - Alarm thresholds are set without alarms being enabled.
        """
        if incremental_node:
            if self.type in {NodeType.BOOL, NodeType.STRING}:
                raise ValueError(f"incremental_node is not valid for {self.type.name} nodes.")
            if self.min_alarm or self.max_alarm:
                raise ValueError("Alarms are not applicable to incremental nodes.")

        self.incremental_node = incremental_node

    def set_logging(self, logging: bool, logging_period: int) -> None:
        """
        Updates the logging configuration for this node.

        This method sets whether the node should log its values periodically, and at what interval.
        It performs local validation to ensure that the provided logging period is valid.

        Note:
            This method only validates the configuration of the current node.
            To ensure consistent logging periods across related nodes (e.g., same category),
            a system-wide validation should be performed using
            `EnergyMeterNodes.validate_logging_consistency()`.

        Args:
            logging (bool): Whether logging is enabled for this node.
            logging_period (int): Logging interval in minutes (must be a positive integer).

        Raises:
            ValueError: If logging is enabled but the logging period is not a positive integer.
        """

        if logging and (not isinstance(logging_period, int) or logging_period <= 0):
            raise ValueError(f"Invalid logging period '{logging_period}' for node '{self.name}'. Must be a positive integer.")

        self.logging = logging
        self.logging_period = logging_period

    def set_alarms(self, min_alarm: bool, max_alarm: bool, min_alarm_value: float, max_alarm_value: float) -> None:
        """
        Configures the alarm settings for the node.

        Args:
            min_alarm (bool): Enable alarm if value falls below `min_alarm_value`.
            max_alarm (bool): Enable alarm if value rises above `max_alarm_value`.
            min_alarm_value (float): Threshold for minimum value alarm.
            max_alarm_value (float): Threshold for maximum value alarm.

        Raises:
            ValueError: If alarm configuration is invalid based on node type or mode:
                - Alarms are not supported for BOOL or STRING nodes.
                - Alarms are not allowed on incremental nodes.
                - Thresholds are set without enabling their respective alarms.
        """

        if self.type in {NodeType.BOOL, NodeType.STRING}:
            if min_alarm or max_alarm:
                raise ValueError(f"Alarms are not supported for {self.type.name} nodes.")

        if self.incremental_node:
            if min_alarm or max_alarm:
                raise ValueError("Alarms are not supported for incremental nodes.")

        if min_alarm and min_alarm_value is None:
            raise ValueError("min_alarm is enabled but min_alarm_value is None.")

        if max_alarm and max_alarm_value is None:
            raise ValueError("max_alarm is enabled but max_alarm_value is None.")

        self.min_alarm = min_alarm
        self.max_alarm = max_alarm
        self.min_alarm_value = min_alarm_value
        self.max_alarm_value = max_alarm_value

    def check_alarms(self, value: Union[float, int]) -> None:
        """
        Evaluates the current value against configured alarm thresholds and updates alarm states.

        Args:
            value (Union[float, int]): The current node value to check.

        Raises:
            ValueError: If called on a non-numeric node type.
        """

        if self.type not in {NodeType.FLOAT, NodeType.INT}:
            raise ValueError(f"Alarms are not applicable for node type: {self.type}")

        if self.min_alarm and value < self.min_alarm_value:
            self.min_alarm_state = True

        if self.max_alarm and value > self.max_alarm_value:
            self.max_alarm_state = True

    def reset_alarms(self) -> None:
        """
        Clears both minimum and maximum alarm states.

        Use this after alarm conditions have been addressed or acknowledged.
        """

        self.min_alarm_state = False
        self.max_alarm_state = False

    def set_value(self, value: Union[float, int, str, bool]) -> None:
        """
        Updates the node's value and processes internal state accordingly.

        This method:
            - Updates the internal timestamp and calculates elapsed time since last update.
            - Delegates value handling to the appropriate method based on node type and configuration:
                - `set_value_str_bool()` for BOOL or STRING nodes.
                - `set_value_incremental()` for numeric incremental nodes.
                - `set_value_standard()` for regular numeric nodes.
            - Triggers the `on_value_change` callback if defined.

        Args:
            value (Union[float, int, str, bool]): The new value to assign to the node.
        """

        if self.enabled:

            self.update_timestamp()

            if self.type in {NodeType.BOOL, NodeType.STRING}:
                self.set_value_str_bool(value)
            elif self.incremental_node:
                self.set_value_incremental(value)
            else:
                self.set_value_standard(value)

            if self.on_value_change:
                self.on_value_change(self)

    def update_timestamp(self) -> None:
        """
        Updates the node's timestamp and calculates elapsed time since the last update.

        If the timestamp is None (initialization) sets elapsed time to 0.0.
        Otherwise, computes the time difference between the current and last update.
        """

        current_timestamp = time.time()
        if self.timestamp is None:
            self.timestamp = current_timestamp
            self.elapsed_time = 0.0
        else:
            self.elapsed_time = current_timestamp - self.timestamp
            self.timestamp = current_timestamp

    def update_direction(self, new_value: Union[int, float]) -> None:
        """
        Updates the direction flags based on how the new value compares to the current value.

        Sets:
            - `positive_direction` to True if the new value is greater.
            - `negative_direction` to True if the new value is smaller.
            - Both flags remain False if the value hasn't changed.

        Args:
            new_value (Union[int, float]): The latest value to compare against the current node value.
        """

        if new_value > self.value:
            self.positive_direction = True
            self.negative_direction = False

        elif new_value < self.value:
            self.positive_direction = False
            self.negative_direction = True

    def update_stats(self, value: Union[int, float]) -> None:
        """
        Updates statistical tracking for the node, including mean, min, and max values.

        This method:
            - Accumulates the total sum and count of values to compute the mean.
            - Updates the current minimum and maximum values if applicable.

        Args:
            value (Union[int, float]): The new value to include in the statistics.
        """

        self.mean_sum += value
        self.mean_count += 1
        self.mean_value = self.mean_sum / self.mean_count

        if self.min_value is None or value < self.min_value:
            self.min_value = value

        if self.max_value is None or value > self.max_value:
            self.max_value = value

    def set_value_str_bool(self, value: Union[str, bool]) -> None:
        """
        Sets the value for STRING or BOOL node types.

        Args:
            value (Union[str, bool]): The new value to assign to the node.
        """

        self.value = value

    def set_value_standard(self, value: Union[int, float]) -> None:
        """
        Sets and processes the value for non-incremental numeric nodes (INT or FLOAT).

        This includes:
            - Updating direction (positive/negative change).
            - Updating statistical tracking (min, max, mean).
            - Evaluating alarm thresholds.

        Args:
            value (Union[int, float]): The new numeric value to assign to the node.
        """

        if value is None:
            self.value = None
            return

        if self.value is not None:
            self.update_direction(value)

        self.value = value
        self.update_stats(value)
        self.check_alarms(value)

    def set_value_incremental(self, value: Union[int, float]) -> None:
        """
        Sets the value for an incremental numeric node (e.g., energy counters).

        This method handles:
            - Initial value capture on first call.
            - Calculation of the new value based on the current reading and mode:
                - Raw value (if `calculate_increment` is False),
                - Positive accumulation (adds value over time),
                - Delta from initial (default behavior).
            - Updates direction (positive or negative change).

        Args:
            value (Union[int, float]): The current raw value read from the device.
        """

        if value is None:
            return

        if self.initial_value is None:
            self.initial_value = value
            self.value = 0.0 if self.type == NodeType.FLOAT else 0
            return

        if not self.calculate_increment:
            calculated = value
            self.update_direction(calculated)
            self.value = calculated

        elif self.positive_incremental:
            calculated = value
            self.update_direction(calculated)
            self.value += calculated

        else:
            calculated = value - self.initial_value
            self.update_direction(calculated)
            self.value = calculated

    def reset_value(self) -> None:
        """
        Resets the internal state of the node's value tracking.

        This includes:
            - Initial value for incremental nodes
            - Min/max/mean statistics
            - Direction tracking
            - Timestamps and elapsed time
        """

        self.initial_value = None
        self.value = None
        self.min_value = None
        self.max_value = None
        self.mean_value = None
        self.mean_sum = 0
        self.mean_count = 0
        self.timestamp = time.time()
        self.elapsed_time = None
        self.positive_direction = False
        self.negative_direction = False

    def reset_direction(self) -> None:
        """
        Resets the directional tracking flags.
        This clears both the positive and negative direction indicators.
        """

        self.positive_direction = False
        self.negative_direction = False

    def get_publish_format(self) -> Dict[str, Any]:
        """
        Prepares the node's current value and metadata for MQTT publishing.
        Applies rounding for non-incremental FLOAT nodes.

        Returns:
            Dict[str, Any]: A dictionary containing the node's value, type, unit,
            and (if applicable) alarm states.

        Raises:
            Exception: If the node value is None, indicating no data has been set yet.
        """

        output = {
            "value": (
                round(self.value, self.decimal_places) if self.type is NodeType.FLOAT and not self.incremental_node and self.value is not None else self.value
            ),
            "type": self.type.value,
            "unit": self.unit,
        }

        if self.type in {NodeType.FLOAT, NodeType.INT} and not self.incremental_node:
            if self.min_alarm:
                output["min_alarm_state"] = self.min_alarm_state
            if self.max_alarm:
                output["max_alarm_state"] = self.max_alarm_state

        return output

    def submit_log(self, date_time: datetime) -> Dict[str, Any]:
        """
        Generates a log entry for the node, capturing statistical data or the current value
        depending on its type and configuration. Resets internal value state after submission.
        Applies rounding for FLOAT non-incremental nodes.

        Args:
            date_time (datetime): Timestamp marking the end of the logging period.

        Returns:
            Dict[str, Any]: A dictionary containing the log entry for this node.
                - For numeric nodes: includes mean, min, and max values.
                - For BOOL/STRING/incremental nodes: includes the last value.
        """

        output = {"name": self.name, "unit": self.unit, "start_time": self.last_log_datetime, "end_time": date_time}

        if self.type in {NodeType.INT, NodeType.FLOAT} and not self.incremental_node:
            output["mean_value"] = round(self.mean_value, self.decimal_places)
            output["min_value"] = round(self.min_value, self.decimal_places) if self.type is NodeType.FLOAT and self.min_value is not None else self.min_value
            output["max_value"] = round(self.max_value, self.decimal_places) if self.type is NodeType.FLOAT and self.max_value is not None else self.max_value
        else:
            output["value"] = self.value

        self.reset_value()
        self.last_log_datetime = date_time

        return output

    def get_node_record(self) -> NodeRecord:
        """
        Converts the current node instance into a serializable NodeRecord object.

        This method extracts all relevant configuration fields from the node,
        including protocol-specific attributes such as register (ModbusRTUNode)
        or node_id (OPCUANode), and returns a NodeRecord suitable for persistence.

        Returns:
            NodeRecord: A representation of the current node for database storage.
                - device_id is set to None and should be assigned externally.
                - protocol is inferred from the node class type.
                - config includes all configurable attributes.
        """

        configuration: Dict[str, Any] = {}
        configuration["enabled"] = self.enabled
        configuration["type"] = self.type.value
        configuration["unit"] = self.unit
        configuration["publish"] = self.publish
        configuration["calculated"] = self.calculated
        configuration["custom"] = self.custom
        configuration["decimal_places"] = self.decimal_places
        configuration["logging"] = self.logging
        configuration["logging_period"] = self.logging_period
        configuration["min_alarm"] = self.min_alarm
        configuration["max_alarm"] = self.max_alarm
        configuration["min_alarm_value"] = self.min_alarm_value
        configuration["max_alarm_value"] = self.max_alarm_value
        configuration["incremental_node"] = self.incremental_node
        configuration["positive_incremental"] = self.positive_incremental
        configuration["calculate_increment"] = self.calculate_increment

        if hasattr(self, "register") and self.protocol == Protocol.MODBUS_RTU:
            configuration["register"] = self.register
        elif hasattr(self, "node_id") and self.protocol == Protocol.OPC_UA:
            configuration["node_id"] = self.node_id

        return NodeRecord(device_id=None, name=self.name, protocol=self.protocol, config=configuration)


class ModbusRTUNode(Node):
    """
    Represents a Modbus RTU node (data point) with additional configuration
    such as logging, alarms, register address and connection status.

    Inherits from:
        Node: Base class representing a generic data point.

    Args:
        name (str): Unique name identifying the node.
        type (NodeType): Type of the node (e.g., NodeType.FLOAT, NodeType.STRING).
        register (int): Modbus register address where the value is located.
        unit (str): Unit of measurement (e.g., 'V', 'A').
        enabled (bool): Whether the node is enabled for data collection and processing.
        publish (bool): Whether to publish the node value via MQTT (default: True).
        calculated (bool): Whether the value is calculated instead of read directly (default: False).
        custom (bool): If the node is a custom node (custom name and unit).
        logging (bool): Whether the node value should be logged (default: False).
        logging_period (int): Logging interval in minutes (default: 15).
        min_alarm (bool): Enable alarm if value drops below `min_alarm_value` (default: False).
        max_alarm (bool): Enable alarm if value rises above `max_alarm_value` (default: False).
        min_alarm_value (float): Minimum threshold for triggering minimum value alarm (default: 0.0).
        max_alarm_value (float): Maximum threshold for triggering maximum value alarm (default: 0.0).

    Attributes:
        connected (bool): Indicates whether the node is currently reachable/responding.
    """

    def __init__(
        self,
        name: str,
        type: NodeType,
        register: int,
        unit: str,
        enabled: bool = True,
        incremental_node: bool | None = False,
        positive_incremental: bool | None = False,
        calculate_increment: bool | None = True,
        publish: bool = True,
        calculated: bool = False,
        custom: bool = False,
        logging: bool = False,
        logging_period: int = 15,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
        decimal_places: int | None = 3,
    ):
        super().__init__(
            name=name,
            type=type,
            unit=unit,
            protocol=Protocol.MODBUS_RTU,
            enabled=enabled,
            incremental_node=incremental_node,
            positive_incremental=positive_incremental,
            calculate_increment=calculate_increment,
            publish=publish,
            calculated=calculated,
            custom=custom,
            logging=logging,
            logging_period=logging_period,
            min_alarm=min_alarm,
            max_alarm=max_alarm,
            min_alarm_value=min_alarm_value,
            max_alarm_value=max_alarm_value,
            decimal_places=decimal_places,
        )

        self.register = register
        self.connected = False

    def set_connection_state(self, state: bool):
        """
        Sets the connection status of the node.

        Args:
            state (bool): True if the node is reachable and responding, False otherwise.
        """

        self.connected = state


class OPCUANode(Node):
    """
    Represents an OPC UA node (data point) with specific metadata and state tracking.

    This class extends the generic Node to include the necessary configuration
    for identifying and reading values from an OPC UA server.

    Inherits from:
        Node: Base class representing a generic data point.

    Args:
        name (str): Unique name identifying the node.
        type (NodeType): Type of the node (e.g., NodeType.FLOAT).
        node_id (str): OPC UA Node ID used to access the value on the server (e.g., 'ns=2;s=Voltage_L1').
        unit (str): Unit of measurement (e.g., 'V', 'A').
        enabled (bool): Whether the node is enabled for data collection and processing.
        publish (bool): Whether to publish the node value via MQTT (default: True).
        calculated (bool): Whether the value is calculated instead of read directly (default: False).
        custom (bool): If the node is a custom node (custom name and unit).
        logging (bool): Whether the node value should be logged (default: False).
        logging_period (int): Logging interval in minutes (default: 15).
        min_alarm (bool): Enable alarm if value drops below `min_alarm_value` (default: False).
        max_alarm (bool): Enable alarm if value rises above `max_alarm_value` (default: False).
        min_alarm_value (float): Minimum threshold for triggering a minimum value alarm (default: 0.0).
        max_alarm_value (float): Maximum threshold for triggering a maximum value alarm (default: 0.0).

    Attributes:
        node_id (str): OPC UA Node ID used to query values.
        connected (bool): Indicates whether the node is currently reachable/responding.
    """

    def __init__(
        self,
        name: str,
        type: NodeType,
        node_id: str,
        unit: str,
        enabled: bool = True,
        incremental_node: bool | None = False,
        positive_incremental: bool | None = False,
        calculate_increment: bool | None = True,
        publish: bool = True,
        calculated: bool = False,
        custom: bool = False,
        logging: bool = False,
        logging_period: int = 15,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
        decimal_places: int | None = 3,
    ):
        super().__init__(
            name=name,
            type=type,
            unit=unit,
            protocol=Protocol.OPC_UA,
            enabled=enabled,
            incremental_node=incremental_node,
            positive_incremental=positive_incremental,
            calculate_increment=calculate_increment,
            publish=publish,
            calculated=calculated,
            custom=custom,
            logging=logging,
            logging_period=logging_period,
            min_alarm=min_alarm,
            max_alarm=max_alarm,
            min_alarm_value=min_alarm_value,
            max_alarm_value=max_alarm_value,
            decimal_places=decimal_places,
        )
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state
