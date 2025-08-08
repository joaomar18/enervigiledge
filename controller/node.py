###########EXTERNAL IMPORTS############

from datetime import datetime
import time
from typing import Optional, Callable, Dict, Union, Any

#######################################

#############LOCAL IMPORTS#############

from controller.types import Protocol, NodeType
from db.db import NodeRecord

#######################################


class Node:
    """
    Represents a data point in an energy meter device, such as a sensor reading or measurement value.

    The Node class provides comprehensive functionality for handling different types of data points
    with support for real-time processing, historical tracking, alarms, and various calculation modes.

    Key Features:
        - Multi-type support: INT, FLOAT, BOOL, STRING values
        - Incremental value handling for energy counters and accumulators
        - Configurable alarm system with min/max thresholds
        - Statistical tracking (min, max, mean values)
        - Direction detection for value change trends
        - Flexible logging with configurable intervals
        - Protocol-agnostic design (Modbus RTU, OPC UA, calculated nodes)
        - Value change callbacks for real-time event handling

    Configuration Parameters:
        name (str): Unique identifier for the node
        type (NodeType): Data type (FLOAT, INT, BOOL, STRING)
        unit (str | None): Measurement unit (e.g., 'V', 'kWh', 'Hz'). Auto-set to None for BOOL/STRING
        protocol (Protocol): Communication protocol (MODBUS_RTU, OPC_UA, NONE for virtual nodes)
        enabled (bool): Whether the node is active for data collection
        publish (bool): Whether to publish values over MQTT
        calculated (bool): True if value is computed rather than read from hardware
        custom (bool): True if node uses custom naming or units
        decimal_places (int | None): Decimal precision for FLOAT values. Auto-set to None for non-FLOAT

    Incremental Node Features:
        incremental_node (bool | None): Enables counter/accumulator behavior. None for BOOL/STRING
        positive_incremental (bool | None): Direction of incremental counting
        calculate_increment (bool | None): Whether to compute increment from initial value
        initial_value (Union[float, int] | None): First observed value for increment calculations

    Logging Configuration:
        logging (bool): Whether to store historical values
        logging_period (int): Time interval in minutes between log entries
        last_log_datetime (Optional[datetime]): Timestamp of most recent log entry

    Alarm System:
        min_alarm (bool): Enables low-value threshold monitoring
        max_alarm (bool): Enables high-value threshold monitoring
        min_alarm_value (float | None): Lower threshold trigger. Auto-set to None for BOOL/STRING
        max_alarm_value (float | None): Upper threshold trigger. Auto-set to None for BOOL/STRING
        min_alarm_state (bool): Current state of minimum value alarm
        max_alarm_state (bool): Current state of maximum value alarm

    Runtime State:
        value (Union[float, int, str, bool] | None): Current node value
        timestamp (Optional[float]): Unix timestamp of last value update
        elapsed_time (Optional[float]): Seconds between last and current update
        positive_direction (bool): True if value increased since last change
        negative_direction (bool): True if value decreased since last change

    Statistical Tracking:
        min_value (Union[float, int] | None): Lowest observed value since reset
        max_value (Union[float, int] | None): Highest observed value since reset
        mean_value (Union[float, int] | None): Average value since reset
        mean_sum (float): Cumulative sum for mean calculation
        mean_count (int): Number of values used in mean calculation

    Event Handling:
        on_value_change (Optional[Callable]): Callback function triggered on value updates

    Type Restrictions:
        - BOOL/STRING nodes: Cannot use incremental features, alarms, or units
        - Incremental nodes: Cannot use alarm features (conflicts with accumulator logic)
        - FLOAT nodes: Support full feature set including decimal precision
        - INT nodes: Support most features except decimal precision

    Usage Examples:
        # Basic sensor reading
        voltage_node = Node("l1_voltage", NodeType.FLOAT, "V", logging=True)

        # Energy counter with increment calculation
        energy_node = Node("active_energy", NodeType.FLOAT, "kWh",
                          incremental_node=True, calculate_increment=True)

        # Boolean status with MQTT publishing
        status_node = Node("device_online", NodeType.BOOL, None, publish=True)

        # Calculated value with alarms
        power_node = Node("total_power", NodeType.FLOAT, "kW", calculated=True,
                         min_alarm=True, min_alarm_value=0.0, max_alarm=True, max_alarm_value=1000.0)
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
    Node implementation for Modbus RTU protocol communication.

    Adds Modbus RTU-specific register addressing and connection tracking to base Node functionality.

    Additional Attributes:
        register (int): Modbus register address where the data resides
        connected (bool): Connection status with the Modbus device
    """

    def __init__(self, name: str, type: NodeType, register: int, unit: str, **kwargs):
        super().__init__(name=name, type=type, unit=unit, protocol=Protocol.MODBUS_RTU, **kwargs)
        self.register = register
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state


class OPCUANode(Node):
    """
    Node implementation for OPC UA protocol communication.

    Adds OPC UA-specific node ID addressing and connection tracking to base Node functionality.

    Additional Attributes:
        node_id (str): OPC UA Node ID for server access (e.g., "ns=4;i=7")
        connected (bool): Connection status with the OPC UA server
    """

    def __init__(self, name: str, type: NodeType, node_id: str, unit: str, **kwargs):
        super().__init__(name=name, type=type, unit=unit, protocol=Protocol.OPC_UA, **kwargs)
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state
