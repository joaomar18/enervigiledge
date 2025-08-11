###########EXTERNAL IMPORTS############

from datetime import datetime
import time
from typing import Optional, Dict, Union, Any

#######################################

#############LOCAL IMPORTS#############

from controller.types import Protocol, NodeType, NodeConfig, BaseNodeRecordConfig, NodeRecord

#######################################


class Node:
    """
    Represents a data point with support for different value types, incremental counting,
    alarms, statistical tracking, and logging.

    Supports INT, FLOAT, BOOL, and STRING data types with protocol-agnostic design.
    Provides incremental behavior for energy counters, configurable alarm thresholds,
    statistical tracking (min/max/mean), and periodic logging with MQTT publishing.

    Key Features:
        - Value change callbacks via on_value_change
        - Incremental nodes: energy counters with delta or accumulation modes
        - Alarms: min/max threshold monitoring (numeric types only)
        - Statistics: automatic min/max/mean tracking for non-incremental numeric nodes
        - Logging: periodic data capture with configurable intervals

    Type Restrictions:
        - BOOL/STRING: No incremental features, alarms, or units allowed
        - Incremental nodes: Cannot use alarms (conflicts with accumulator logic)
        - Only numeric types support alarms and statistical tracking

    Args:
        configuration (NodeConfig): Complete node configuration including type, protocol,
            alarms, logging, and incremental settings.
    """

    def __init__(self, configuration: NodeConfig):
        configuration.validate()

        # Configuration
        self.name = configuration.name
        self.type = configuration.type
        self.unit = configuration.unit
        self.protocol = configuration.protocol
        self.enabled = configuration.enabled
        self.publish = configuration.publish
        self.calculated = configuration.calculated
        self.custom = configuration.custom
        self.decimal_places = configuration.decimal_places
        self.on_value_change = configuration.on_value_change

        # Logging
        self.logging = configuration.logging
        self.logging_period = configuration.logging_period
        self.last_log_datetime: Optional[datetime] = None

        # Alarms
        self.min_alarm = configuration.min_alarm
        self.max_alarm = configuration.max_alarm
        self.min_alarm_value = configuration.min_alarm_value
        self.max_alarm_value = configuration.max_alarm_value
        self.min_alarm_state = False
        self.max_alarm_state = False

        # Incremental logic
        self.incremental_node = configuration.incremental_node
        self.positive_incremental = configuration.positive_incremental
        self.calculate_increment = configuration.calculate_increment
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

        Uses BaseNodeRecordConfig for core attributes. Protocol-specific subclasses
        should override this method to add their own attributes.

        Returns:
            NodeRecord: A representation of the current node for database storage.
        """

        base_config = BaseNodeRecordConfig(
            enabled=self.enabled,
            type=self.type,
            unit=self.unit,
            publish=self.publish,
            calculated=self.calculated,
            custom=self.custom,
            decimal_places=self.decimal_places,
            logging=self.logging,
            logging_period=self.logging_period,
            min_alarm=self.min_alarm,
            max_alarm=self.max_alarm,
            min_alarm_value=self.min_alarm_value,
            max_alarm_value=self.max_alarm_value,
            incremental_node=self.incremental_node,
            positive_incremental=self.positive_incremental,
            calculate_increment=self.calculate_increment,
        )

        configuration = base_config.get_config()
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
        config = NodeConfig(name=name, type=type, unit=unit, protocol=Protocol.MODBUS_RTU, **kwargs)
        super().__init__(configuration=config)
        self.register = register
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state

    def get_node_record(self) -> NodeRecord:
        """
        Converts the Modbus RTU node instance into a serializable NodeRecord object.

        Extends the base implementation to include Modbus-specific register attribute.

        Returns:
            NodeRecord: A representation of the current node for database storage.
        """

        node_record = super().get_node_record()
        node_record.config["register"] = self.register

        return node_record


class OPCUANode(Node):
    """
    Node implementation for OPC UA protocol communication.

    Adds OPC UA-specific node ID addressing and connection tracking to base Node functionality.

    Additional Attributes:
        node_id (str): OPC UA Node ID for server access (e.g., "ns=4;i=7")
        connected (bool): Connection status with the OPC UA server
    """

    def __init__(self, name: str, type: NodeType, node_id: str, unit: str, **kwargs):
        config = NodeConfig(name=name, type=type, unit=unit, protocol=Protocol.OPC_UA, **kwargs)
        super().__init__(configuration=config)
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state

    def get_node_record(self) -> NodeRecord:
        """
        Converts the OPC UA node instance into a serializable NodeRecord object.

        Extends the base implementation to include OPC UA-specific node_id attribute.

        Returns:
            NodeRecord: A representation of the current node for database storage.
        """

        node_record = super().get_node_record()
        node_record.config["node_id"] = self.node_id

        return node_record
