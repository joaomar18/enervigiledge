###########EXTERNAL IMPORTS############

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Type, TypeVar, Generic, TypeGuard
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import NodeConfig
import util.functions.date as date

#######################################

V = TypeVar("V")
N = TypeVar("N", int, float)  # numeric-only types


class NodeProcessor(ABC, Generic[V]):
    """
    Abstract base class for all node value processors.

    Provides common functionality for value management, timestamping, alarms,
    and data serialization. Subclasses implement type-specific value handling.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
        value_type (Type[V]): The Python type this processor handles.
    """

    def __init__(self, configuration: NodeConfig, value_type: Type[V]):
        self.config = configuration
        self._value_type = value_type

        # Logging
        self.last_log_datetime: Optional[datetime] = None

        # Alarms and Warnings
        self.min_alarm_state = False
        self.max_alarm_state = False
        self.min_warning_state = False
        self.max_warning_state = False

        # Value and Tracking
        self.value: Optional[V] = None
        self.timestamp: Optional[int] = None
        self.elapsed_time: Optional[float] = None

    @staticmethod
    def is_numeric_processor(processor: "NodeProcessor[Any]") -> TypeGuard["NodeProcessor[int] | NodeProcessor[float]"]:
        """
        Type guard to check if a processor handles numeric values.

        Args:
            processor (NodeProcessor[Any]): The processor to check.

        Returns:
            TypeGuard: True if the processor handles int or float values.
        """
        return issubclass(processor.get_value_type(), (int, float))

    def get_value_type(self) -> Type[V]:
        """
        Returns the Python type this processor handles.

        Returns:
            Type[V]: The value type (e.g., int, float, str, bool).
        """
        return self._value_type

    def is_healthy(self) -> bool:
        """Returns whether the node is currently healthy based on the processor state."""

        return self.min_alarm_state is False and self.max_alarm_state is False and self.min_warning_state is False and self.max_warning_state is False and self.value is not None

    def in_alarm(self) -> bool:
        """Returns whether the node is currently in an alarm state based on the processor state."""

        return self.min_alarm_state is True or self.max_alarm_state is True

    def in_warning(self) -> bool:
        """Returns whether the node is currently in a warning state based on the processor state."""

        return self.min_warning_state is True or self.max_warning_state is True

    @abstractmethod
    def check_alarms(self, value: V) -> None:
        pass

    @abstractmethod
    def set_value(self, value: Optional[V]) -> None:
        pass

    def prepare_set_value(self, value: Optional[V]) -> bool:
        """
        Prepares for value setting by checking node status and updating timestamp.

        Args:
            value (Optional[V]): The value to be set.

        Returns:
            bool: True if the value setting should proceed, False otherwise.
        """

        if not self.config.enabled:
            return False

        if value is None:
            self.value = None
            return False

        self.update_timestamp()
        return True

    def update_timestamp(self) -> None:
        """
        Updates the processor's timestamp and calculates elapsed time since last update.
        """

        current_timestamp = date.get_timestamp(date.get_current_utc_datetime())
        if self.timestamp is None:
            self.elapsed_time = 0.0
        else:
            self.elapsed_time = (current_timestamp - self.timestamp) / 1000.0  # converted to seconds

        self.timestamp = current_timestamp

    def reset_alarms(self) -> None:
        """
        Resets all alarm and warnings states to False.
        """

        self.min_alarm_state = False
        self.max_alarm_state = False
        self.min_warning_state = False
        self.max_warning_state = False

    def reset_value(self) -> None:
        """
        Resets the processor state.

        This method is defined in the base class and should be overridden by
        subclasses to clear stored values, internal state, and timestamps as
        appropriate for each specific processor type.
        """

        pass

    def create_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Builds the node publish payload by exposing configuration metadata,
        optional alarm and warning states, and any protocol-specific attributes.
        """

        output = additional_data.copy()
        output["type"] = self.config.type.value
        output["unit"] = self.config.unit
        output["is_counter"] = self.config.is_counter

        if self.config.min_alarm:
            output["min_alarm_state"] = self.min_alarm_state

        if self.config.min_warning:
            output["min_warning_state"] = self.min_warning_state

        if self.config.max_alarm:
            output["max_alarm_state"] = self.max_alarm_state

        if self.config.max_warning:
            output["max_warning_state"] = self.max_warning_state

        attributes = self.config.attributes.get_attributes()
        for name, value in attributes.items():
            output[name] = value

        return output

    def create_extended_info(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Builds an extended, read-only information payload for the node.

        The returned data includes lifecycle timestamps (last update and reset),
        configuration metadata (type, protocol, logging period), and any configured
        alarm or warning thresholds when applicable.
        """

        output = additional_data.copy()
        output["last_update_date"] = date.to_iso(date.get_date_from_timestamp(self.timestamp)) if self.timestamp is not None else None
        output["last_reset_date"] = date.to_iso(self.last_log_datetime) if self.last_log_datetime is not None else None

        if self.config.min_alarm:
            output["min_alarm_value"] = self.config.min_alarm_value

        if self.config.min_warning:
            output["min_warning_value"] = self.config.min_warning_value

        if self.config.max_alarm:
            output["max_alarm_value"] = self.config.max_alarm_value

        if self.config.max_warning:
            output["max_warning_value"] = self.config.max_warning_value

        output["type"] = self.config.type
        output["protocol"] = self.config.protocol
        if self.config.logging:
            output["logging_period"] = self.config.logging_period

        return output

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Prepares processor data for database logging and resets the processor state.

        Args:
            date_time (datetime): The end time for this logging period.
            additional_data (Dict[str, Any]): Additional data to include in the log.

        Returns:
            Dict[str, Any]: Formatted log data including name and time period.
        """

        output = additional_data.copy()
        output["name"] = self.config.name
        output["start_time"] = date_time - timedelta(minutes=self.config.logging_period)
        output["end_time"] = date_time

        self.reset_value()
        self.last_log_datetime = date_time

        return output
