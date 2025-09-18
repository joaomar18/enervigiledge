###########EXTERNAL IMPORTS############

from datetime import datetime
import time
from typing import Optional, Dict, Any, Type, TypeVar, Generic, TypeGuard
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import NodeConfig

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

        # Alarms
        self.min_alarm_state = False
        self.max_alarm_state = False

        # Value and Tracking
        self.value: Optional[V] = None
        self.timestamp: Optional[float] = None
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

        self.update_timestamp()
        if value is None:
            self.value = None
            return False

        return True

    def update_timestamp(self) -> None:
        """
        Updates the processor's timestamp and calculates elapsed time since last update.
        """

        current_timestamp = time.time()
        if self.timestamp is None:
            self.timestamp = current_timestamp
            self.elapsed_time = 0.0
        else:
            self.elapsed_time = current_timestamp - self.timestamp
            self.timestamp = current_timestamp

    def reset_alarms(self) -> None:
        """
        Resets all alarm states to False.
        """

        self.min_alarm_state = False
        self.max_alarm_state = False

    def reset_value(self) -> None:
        """
        Resets the processor value and updates timestamp, clearing elapsed time.
        """

        self.value = None
        self.timestamp = time.time()
        self.elapsed_time = None

    def get_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Formats processor data for MQTT publishing.

        Args:
            additional_data (Dict[str, Any]): Additional data to include in the output.

        Returns:
            Dict[str, Any]: Formatted data including type, unit, alarms, and attributes.
        """

        output = additional_data.copy()
        output["type"] = self.config.type.value
        output["unit"] = self.config.unit
        output["incremental"] = self.config.incremental_node

        if self.config.min_alarm:
            output["min_alarm_state"] = self.min_alarm_state
        if self.config.max_alarm:
            output["max_alarm_state"] = self.max_alarm_state

        attributes = self.config.attributes.get_attributes()
        for name, value in attributes.items():
            output[name] = value

        return output

    def get_detailed_state(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output["type"] = self.config.type.value
        output["incremental"] = self.config.incremental_node
        output["last_update_date"] = datetime.fromtimestamp(self.timestamp) if self.timestamp is not None else None
        output["last_reset_date"] = self.last_log_datetime
        output["min_alarm_value"] = self.config.min_alarm_value
        output["max_alarm_value"] = self.config.max_alarm_value

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
        output = {"name": self.config.name, "start_time": self.last_log_datetime, "end_time": date_time}
        self.reset_value()
        self.last_log_datetime = date_time

        return output
