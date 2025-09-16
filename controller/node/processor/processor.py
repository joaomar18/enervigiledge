###########EXTERNAL IMPORTS############

from datetime import datetime
import time
from typing import Optional, Dict, Any, Type, TypeVar, Generic, TypeGuard
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.types.node import NodeConfig

#######################################

V = TypeVar("V")
N = TypeVar("N", int, float)  # numeric-only types


class NodeProcessor(ABC, Generic[V]):
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
        return issubclass(processor.get_value_type(), (int, float))

    def get_value_type(self) -> Type[V]:
        return self._value_type

    @abstractmethod
    def check_alarms(self, value: V) -> None:
        pass

    @abstractmethod
    def set_value(self, value: Optional[V]) -> None:
        pass

    def prepare_set_value(self, value: Optional[V]) -> bool:

        if not self.config.enabled:
            return False

        self.update_timestamp()
        if value is None:
            self.value = None
            return False

        return True

    def update_timestamp(self) -> None:

        current_timestamp = time.time()
        if self.timestamp is None:
            self.timestamp = current_timestamp
            self.elapsed_time = 0.0
        else:
            self.elapsed_time = current_timestamp - self.timestamp
            self.timestamp = current_timestamp

    def reset_alarms(self) -> None:

        self.min_alarm_state = False
        self.max_alarm_state = False

    def reset_value(self) -> None:

        self.value = None
        self.timestamp = time.time()
        self.elapsed_time = None

    def get_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output["type"] = self.config.type.value
        output["unit"] = self.config.unit

        if self.config.min_alarm:
            output["min_alarm_state"] = self.min_alarm_state
        if self.config.max_alarm:
            output["max_alarm_state"] = self.max_alarm_state

        attributes = self.config.attributes.get_attributes()
        for name, value in attributes.items():
            output[name] = value

        return output

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output = {"name": self.config.name, "unit": self.config.unit, "start_time": self.last_log_datetime, "end_time": date_time}
        self.reset_value()
        self.last_log_datetime = date_time

        return output
