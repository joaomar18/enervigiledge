###########EXTERNAL IMPORTS############

from typing import Dict, Any, Optional, Type
from abc import ABC, abstractmethod
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.processor import NodeProcessor, N
from model.controller.node import NodeConfig, CounterMode
import util.functions.calculation as calculation

#######################################


class NumericNodeProcessor(NodeProcessor[N]):
    """
    Abstract base class for processors that handle numeric values (int or float).

    Provides functionality for alarm checking, statistical tracking, directional
    monitoring, and incremental value handling specific to numeric data types.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
        value_type (Type[N]): The numeric type this processor handles (int or float).
    """

    @property
    @abstractmethod
    def ZERO(self) -> N:
        """
        Zero value defined in subclasses
        """
        pass

    def __init__(self, configuration: NodeConfig, value_type: Type[N]):

        super().__init__(configuration=configuration, value_type=value_type)

        # Value and tracking
        self.initial_value: Optional[N] = None
        self.value: Optional[N] = None
        self.positive_direction = False
        self.negative_direction = False
        self.min_value: Optional[N] = None
        self.max_value: Optional[N] = None
        self.mean_sum: float = 0.0
        self.mean_count: int = 0

    def __init_subclass__(cls, **kw):
        if cls is not NumericNodeProcessor and "ZERO" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must define property ZERO")
        super().__init_subclass__(**kw)

    def check_alarms(self, value: N) -> None:
        """
        Checks value against configured alarm / warning thresholds and updates alarm / warning states.

        Args:
            value (N): The value to check against alarm / warning thresholds.
        """

        if self.config.min_alarm and self.config.min_alarm_value is not None:
            self.min_alarm_state = value < self.config.min_alarm_value

        if self.config.max_alarm and self.config.max_alarm_value is not None:
            self.max_alarm_state = value > self.config.max_alarm_value

        if self.config.min_warning and self.config.min_warning_value is not None:
            self.min_warning_state = value < self.config.min_warning_value

        if self.config.max_warning and self.config.max_warning_value is not None:
            self.max_warning_state = value > self.config.max_warning_value

    def reset_direction(self) -> None:
        """
        Resets the directional tracking flags to False.
        """

        self.positive_direction = False
        self.negative_direction = False

    def update_direction(self, new_value: N) -> None:
        """
        Updates directional tracking flags based on value change.

        Args:
            new_value (N): The new value to compare against the current value.
        """

        if self.value is None:
            return

        if new_value > self.value:
            self.positive_direction = True
            self.negative_direction = False

        elif new_value < self.value:
            self.positive_direction = False
            self.negative_direction = True

    def reset_value(self) -> None:
        """
        Resets all processor state including value, statistics, and directional tracking.
        """

        super().reset_value()
        self.initial_value = None
        self.min_value = None
        self.max_value = None
        self.mean_sum = 0.0
        self.mean_count = 0
        self.positive_direction = False
        self.negative_direction = False

    def update_statistics(self, value: N) -> None:
        """
        Updates running statistics (min, max, mean) with the new value.

        Args:
            value (N): The new value to include in statistics.
        """

        float_value = float(value)
        self.mean_sum += float_value
        self.mean_count += 1

        if self.min_value is None or value < self.min_value:
            self.min_value = value

        if self.max_value is None or value > self.max_value:
            self.max_value = value

    def set_value(self, value: Optional[N]) -> None:
        """
        Sets the processor value, handling both counter and normal modes.

        Args:
            value (Optional[N]): The value to set, or None to clear the value.
        """

        if not super().prepare_set_value(value) or value is None:  # Node disabled or value is None
            return

        if self.config.is_counter:
            self.__set_value_counter(value)
        else:
            self.__set_value_normal(value)

    def __set_value_counter(self, value: N) -> None:
        """
        Handles value setting for counter nodes (counters, energy meters).

        Args:
            value (N): The raw value from the device.
        """

        if self.initial_value is None:
            if self.config.counter_mode is CounterMode.DELTA:
                self.value = self.ZERO
                self.initial_value = self.value
            else:
                self.initial_value = value

        current_value = self.value if self.value is not None else self.ZERO

        if self.config.counter_mode is CounterMode.DIRECT:
            new_value = value
            delta = new_value - current_value

        elif self.config.counter_mode is CounterMode.DELTA:
            delta = value
            new_value = current_value + delta
        elif self.config.counter_mode is CounterMode.CUMULATIVE:
            new_value = value - self.initial_value
            delta = new_value - current_value
        else:
            raise ValueError(f"Counter mode is not valid: {self.config.counter_mode}")

        self.update_direction(delta)
        self.value = new_value

    def __set_value_normal(self, value: N) -> None:
        """
        Handles value setting for standard measurement nodes.

        Args:
            value (N): The measurement value from the device.
        """

        self.update_direction(value)
        self.value = value
        self.update_statistics(value)
        self.check_alarms(value)

    def create_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Formats the node value for publishing based on its configuration and
        delegates construction of the final payload to the base implementation.
        """

        output = additional_data.copy()
        if self.value is not None:
            output["value"] = round(self.value, self.config.decimal_places) if self.config.decimal_places is not None else int(self.value)
        else:
            output["value"] = self.value

        if self.config.decimal_places is not None:
            output["decimal_places"] = self.config.decimal_places

        if self.config.min_alarm:
            output["min_value_range"] = self.config.min_alarm_value
        
        if self.config.max_alarm:
            output["max_value_range"] = self.config.max_alarm_value

        return super().create_publish_format(additional_data=output)

    def create_extended_info(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Extends the base node information with additional metadata.
        """

        return super().create_extended_info(additional_data=additional_data)

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Prepares numeric data for database logging including statistical values.

        Args:
            date_time (datetime): The end time for this logging period.
            additional_data (Dict[str, Any]): Additional data to include in the log.

        Returns:
            Dict[str, Any]: Log data including mean, min, and max values with proper formatting.
        """

        output = additional_data.copy()

        if self.config.is_counter:
            output["value"] = calculation.get_scaled_value(self.value, self.config.unit) if self.value is not None else None

        else:
            output["mean_sum"] = calculation.get_scaled_value(self.mean_sum, self.config.unit)
            output["mean_count"] = self.mean_count

            if self.min_value is not None:
                min_value = (
                    round(self.min_value, self.config.decimal_places) if self.config.decimal_places is not None else int(self.min_value)
                )
                output["min_value"] = calculation.get_scaled_value(min_value, self.config.unit)
            else:
                output["min_value"] = self.min_value

            if self.max_value is not None:
                max_value = (
                    round(self.max_value, self.config.decimal_places) if self.config.decimal_places is not None else int(self.max_value)
                )
                output["max_value"] = calculation.get_scaled_value(max_value, self.config.unit)
            else:
                output["max_value"] = self.max_value

        log_data = super().submit_log(date_time=date_time, additional_data=output)
        self.reset_value()
        return log_data
