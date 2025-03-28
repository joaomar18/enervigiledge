###########EXERTNAL IMPORTS############

import asyncio
import time
import threading
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

import util.debug as debug

#######################################


class Protocol:  # PROTOCOL CLASS
    OPC_UA = "OPC_UA"
    MQTT = "MQTT"
    MODBUS_TCP = "MODBUS_TCP"
    MODBUS_RTU = "MODBUS_RTU"


class NodeType:
    INT = "INT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"
    STRING = "STRING"


class Node:  # ABSTRACT NODE CLASS
    def __init__(
        self,
        name: str,
        type: NodeType,
        unit: str,
        incremental_node=False,
        positive_incremental=False,
        publish: bool = True,
        calculated: bool = False,
        logging: bool = False,
        logging_period: int = 0,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
        on_value_change: callable = None,
    ):
        self.name = name
        self.type = type
        self.unit = unit
        self.incremental_node = incremental_node
        self.positive_incremental = positive_incremental
        self.publish = publish
        self.calculated = calculated
        self.logging = logging
        self.logging_period = logging_period
        self.min_alarm = min_alarm
        self.max_alarm = max_alarm
        self.min_alarm_value = min_alarm_value
        self.max_alarm_value = max_alarm_value
        self.min_alarm_state = False
        self.max_alarm_state = False
        self.on_value_change = on_value_change

        self.initial_value = None  # used for incremental nodes (for example energy nodes)
        self.value = None
        self.timestamp: float = None  # timestamp of the current measurement
        self.elapsed_time: float = None  # elapsed time since the last measure to the current measure
        self.positive_direction: False #used to keep track of incremental direction
        self.negative_direction: False #used to keep track of incremental direction
        self.min_value = None
        self.max_value = None
        self.mean_value = None
        self.mean_sum = 0
        self.mean_count = 0

    def set_unit(self, unit: str):
        self.unit = unit

    def set_incremental_node(self, incremental_node: bool):
        self.incremental_node = incremental_node

    def set_logging(self, logging: bool, logging_period: int):
        self.logging = logging
        self.logging_period = logging_period

    def set_alarms(
        self,
        min_alarm: bool,
        max_alarm: bool,
        min_alarm_value: float,
        max_alarm_value: float,
    ):
        self.min_alarm = min_alarm
        self.max_alarm = max_alarm
        self.min_alarm_value = min_alarm_value
        self.max_alarm_value = max_alarm_value

    def check_alarms(self, value):
        if self.min_alarm and value < self.min_alarm_value:
            self.min_alarm_state = True
        if self.max_alarm and value > self.max_alarm_value:
            self.max_alarm_state = True

    def reset_alarms(self):
        self.min_alarm_state = False
        self.max_alarm_state = False

    def set_value(self, value):

        if self.timestamp is None:
            self.timestamp = time.time()
            self.elapsed_time = 0.0
        else:
            current_timestamp = time.time()
            self.elapsed_time = current_timestamp - self.timestamp
            self.timestamp = current_timestamp

        if self.incremental_node:
            if self.initial_value is None:
                self.initial_value = value
            elif self.positive_incremental:
                
                calculated_value = value + self.initial_value
                self.positive_direction = calculated_value > self.value
                self.negative_direction = calculated_value < self.value
                
                self.value = calculated_value
                
            elif not self.positive_incremental:
                
                calculated_value = value - self.initial_value
                self.positive_direction = calculated_value > self.value
                self.negative_direction = calculated_value < self.value
                
                self.value = calculated_value

        else:

            self.value = value

        if (
            self.type != NodeType.BOOL
            and self.type != NodeType.STRING
            and not self.incremental_node
        ):

            self.mean_sum += value
            self.mean_count += 1
            self.mean_value = self.mean_sum / self.mean_count

            if self.min_value is None:
                self.min_value = value
            elif self.min_value > value:
                self.min_value = value

            if self.max_value is None:
                self.max_value = value
            elif self.max_value < value:
                self.max_value = value

            self.check_alarms(value)

        if self.on_value_change:
            self.on_value_change(self)

    def reset_value(self):
        self.initial_value = None
        self.min_value = None
        self.max_value = None
        self.positive_direction: False
        self.negative_direction: False
        self.mean_value = None
        self.timestamp = None
        self.elapsed_time = None
        self.mean_sum = 0
        self.mean_count = 0

    def get_publish_format(self) -> dict[str]:

        output = dict()
        output["value"] = self.value
        output["type"] = self.type
        output["unit"] = self.unit
        output["min_alarm_state"] = self.min_alarm_state
        output["max_alarm_state"] = self.max_alarm_state

        return output


class Device(ABC):  # ABSTRACT DEVICE CLASS
    def __init__(
        self,
        id: int,
        name: str,
        protocol: int,
        publish_queue: asyncio.Queue,
    ):
        self.id = id
        self.name = name
        self.connected = False
        self.publish_queue = publish_queue
        try:
            if protocol in [
                Protocol.OPC_UA,
                Protocol.MQTT,
                Protocol.MODBUS_TCP,
                Protocol.MODBUS_RTU,
            ]:
                self.protocol = protocol
            else:
                raise ValueError(f"Invalid protocol: {protocol}")
        except ValueError as e:
            debug.logger.exception(e)

    def stringify(self) -> str:  # returns a string defining the device
        return (
            "id:"
            + str(self.id)
            + ";name:"
            + self.name
            + ";protocol:"
            + str(self.protocol)
        )
