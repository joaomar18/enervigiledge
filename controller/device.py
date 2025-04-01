###########EXERTNAL IMPORTS############

from datetime import datetime
import asyncio
import time
import threading
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

import util.debug as debug
from mqtt.client import MQTTMessage

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
        calculate_increment=True,
        publish: bool = True,
        calculated: bool = False,
        logging: bool = False,
        logging_period: int = 15,  # logging period in minutes
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
        decimal_places: int = 3,
        on_value_change: callable = None,
    ):
        self.name = name
        self.type = type
        self.unit = unit
        self.incremental_node = incremental_node
        self.positive_incremental = positive_incremental
        self.calculate_increment = calculate_increment
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
        self.decimal_places = decimal_places
        self.on_value_change = on_value_change

        self.initial_value = None  # used for incremental nodes (for example energy nodes)
        self.value = None
        self.timestamp: float = None  # timestamp of the current measurement
        self.elapsed_time: float = None  # elapsed time since the last measure to the current measure
        self.positive_direction: bool = False  # used to keep track of incremental direction
        self.negative_direction: bool = False  # used to keep track of incremental direction
        self.last_log_datetime: datetime = None  # last logging date / time
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

    def set_alarms(self, min_alarm: bool, max_alarm: bool, min_alarm_value: float, max_alarm_value: float):
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

                self.value = 0.0 if self.type is NodeType.FLOAT else 0

            else:
                if not self.calculate_increment:
                    calculated_value = value
                elif self.positive_incremental:
                    calculated_value = value + self.initial_value
                elif not self.positive_incremental:
                    calculated_value = value - self.initial_value

                if calculated_value > self.value:
                    self.positive_direction = True
                    self.negative_direction = False
                elif calculated_value < self.value:
                    self.positive_direction = False
                    self.negative_direction = True

                self.value = round(number=calculated_value, ndigits=self.decimal_places) if self.type is NodeType.FLOAT else calculated_value

        else:

            if self.value is not None:
                if value > self.value:
                    self.positive_direction = True
                    self.negative_direction = False
                elif value < self.value:
                    self.positive_direction = False
                    self.negative_direction = True

            self.value = round(number=value, ndigits=self.decimal_places) if self.type is NodeType.FLOAT else value

        if self.type != NodeType.BOOL and self.type != NodeType.STRING and not self.incremental_node:

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
        self.positive_direction = False
        self.negative_direction = False
        self.mean_value = None
        self.timestamp = None
        self.elapsed_time = None
        self.mean_sum = 0
        self.mean_count = 0

    def reset_direction(self):
        self.positive_direction = False
        self.negative_direction = False

    def get_publish_format(self) -> dict[str]:
        if self.value is None:
            raise Exception(f"Error: Trying to publish null value on node {self.name} with value {self.value}")

        output = dict()
        output["value"] = self.value
        output["type"] = self.type
        output["unit"] = self.unit

        if self.type != NodeType.BOOL and self.type != NodeType.STRING and not self.incremental_node:
            if self.min_alarm:
                output["min_alarm_state"] = self.min_alarm_state
            if self.max_alarm:
                output["max_alarm_state"] = self.max_alarm_state
        return output

    def submit_log(self, date_time: datetime) -> dict[str]:

        output = dict()
        output["name"] = self.name
        if self.value is not None:
            output["start_time"] = self.last_log_datetime
            output["end_time"] = date_time

            if self.type != NodeType.BOOL and self.type != NodeType.STRING and not self.incremental_node:
                output["mean_value"] = self.mean_value
                output["min_value"] = self.min_value
                output["max_value"] = self.max_value
            else:
                output["value"] = self.value

        self.reset_value()
        self.last_log_datetime = date_time
        return output


class Device(ABC):  # ABSTRACT DEVICE CLASS
    def __init__(self, id: int, name: str, protocol: int, publish_queue: asyncio.Queue):
        self.id = id
        self.name = name
        self.connected = False
        self.publish_queue = publish_queue
        try:
            if protocol in [Protocol.OPC_UA, Protocol.MQTT, Protocol.MODBUS_TCP, Protocol.MODBUS_RTU]:
                self.protocol = protocol
            else:
                raise ValueError(f"Invalid protocol: {protocol}")
        except ValueError as e:
            debug.logger.exception(e)

    def set_connected(self):
        if not self.connected:
            self.connected = True

    def set_disconnected(self):
        if self.connected:
            self.connected = False

    def get_device_state(self) -> dict[str]:
        state_dict: dict[str] = dict()
        state_dict["id"] = self.id
        state_dict["name"] = self.name
        state_dict["protocol"] = self.protocol
        state_dict["connected"] = self.connected
        return state_dict


class DeviceManager:  # DEVICE MANAGER CLASS
    def __init__(self, publish_queue: asyncio.Queue):
        self.devices: set[Device] = set()
        self.publish_queue = publish_queue
        asyncio.get_event_loop().create_task(self.handle_devices())

    def add_device(self, device: Device):
        self.devices.add(device)

    async def handle_devices(self):
        while True:
            await self.publish_devices_state()
            await asyncio.sleep(10)

    async def publish_devices_state(self):
        topic = f"devices_state"
        payload: dict[int] = dict()
        for device in self.devices:
            payload[device.id] = device.get_device_state()
        await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))
