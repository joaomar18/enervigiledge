###########EXTERNAL IMPORTS############

import asyncio
from typing import Optional, Dict, Set, Any
from abc import ABC

#######################################

#############LOCAL IMPORTS#############

from mqtt.client import MQTTMessage
from controller.node import Node

#######################################


class Protocol:
    """
    Enumeration of supported device communication protocols.

    Attributes:
        OPC_UA (str): OPC UA protocol identifier.
        MQTT (str): MQTT protocol identifier.
        MODBUS_TCP (str): Modbus TCP protocol identifier.
        MODBUS_RTU (str): Modbus RTU protocol identifier.
        VALID_PROTOCOLS (set): Set containing all supported protocol strings for validation.
    """

    OPC_UA = "OPC_UA"
    MQTT = "MQTT"
    MODBUS_TCP = "MODBUS_TCP"
    MODBUS_RTU = "MODBUS_RTU"

    VALID_PROTOCOLS = {OPC_UA, MQTT, MODBUS_TCP, MODBUS_RTU}


class Device(ABC):
    """
    Abstract base class representing a generic device.

    Attributes:
        id (int): Unique device identifier.
        name (str): Display name of the device.
        protocol (str): Communication protocol used by the device.
        publish_queue (asyncio.Queue): Queue for outgoing MQTT messages.
        measurements_queue (asyncio.Queue): Queue for logging measurements.
        nodes (Set[Node]): Set of nodes (data points) for the device.
        connected (bool): Indicates whether the device is currently connected.
    """

    def __init__(self, id: int, name: str, protocol: str, publish_queue: asyncio.Queue, measurements_queue: asyncio.Queue, nodes: Set[Node]):
        self.id = id
        self.name = name
        self.connected = False
        self.publish_queue = publish_queue
        self.measurements_queue = measurements_queue
        self.nodes = nodes
        if protocol not in Protocol.VALID_PROTOCOLS:
            raise ValueError(f"Invalid protocol: {protocol}")

        self.protocol = protocol

    def set_connection_state(self, state: bool):
        """
        Updates the connection state of the device.

        Args:
            state (bool): True if the node is connected, False otherwise.
        """

        self.connected = state

    def get_device_state(self) -> Dict[str, Any]:
        """
        Returns the current state of the device in a dictionary format
        suitable for MQTT publishing.

        Returns:
            Dict[str, Any]: Dictionary containing the device's ID, name, protocol,
            and connection status.
        """

        return {"id": self.id, "name": self.name, "protocol": self.protocol, "connected": self.connected}


class DeviceManager:
    """
    Manages a collection of devices and periodically publishes their state.

    Attributes:
        devices (Set[Device]): Set of registered devices.
        publish_queue (asyncio.Queue): Queue used for publishing MQTT messages.
    """

    def __init__(self, publish_queue: asyncio.Queue):
        self.devices: Set[Device] = set()
        self.publish_queue = publish_queue
        self.start()

    def start(self) -> None:
        """
        Starts the background task that handles device state publishing.
        """

        loop = asyncio.get_event_loop()
        self.handler_task = loop.create_task(self.handle_devices())


    def add_device(self, device: Device):
        """
        Registers a new device.

        Args:
            device (Device): The device instance to add.
        """

        self.devices.add(device)

    def delete_device(self, device: Device) -> None:
        """
        Removes a device from the manager if it exists.

        Args:
            device (Device): The device instance to remove.
        """

        self.devices.discard(device)
        

    def get_device(self, name: str, device_id: int) -> Optional[Device]:
        """
        Retrieves a device by name and ID.

        Args:
            name (str): The name of the device.
            device_id (int): The unique identifier of the device.

        Returns:
            Optional[Device]: The matched device, or None if not found.
        """

        return next((device for device in self.devices if device.name == name and device.id == device_id), None)

    async def handle_devices(self):
        """
        Periodically publishes the state of all registered devices.
        """

        while True:
            await self.publish_devices_state()
            await asyncio.sleep(10)

    async def publish_devices_state(self):
        """
        Publishes a dictionary of all device states to the MQTT queue.
        """

        topic = "devices_state"
        payload: Dict[int, Dict[str, Any]] = {device.id: device.get_device_state() for device in self.devices}
        await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))
