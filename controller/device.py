###########EXTERNAL IMPORTS############

import asyncio
from typing import Optional, Dict, Set, Any
from abc import ABC

#######################################

#############LOCAL IMPORTS#############

from controller.enums import Protocol
from controller.node import Node

#######################################


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
        if protocol not in Protocol.valid_protocols():
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

        print(f"Protocol: {self.protocol}")

        return {"id": self.id, "name": self.name, "protocol": self.protocol, "connected": self.connected}
