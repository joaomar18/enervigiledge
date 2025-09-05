###########EXTERNAL IMPORTS############

import asyncio
from typing import Dict, Set, Any
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.types import Protocol
from controller.node import Node

#######################################


class Device(ABC):
    """
    Abstract base class representing a generic device.

    Attributes:
        id (int): Unique device identifier.
        name (str): Display name of the device.
        protocol (Protocol): Communication protocol used by the device.
        publish_queue (asyncio.Queue): Queue for outgoing MQTT messages.
        measurements_queue (asyncio.Queue): Queue for logging measurements.
        nodes (Set[Node]): Set of nodes (data points) for the device.
        connected (bool): Indicates whether the device is currently connected.
        network_connected (bool): Indicates whether the device network is responding.
    """

    def __init__(
        self,
        id: int,
        name: str,
        protocol: Protocol,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        nodes: Set[Node],
    ):
        self.id = id
        self.name = name
        self.connected = False
        self.network_connected = False
        self.publish_queue = publish_queue
        self.measurements_queue = measurements_queue
        self.nodes = nodes
        if protocol not in Protocol.valid_protocols():
            raise ValueError(f"Invalid protocol: {protocol}")

        self.protocol = protocol

    @abstractmethod
    async def start(self) -> None:
        """
        Starts the energy meter device operations and communication protocol.
        """

        pass

    @abstractmethod
    async def stop(self) -> None:
        """
        Stops the energy meter device operations and cleans up resources.
        """

        pass

    def set_connection_state(self, state: bool):
        """
        Updates the device connection state.
        """

        self.connected = state

    def set_network_state(self, state: bool):
        """
        Updates the device network connectivity state.
        """

        self.network_connected = state
        if not state:
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
