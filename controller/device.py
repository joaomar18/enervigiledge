###########EXTERNAL IMPORTS############

import asyncio
from typing import Dict, Set, Any, Callable
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol
from model.controller.device import DeviceHistoryStatus
from controller.node.node import Node

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
        on_connection_change (Callable[[int, bool], bool] | None): Optional callback triggered when the device connection state changes.
            Expects two parameters: device id (int) and state (bool).
    """

    def __init__(
        self,
        id: int,
        name: str,
        protocol: Protocol,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        nodes: Set[Node],
        on_connection_change: Callable[[int, bool], bool] | None = None,
    ):
        self.id = id
        self.name = name
        self.connected = False
        self.network_connected = False
        self.publish_queue = publish_queue
        self.measurements_queue = measurements_queue
        self.nodes = nodes
        self.on_connection_change = on_connection_change
        if protocol not in Protocol.valid_protocols():
            raise ValueError(f"Invalid protocol: {protocol}")

        self.protocol = protocol

    @abstractmethod
    async def start(self) -> None:
        """
        Starts the device operations and communication protocol.
        """

        pass

    @abstractmethod
    async def stop(self) -> None:
        """
        Stops the device operations and cleans up resources.
        """

        pass

    def set_connection_state(self, state: bool):
        """
        Updates the device connection state.
        """

        if self.on_connection_change and state != self.connected:
            self.on_connection_change(self.id, state)

        self.connected = state

    def set_network_state(self, state: bool):
        """
        Updates the device network connectivity state.
        """

        self.network_connected = state
        if not state:
            self.connected = state

    def get_device(self) -> Dict[str, Any]:
        """
        Returns the device object in a dictionary format
        suitable for MQTT publishing.

        Returns:
            Dict[str, Any]: Dictionary containing the device's ID, name, protocol,
            and connection status.
        """

        return {"id": self.id, "name": self.name, "protocol": self.protocol, "connected": self.connected}

    def get_device_info(self, get_history_method: Callable[[int], DeviceHistoryStatus]) -> Dict[str, Any]:
        """
        Returns comprehensive device information including history status.

        Args:
            get_history_method: Callback to retrieve device history status.

        Returns:
            Dict[str, Any]: Device info with ID, name, protocol, status, and history.
        """

        history = get_history_method(self.id)
        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
            "history": history.get_status() if history else None,
        }
