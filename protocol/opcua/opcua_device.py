############### EXTERNAL IMPORTS ###############

import asyncio
from asyncua import Client
from dataclasses import dataclass, asdict
from typing import Dict, Set, Optional, Any, Callable
import logging

############### LOCAL IMPORTS ###############

from util.debug import LoggerManager
from controller.node import Node, OPCUANode
from controller.types import Protocol, EnergyMeterType, EnergyMeterOptions
from controller.meter.meter import EnergyMeter

LoggerManager.get_logger(__name__).setLevel(logging.ERROR)


@dataclass
class OPCUAOptions:
    """
    Configuration container for OPC UA connection parameters.

    This class defines the necessary configuration parameters for establishing
    and managing a connection with an OPC UA server.

    Attributes:
        url (str): Endpoint URL of the OPC UA server (e.g., 'opc.tcp://192.168.0.100:4840').
        username (Optional[str]): Optional username for authentication (default: None).
        password (Optional[str]): Optional password for authentication (default: None).
        read_period (int): Time in seconds between consecutive read cycles (default: 5).
        timeout (int): Timeout duration in seconds for connection and read operations (default: 5).
    """

    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    read_period: int = 5
    timeout: int = 5

    def get_communication_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current OPC-UA options.

        Returns:
            Dict[str, Any]: A dictionary with all configuration flags and their values.
        """

        return asdict(self)


class OPCUAEnergyMeter(EnergyMeter):
    """
    Represents an energy meter that communicates over the OPC UA protocol.

    This class extends the generic EnergyMeter to implement specific functionality
    for devices connected via OPC UA. It manages the OPC UA client lifecycle,
    node reading routines, and connection handling.

    Inherits from:
        EnergyMeter: Base class for energy meter abstraction.

    Args:
        id (int): Unique identifier of the energy meter.
        name (str): Display name of the meter.
        publish_queue (asyncio.Queue): Queue used to publish processed meter data to MQTT.
        measurements_queue (asyncio.Queue): Queue for pushing measurements to be logged.
        meter_type (EnergyMeterType): Specifies the type of meter (EnergyMeterType.SINGLE_PHASE, EnergyMeterType.THREE_PHASE).
        meter_options (EnergyMeterOptions): General configuration options for the meter.
        communication_options (OPCUAOptions): Connection configuration parameters for the OPC UA client.
        nodes (Optional[Set[Node]]): Set of nodes representing individual measurement points.
        on_connection_change (Callable[[int, bool], None] | None): Optional callback triggered when the device connection state changes.
            Expects two parameters: device id (int) and state (bool).

    Attributes:
        client (Optional[asyncua.Client]): Instance of the OPC UA client used for communication.
        communication_options (OPCUAOptions): Configuration used to initialize the OPC UA client.
        nodes (Set[Node]): All nodes associated with this meter.
        opcua_nodes (Set[OPCUANode]): Subset of nodes specific to OPC UA.
    """

    def __init__(
        self,
        id: int,
        name: str,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        communication_options: OPCUAOptions,
        nodes: Optional[Set[Node]] = None,
        on_connection_change: Callable[[int, bool], None] | None = None,
    ):
        super().__init__(
            id=id,
            name=name,
            protocol=Protocol.OPC_UA,
            publish_queue=publish_queue,
            measurements_queue=measurements_queue,
            meter_type=meter_type,
            meter_options=meter_options,
            communication_options=communication_options,
            meter_nodes=nodes if nodes else set(),
            on_connection_change=on_connection_change,
        )

        self.communication_options = communication_options
        self.client: Optional[Client] = None

        self.nodes = nodes if nodes else set()
        self.opcua_nodes: Set[OPCUANode] = {node for node in self.nodes if isinstance(node, OPCUANode)}

        self.run_connection_task = False
        self.run_receiver_task = False

        self.connection_task: asyncio.Task | None = None
        self.receiver_task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Starts the OPC UA energy meter background tasks for connection management and data acquisition.
        """

        if self.client is not None:
            raise RuntimeError(f"OPC UA Client for device {self.name} is already running")

        self.client = Client(url=self.communication_options.url, timeout=self.communication_options.timeout)
        if self.communication_options.username:
            self.client.set_user(self.communication_options.username)
            self.client.set_password(self.communication_options.password)

        loop = asyncio.get_event_loop()
        self.run_connection_task = True
        self.run_receiver_task = True
        self.connection_task = loop.create_task(self.manage_connection())
        self.receiver_task = loop.create_task(self.receiver())

    async def stop(self) -> None:
        """
        Stops the OPC UA energy meter tasks and closes the connection.
        """

        if self.client is None:
            raise RuntimeError(f"OPC UA Client for device {self.name} is already not running")

        self.run_connection_task = False
        self.run_receiver_task = False
        self.connection_task = None
        self.receiver_task = None
        await self.close_connection()

    async def manage_connection(self):
        """
        Manages the OPC UA client connection lifecycle.

        This asynchronous task continuously attempts to establish and maintain a stable
        connection with the OPC UA server. It performs the following actions:

            - Attempts to connect to the configured OPC UA server.
            - Monitors the connection status in a loop using `check_connection()`.
            - Handles disconnection events and triggers a reconnection attempt.
            - Updates the internal connection state and logs status changes.

        In case of connection loss or unexpected errors, the client is properly closed
        and a reconnection is attempted after a short delay.
        """

        logger = LoggerManager.get_logger(__name__)

        while self.run_connection_task:
            try:
                logger.info(f"Trying to connect OPC UA client {self.name} with id {self.id}...")
                await self.client.connect()
                self.set_network_state(True)
                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.network_connected:
                    await asyncio.sleep(3)
                    await self.client.check_connection()

            except Exception as e:
                if self.network_connected:
                    logger.warning(f"Client {self.name} with id {self.id} disconnected")
                logger.warning(f"Connection error on client {self.name} with id {self.id}: {e}")
                self.set_network_state(False)
                await asyncio.sleep(3)

    async def receiver(self):
        """
        Continuously reads data from all OPC UA nodes and updates their values.

        This asynchronous task runs in a loop and performs the following operations
        while the connection is open:

            - Creates individual read tasks for each OPC UA node.
            - Collects all results using `asyncio.gather`, handling exceptions per node.
            - Sets each node’s value or flags it as failed (sets to None) if reading fails.
            - Logs any failed node readings for diagnostic purposes.
            - Calls `process_nodes()` to handle post-read logic (e.g., logging, publishing).

        In case of unexpected exceptions, the client connection is closed and will be
        re-established by the `manage_connection` task.
        """

        logger = LoggerManager.get_logger(__name__)

        while self.run_receiver_task:
            try:
                if self.network_connected:
                    tasks = [asyncio.create_task(self.read_float(node)) for node in self.opcua_nodes if node.enabled]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    failed_nodes = []

                    for node, result in zip(self.opcua_nodes, results):
                        if isinstance(result, Exception):
                            failed_nodes.append(node.name)
                            node.set_value(None)
                            continue
                        node.set_value(result)

                    if failed_nodes:
                        logger.warning(f"Failed to read {len(failed_nodes)} nodes from {self.name}: {', '.join(failed_nodes)}")

                    if any(node.connected for node in self.opcua_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                    await self.process_nodes()

            except Exception as e:
                logger.exception(f"{e}")
                self.set_connection_state(False)

            await asyncio.sleep(self.communication_options.read_period)

    async def read_float(self, node: OPCUANode):
        """
        Reads a float value from a specific OPC UA node.

        This method attempts to read the value of the given node using its configured
        OPC UA Node ID. The read value is converted to float before being returned.

        Args:
            node (OPCUANode): The node to read the value from.

        Returns:
            float: The value read from the OPC UA server.

        Raises:
            Exception: If the read operation fails or returns an invalid value.

        Notes:
            - Updates the node connection state based on the read result.
            - If the read fails, the node value is not updated and an exception is raised.
        """

        try:
            opc_node = self.client.get_node(node.node_id)
            value = await opc_node.read_value()
            node.set_connection_state(True)
            return float(value)
        except Exception as e:
            node.set_connection_state(False)
            raise Exception(f"Failed to read {node.name} from {self.name}") from e

    async def close_connection(self):
        """
        Closes the OPC UA client connection and updates connection state.
        """

        try:
            if self.network_connected:
                await self.client.disconnect()
        except Exception:
            pass
        self.set_network_state(False)
        self.client = None
