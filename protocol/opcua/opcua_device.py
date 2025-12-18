############### EXTERNAL IMPORTS ###############

import asyncio
import asyncua
from typing import Set, List, Dict, Optional, Any, Callable
import logging

############### LOCAL IMPORTS ###############

from util.debug import LoggerManager
from controller.node.node import Node, OPCUANode
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions
from model.controller.protocol.opc_ua import OPCUAOptions, OPCUANodeType
from controller.meter.meter import EnergyMeter

#######################################


LoggerManager.get_logger(__name__).setLevel(logging.ERROR)


class OPCUAEnergyMeter(EnergyMeter):
    """
    Energy meter implementation that communicates using the OPC UA protocol.

    This class extends `EnergyMeter` with OPC UA–specific connection handling,
    asynchronous node reading, and lifecycle management for an `asyncua.Client`.
    It maps each node type to an appropriate async value reader and tracks the
    connection state of both the device and its nodes.

    Inherits:
        EnergyMeter: Base abstraction for energy meter devices.

    Args:
        id (int): Unique identifier of the meter.
        name (str): Display name of the meter.
        publish_queue (asyncio.Queue): Queue used to publish processed readings.
        measurements_queue (asyncio.Queue): Queue used to push values for logging.
        meter_type (EnergyMeterType): Single-phase or three-phase meter type.
        meter_options (EnergyMeterOptions): General meter configuration.
        communication_options (OPCUAOptions): OPC UA connection parameters.
        nodes (Optional[Set[Node]]): Node definitions for this meter.
        on_connection_change (Callable[[int, bool], None] | None): Optional callback
            invoked when the meter's connection state changes.

    Attributes:
        client (Optional[asyncua.Client]): OPC UA client instance.
        communication_options (OPCUAOptions): Connection configuration.
        nodes (Set[Node]): All nodes associated with this meter.
        opcua_nodes (Set[OPCUANode]): Nodes specific to the OPC UA protocol.
        get_value_map (Dict[OPCUANodeType, Callable]): Mapping of node types to
            their asynchronous value-reader functions.
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
        on_connection_change: Callable[[int, bool], bool] | None = None,
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
            nodes=nodes if nodes else set(),
            on_connection_change=on_connection_change,
        )

        self.communication_options = communication_options
        self.client: Optional[asyncua.Client] = None

        self.nodes = nodes if nodes else set()
        self.opcua_nodes: Set[OPCUANode] = {node for node in self.nodes if isinstance(node, OPCUANode)}

        self.run_connection_task = False
        self.run_receiver_task = False

        self.connection_task: asyncio.Task | None = None
        self.receiver_task: asyncio.Task | None = None

        self.get_value_map: Dict[OPCUANodeType, Callable[[Any], float | int | str | bool]] = {
            OPCUANodeType.FLOAT: self.get_float,
            OPCUANodeType.INT: self.get_int,
            OPCUANodeType.STRING: self.get_string,
            OPCUANodeType.BOOL: self.get_bool,
        }

    async def start(self) -> None:
        """
        Starts the OPC UA energy meter background tasks for connection management and data acquisition.
        """

        if self.client is not None:
            raise RuntimeError(f"OPC UA Client for device {self.name} is already running")

        self.client = asyncua.Client(url=self.communication_options.url, timeout=self.communication_options.timeout)
        if self.communication_options.username is not None and self.communication_options.password is not None:
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

    def __require_client(self) -> asyncua.Client:
        """
        Return the active OPC UA client connection.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.client is None:
            raise RuntimeError(f"OPC UA client for device {self.name} with id {self.id} is not instantiated properly. ")
        return self.client

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
        client = self.__require_client()

        while self.run_connection_task:
            try:
                logger.info(f"Trying to connect OPC UA client {self.name} with id {self.id}...")
                await client.connect()
                self.set_network_state(True)
                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.network_connected:
                    await asyncio.sleep(3)
                    await client.check_connection()

            except Exception as e:
                if self.network_connected:
                    logger.warning(f"Client {self.name} with id {self.id} disconnected")
                logger.warning(f"Connection error on client {self.name} with id {self.id}: {e}")
                self.set_network_state(False)
                await asyncio.sleep(3)

    async def receiver(self):
        """
        Main acquisition loop that reads all OPC UA nodes using a combination of
        batch reads and individual reads. Nodes eligible for batch reading are
        processed together for efficiency, while nodes excluded from batch mode
        are read individually. After reading, node states are updated and
        post-processing logic is executed.

        Runs continuously while the receiver task is active.
        """

        logger = LoggerManager.get_logger(__name__)
        client = self.__require_client()

        while self.run_receiver_task:
            try:
                if self.network_connected:
                    enabled_nodes = [node for node in self.opcua_nodes if node.config.enabled]
                    batch_read_nodes = [node for node in enabled_nodes if node.enable_batch_read]
                    single_read_nodes = [node for node in enabled_nodes if not node.enable_batch_read]

                    await self.process_batch_read(client, batch_read_nodes, single_read_nodes)
                    await self.process_single_reads(client, single_read_nodes)

                    if not enabled_nodes or any(node.connected for node in enabled_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                await self.process_nodes()

            except Exception as e:
                logger.exception(f"{e}")
                self.set_connection_state(False)

            await asyncio.sleep(self.communication_options.read_period)

    async def process_batch_read(
        self, client: asyncua.Client, batch_read_nodes: List[OPCUANode], single_read_nodes: List[OPCUANode]
    ) -> None:
        """
        Perform a batch read for the given OPC UA nodes. If the batch read succeeds,
        each node is assigned its corresponding typed value. If the batch read fails,
        all nodes in the batch are moved to the single-read list so they can be
        retried individually.

        Args:
            client (asyncua.Client): Active OPC UA client connection.
            batch_read_nodes (List[OPCUANode]): Nodes intended for batch reading.
            single_read_nodes (List[OPCUANode]): Nodes that should fall back to individual reads.
        """

        logger = LoggerManager.get_logger(__name__)

        if not batch_read_nodes:
            return

        try:
            batch_values = await self.batch_read_nodes(client, batch_read_nodes)
            for node, result in zip(batch_read_nodes, batch_values):
                node.processor.set_value(result)

        except Exception as e:
            single_read_nodes.extend(batch_read_nodes)
            logger.warning(f"Batch read failed for device {self.name} with id {self.id}: {e}")

    async def process_single_reads(self, client: asyncua.Client, single_read_nodes: List[OPCUANode]) -> None:
        """
        Read each provided OPC UA node individually using separate asynchronous tasks.
        Successful reads update the node’s value, while failed reads set the node value
        to None and trigger failure tracking.

        Args:
            client (asyncua.Client): Active OPC UA client connection.
            single_read_nodes (List[OPCUANode]): Nodes to be read individually.
        """

        logger = LoggerManager.get_logger(__name__)

        if not single_read_nodes:
            return

        tasks = [asyncio.create_task(self.read_node(client, node)) for node in single_read_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_nodes = []

        for node, result in zip(single_read_nodes, results):
            if isinstance(result, Exception):
                failed_nodes.append(node.config.name)
                node.processor.set_value(None)
                continue
            node.processor.set_value(result)

        if failed_nodes:
            logger.warning(f"Failed to read {len(failed_nodes)} nodes from device {self.name} with id {self.id}: {', '.join(failed_nodes)}")

    async def batch_read_nodes(self, client: asyncua.Client, nodes: list[OPCUANode]) -> List[float | int | str | bool]:
        """
        Read multiple OPC UA nodes in a single batch request and convert each
        returned raw value to its configured type.

        Args:
            client (asyncua.Client): Active OPC UA client connection.
            nodes (list[OPCUANode]): Nodes to read in a single batch.

        Returns:
            List[float | int | str | bool]: Typed values in the same order as the nodes.

        Raises:
            Exception: If the batch read operation fails entirely.
        """

        ua_nodes = [client.get_node(node.options.node_id) for node in nodes]

        try:
            values = await client.read_values(ua_nodes)
            typed_values: List[float | int | str | bool] = []
            for i, value in enumerate(values):
                typed_values.append((self.get_value_map[nodes[i].options.type])(value))
            return typed_values

        except Exception as e:
            raise Exception(f"Batch read failed for {self.name}: {e}")

    async def read_node(self, client: asyncua.Client, node: OPCUANode) -> float | int | str | bool:
        """
        Read a value from the given OPC UA node using the appropriate typed getter.

        Args:
            client (asyncua.Client): The OPC UA client used to access the server.
            node (OPCUANode): The node definition containing type and NodeId information.

        Returns:
            float | int | str | bool: The parsed value read from the OPC UA node.

        Raises:
            Exception: If the read operation fails.
        """

        try:
            opc_node = client.get_node(node.options.node_id)
            value = await opc_node.read_value()
            typed_value = (self.get_value_map[node.options.type])(value)
            node.set_connection_state(True)
            return typed_value
        except Exception as e:
            node.set_connection_state(False)
            raise Exception(f"Failed to read {node.config.name} from {self.name}") from e

    def get_float(self, value: Any) -> float:
        """Convert a raw OPC UA value to float."""
        return float(value)

    def get_int(self, value: Any) -> int:
        """Convert a raw OPC UA value to int."""
        return int(value)

    def get_string(self, value: Any) -> str:
        """Convert a raw OPC UA value to string."""
        return str(value)

    def get_bool(self, value: Any) -> bool:
        """Convert a raw OPC UA value to bool."""
        return bool(value)

    async def close_connection(self):
        """
        Closes the OPC UA client connection and updates connection state.
        """

        try:
            if self.network_connected and self.client is not None:
                await self.client.disconnect()
        except Exception:
            pass
        self.set_network_state(False)
        self.client = None
