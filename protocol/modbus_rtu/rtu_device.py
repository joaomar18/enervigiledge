###########EXTERNAL IMPORTS############

import asyncio
import struct
from pymodbus.client import ModbusSerialClient as ModbusRTUClient
from pymodbus import ModbusException
from typing import Optional, Set, Callable
import logging
from typing import Set, Optional
from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node, ModbusRTUNode
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions, BaseCommunicationOptions
from controller.meter.meter import EnergyMeter

#######################################

LoggerManager.get_logger(__name__).setLevel(logging.ERROR)


@dataclass(kw_only=True)
class ModbusRTUOptions(BaseCommunicationOptions):
    """
    Configuration options for Modbus RTU communication.

    Attributes:
        slave_id (int): Modbus slave device ID.
        port (str): Serial port used for communication.
        baudrate (int): Baud rate of the serial connection.
        stopbits (int): Number of stop bits.
        parity (str): Parity mode (e.g., 'N', 'E', 'O').
        bytesize (int): Number of data bits.
        retries (int): Number of retry attempts on failure. Defaults to 3.
    """

    slave_id: int
    port: str
    baudrate: int
    stopbits: int
    parity: str
    bytesize: int
    retries: int = 3


class ModbusRTUEnergyMeter(EnergyMeter):
    """
    Represents an energy meter that communicates over Modbus RTU.

    This class extends the generic EnergyMeter to implement specific functionality
    for devices connected via the Modbus RTU protocol. It prepares a ModbusRTUClient
    with the appropriate serial communication settings and separates Modbus RTU nodes
    from the generic node set.

    Inherits from:
        EnergyMeter: Base class for energy meter abstraction.

    Args:
        id (int): Unique identifier of the energy meter.
        name (str): Display name of the meter.
        publish_queue (asyncio.Queue): Queue used to publish processed meter data to MQTT.
        measurements_queue (asyncio.Queue): Queue for pushing measurements to be logged.
        meter_type (EnergyMeterType): Specifies the type of meter (EnergyMeterType.SINGLE_PHASE, EnergyMeterType.THREE_PHASE).
        meter_options (EnergyMeterOptions): General configuration options for the meter.
        communication_options (ModbusRTUOptions): Serial communication parameters specific to Modbus RTU.
        nodes (set[Node]): Set of nodes representing individual measurement points.
        on_connection_change (Callable[[int, bool], None] | None): Optional callback triggered when the device connection state changes.
            Expects two parameters: device id (int) and state (bool).

    Attributes:
        nodes (set[Node]): All nodes associated with this meter.
        modbus_rtu_nodes (set[ModbusRTUNode]): Subset of nodes specific to Modbus RTU.
        communication_options (ModbusRTUOptions): Connection configuration used to initialize the client.
        client (Optional[ModbusRTUClient]): Instance of the Modbus RTU client used for communication.
    """

    def __init__(
        self,
        id: int,
        name: str,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        communication_options: ModbusRTUOptions,
        nodes: Optional[Set[Node]] = None,
        on_connection_change: Callable[[int, bool], bool] | None = None,
    ):
        super().__init__(
            id=id,
            name=name,
            protocol=Protocol.MODBUS_RTU,
            publish_queue=publish_queue,
            measurements_queue=measurements_queue,
            meter_type=meter_type,
            meter_options=meter_options,
            communication_options=communication_options,
            nodes=nodes if nodes else set(),
            on_connection_change=on_connection_change,
        )

        self.communication_options = communication_options

        self.client: Optional[ModbusRTUClient] = None

        self.nodes = nodes if nodes else set()
        self.modbus_rtu_nodes: Set[ModbusRTUNode] = {node for node in self.nodes if isinstance(node, ModbusRTUNode)}

        self.run_connection_task = False
        self.run_receiver_task = False

        self.connection_task: asyncio.Task | None = None
        self.receiver_task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Starts the Modbus RTU energy meter background tasks for connection management and data polling.
        """

        if self.client is not None:
            raise RuntimeError(f"Modbus RTU Client for device {self.name} is already running")

        self.client = ModbusRTUClient(
            port=self.communication_options.port,
            baudrate=self.communication_options.baudrate,
            stopbits=self.communication_options.stopbits,
            parity=self.communication_options.parity,
            bytesize=self.communication_options.bytesize,
            timeout=float(self.communication_options.timeout),
            retries=self.communication_options.retries,
        )
        loop = asyncio.get_event_loop()
        self.run_connection_task = True
        self.run_receiver_task = True
        self.connection_task = loop.create_task(self.manage_connection())
        self.receiver_task = loop.create_task(self.receiver())

    async def stop(self) -> None:
        """
        Stops the Modbus RTU energy meter tasks and closes the connection.
        """

        if self.client is None:
            raise RuntimeError(f"Modbus RTU Client for device {self.name} is already not running")

        self.run_connection_task = False
        self.run_receiver_task = False
        self.connection_task = None
        self.receiver_task = None
        await self.close_connection()

    def __require_client(self) -> ModbusRTUClient:
        """
        Return the active Modbus RTU client object.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.client is None:
            raise RuntimeError(f"Modbus RTU client for device {self.name} with id {self.id} is not instantiated properly. ")
        return self.client

    async def manage_connection(self):
        """
        Manages the RTU connection lifecycle. Continuously tries to connect to the client,
        monitors its status, and handles reconnection if the link is lost.
        """

        logger = LoggerManager.get_logger(__name__)
        client = self.__require_client()

        while self.run_connection_task:
            try:
                logger.info(f"Trying to connect to client {self.name} with id {self.id}...")
                self.network_connected = client.connect()

                if not self.network_connected:
                    logger.warning(f"Failed to connect to client {self.name} with id {self.id}")
                    await asyncio.sleep(3)
                    continue

                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.network_connected:
                    await asyncio.sleep(3)

                logger.warning(f"Client {self.name} with id {self.id} disconnected")

            except Exception as e:
                logger.error(f"Unexpected error during connection management: {e}")
                self.set_network_state(False)
                await asyncio.sleep(3)

    async def receiver(self):
        """
        Continuously reads data from Modbus RTU nodes and updates their values.
        Handles connection loss and logs per-node failures without stopping the loop.
        """

        logger = LoggerManager.get_logger(__name__)
        client = self.__require_client()

        while self.run_receiver_task:
            try:
                if self.network_connected:
                    tasks = [asyncio.to_thread(self.read_float, client, node) for node in self.modbus_rtu_nodes if node.config.enabled]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    failed_nodes = []

                    for node, result in zip(self.modbus_rtu_nodes, results):
                        if isinstance(result, Exception):
                            failed_nodes.append(node.config.name)
                            node.processor.set_value(None)
                            continue

                        node.processor.set_value(result)

                    if failed_nodes:
                        logger.warning(
                            f"Failed to read {len(failed_nodes)} nodes from device {self.name} with id {self.id}: {', '.join(failed_nodes)}"
                        )

                    if any(node.connected for node in self.modbus_rtu_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                    await self.process_nodes()

            except ModbusException as e:
                logger.error(f"{e}")
                self.set_network_state(False)

            except Exception as e:
                logger.exception(f"{e}")
                self.set_network_state(False)

            await asyncio.sleep(self.communication_options.read_period)

    def read_float(self, client: ModbusRTUClient, node: ModbusRTUNode):
        """
        Reads a 32-bit float value from two consecutive Modbus holding registers.

        Args:
            client (ModbusRTUClient): The Modbus RTU client used for communication.
            node (ModbusRTUNode): The node containing the register address and configuration.

        Returns:
            float: The value read from the specified node.

        Raises:
            ModbusException: If the response is incomplete or a Modbus-related error occurs.
            Exception: If an unexpected error occurs during the read operation.
        """

        try:
            response = client.read_holding_registers(
                address=node.register, count=2, device_id=self.communication_options.slave_id, no_response_expected=False
            )

            if not response or not hasattr(response, "registers") or len(response.registers) < 2:
                raise ModbusException("Incomplete response")

            raw_value = struct.pack(">HH", response.registers[0], response.registers[1])
            value = struct.unpack(">f", raw_value)[0]

            node.set_connection_state(True)
            return value

        except ModbusException as e:
            node.set_connection_state(False)
            raise ModbusException(f"Couldn't read node {node.config.name} from device {self.name} with id {self.id}") from e

        except Exception as e:
            node.set_connection_state(False)
            raise Exception(f"Unexpected error reading node {node.config.name} from device {self.name} with id {self.id}") from e

    async def close_connection(self) -> None:
        """
        Closes the Modbus RTU client connection and updates connection state.
        """

        if self.client is not None:
            self.client.close()
        self.set_network_state(False)
        self.client = None
