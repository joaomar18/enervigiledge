###########EXTERNAL IMPORTS############

import asyncio
import struct
from pymodbus.client import ModbusSerialClient as ModbusRTUClient
from pymodbus import ModbusException
from typing import Optional, Set
import logging
from typing import List, Dict, Set, Optional, Any
from dataclasses import dataclass, asdict

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.device import Protocol
from controller.node import Node, ModbusRTUNode
from controller.meter import EnergyMeter, EnergyMeterType, EnergyMeterOptions

#######################################

LoggerManager.get_logger(__name__).setLevel(logging.ERROR)


@dataclass
class ModbusRTUOptions:
    """
    A simple configuration container for Modbus RTU communication parameters.

    Attributes:
        slave_id (int): The Modbus slave ID of the device.
        port (str): The serial port used for communication (e.g., '/dev/ttyUSB0').
        baudrate (int): The baud rate of the serial connection (e.g., 9600, 19200).
        stopbits (int): Number of stop bits used in the serial connection (usually 1 or 2).
        parity (str): Parity setting ('N' for None, 'E' for Even, 'O' for Odd).
        bytesize (int): Number of data bits (commonly 8).
        read_period (int): Time in seconds between consecutive read cycles.
        timeout (float): Timeout duration in seconds for a single request.
        retries (int): Number of retry attempts before marking a read as failed.
    """

    slave_id: int
    port: str
    baudrate: int
    stopbits: int
    parity: str
    bytesize: int
    read_period: int
    timeout: float
    retries: int

    def get_connection_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current modbus rtu options.

        Returns:
            Dict[str, Any]: A dictionary with all configuration flags and their values.
        """

        return asdict(self)


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
        connection_options (ModbusRTUOptions): Serial communication parameters specific to Modbus RTU.
        nodes (set[Node]): Set of nodes representing individual measurement points.

    Attributes:
        nodes (set[Node]): All nodes associated with this meter.
        modbus_rtu_nodes (set[ModbusRTUNode]): Subset of nodes specific to Modbus RTU.
        connection_options (ModbusRTUOptions): Connection configuration used to initialize the client.
        client (ModbusRTUClient): Instance of the Modbus RTU client used for communication.
        connection_open (bool): Flag indicating whether the RTU connection is currently open.
    """

    def __init__(
        self,
        id: int,
        name: str,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        connection_options: ModbusRTUOptions,
        nodes: Optional[Set[Node]] = None,
    ):
        super().__init__(
            id=id,
            name=name,
            protocol=Protocol.MODBUS_RTU,
            publish_queue=publish_queue,
            measurements_queue=measurements_queue,
            meter_type=meter_type,
            meter_options=meter_options,
            meter_nodes=nodes if nodes else set(),
        )

        self.connection_options = connection_options

        self.client = ModbusRTUClient(
            port=self.connection_options.port,
            baudrate=self.connection_options.baudrate,
            stopbits=self.connection_options.stopbits,
            parity=self.connection_options.parity,
            bytesize=self.connection_options.bytesize,
            timeout=self.connection_options.timeout,
            retries=self.connection_options.retries,
        )

        self.nodes = nodes if nodes else set()
        self.modbus_rtu_nodes: Set[ModbusRTUNode] = {node for node in self.nodes if isinstance(node, ModbusRTUNode)}
        self.connection_open = False
        self.start()

    def start(self) -> None:
        """
        Starts the background tasks for the energy meter.

        Tasks created:
            - `connection_task`: Manages the Modbus RTU connection state.
            - `receiver_task`: Continuously polls data from the Modbus RTU client.

        These tasks are run concurrently using the asyncio event loop.
        """

        loop = asyncio.get_event_loop()
        self.connection_task: asyncio.Task = loop.create_task(self.manage_connection())
        self.receiver_task: asyncio.Task = loop.create_task(self.receiver())

    async def manage_connection(self):
        """
        Manages the RTU connection lifecycle. Continuously tries to connect to the client,
        monitors its status, and handles reconnection if the link is lost.
        """

        logger = LoggerManager.get_logger(__name__)

        while True:
            try:
                logger.info(f"Trying to connect to client {self.name} with id {self.id}...")
                self.connection_open = self.client.connect()

                if not self.connection_open:
                    logger.warning(f"Failed to connect to client {self.name} with id {self.id}")
                    await asyncio.sleep(3)
                    continue

                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.connection_open:
                    await asyncio.sleep(3)

                logger.warning(f"Client {self.name} with id {self.id} disconnected")

            except Exception as e:
                logger.error(f"Unexpected error during connection management: {e}")

                if self.connection_open:
                    self.close_connection()

                await asyncio.sleep(3)

    async def receiver(self):
        """
        Continuously reads data from Modbus RTU nodes and updates their values.
        Handles connection loss and logs per-node failures without stopping the loop.
        """

        logger = LoggerManager.get_logger(__name__)

        while True:
            try:
                if self.connection_open:
                    tasks = [asyncio.to_thread(self.read_float, node) for node in self.modbus_rtu_nodes if node.enabled]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    failed_nodes = []

                    for node, result in zip(self.modbus_rtu_nodes, results):
                        if isinstance(result, Exception):
                            failed_nodes.append(node.name)
                            node.set_value(None)
                            continue

                        node.set_value(result)

                    if failed_nodes:
                        logger.warning(f"Failed to read {len(failed_nodes)} nodes from device {self.name} with id {self.id}: {', '.join(failed_nodes)}")

                    if any(node.connected for node in self.modbus_rtu_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                    await self.process_nodes()

            except ModbusException as e:
                logger.error(f"{e}")
                self.close_connection()

            except Exception as e:
                logger.exception(f"{e}")
                self.close_connection()

            await asyncio.sleep(self.connection_options.read_period)

    def read_float(self, node: ModbusRTUNode):
        """
        Reads a 32-bit float value from two consecutive Modbus holding registers
        and updates the node's connection state accordingly.

        Args:
            node (ModbusRTUNode): The node to read from.

        Returns:
            float: The decoded float value from the Modbus device.

        Raises:
            ModbusException: If the Modbus client fails to read or returns invalid data.
            Exception: For any unexpected error during processing.
        """

        try:
            response = self.client.read_holding_registers(address=node.register, count=2, slave=self.connection_options.slave_id, no_response_expected=False)

            if not response or not hasattr(response, "registers") or len(response.registers) < 2:
                raise ModbusException("Incomplete response")

            raw_value = struct.pack(">HH", response.registers[0], response.registers[1])
            value = struct.unpack(">f", raw_value)[0]

            node.set_connection_state(True)
            return value

        except ModbusException as e:
            node.set_connection_state(False)
            raise ModbusException(f"Couldn't read node {node.name} from device {self.name} with id {self.id}") from e

        except Exception as e:
            node.set_connection_state(False)
            raise Exception(f"Unexpected error reading node {node.name} from device {self.name} with id {self.id}") from e

    def close_connection(self):
        """
        Closes the Modbus RTU client connection and updates internal state.

        This method:
            - Marks the device as disconnected.
            - Closes the Modbus RTU client.
            - Sets the `connection_open` flag to False.
        """

        self.set_connection_state(False)
        self.client.close()
        self.connection_open = False

    def get_device_state(self) -> Dict[str, Any]:
        """
        Returns the current state of the modbus energy meter device, including metadata and configuration.

        Returns:
            Dict[str, Any]: A dictionary containing the device's:
                - ID
                - Name
                - Protocol
                - Connection status
                - Meter options
                - Communication options
                - Meter type
        """

        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
            "options": self.meter_options.get_meter_options(),
            "communication_options": self.connection_options.get_connection_options(),
            "type": self.meter_type,
        }
