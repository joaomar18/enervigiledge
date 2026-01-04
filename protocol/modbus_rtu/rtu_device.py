###########EXTERNAL IMPORTS############

import asyncio
import struct
from pymodbus.pdu import ModbusPDU
from pymodbus.client import ModbusSerialClient as ModbusRTUClient
from pymodbus import ModbusException
from typing import Optional, Set, Dict, List, Callable, Any
import logging

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node, ModbusRTUNode
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions, DeviceHistoryStatus
from model.controller.protocol.modbus_rtu import (
    ModbusRTUOptions,
    ModbusRTUNodeOptions,
    ModbusRTUNodeType,
    ModbusRTUFunction,
    ModbusRTUNodeMode,
    ModbusRTUBatchGroup,
    MODBUS_RTU_TYPE_TO_SIZE_MAP,
)
from controller.meter.device import EnergyMeter

#######################################


LoggerManager.get_logger(__name__).setLevel(logging.ERROR)
ModbusCall = Callable[[ModbusRTUClient, int, int, int, bool], ModbusPDU]


class ModbusRTUEnergyMeter(EnergyMeter):
    """
    Represents an energy meter that communicates over Modbus RTU.
    This class extends EnergyMeter to implement Modbus RTU protocol handling,including client lifecycle management,connection monitoring,and periodic data acquisition.
    It performs optimized reads by grouping nodes into batch requests based on Modbus function and address proximity,with automatic fallback to individual reads when batch operations fail.
    The class decodes raw Modbus responses into typed values(bool,int,float),updates node connection states,and integrates acquired data into the generic EnergyMeter processing pipeline.
    Inherits from:
        EnergyMeter:Base class for energy meter abstraction.
    Args:
        id(int):Unique identifier of the energy meter.
        name(str):Display name of the meter.
        publish_queue(asyncio.Queue):Queue used to publish processed meter data.
        measurements_queue(asyncio.Queue):Queue for pushing measurements to be logged.
        meter_type(EnergyMeterType):Electrical configuration of the meter.
        meter_options(EnergyMeterOptions):General configuration options for the meter.
        communication_options(ModbusRTUOptions):Serial communication parameters for Modbus RTU.
        nodes(set[Node]):Set of nodes representing Modbus measurement points.
        on_connection_change(Callable[[int,bool],None]|None):Optional callback triggered on connection state changes.
    Attributes:
        nodes(set[Node]):All nodes associated with this meter.
        modbus_rtu_nodes(set[ModbusRTUNode]):Subset of nodes using the Modbus RTU protocol.
        communication_options(ModbusRTUOptions):Configuration used to initialize the RTU client.
        client(Optional[ModbusRTUClient]):Active Modbus RTU client instance.
        modbus_function_map(Dict[ModbusRTUFunction,Callable]):Dispatch table mapping Modbus function enums to client read operations.
        get_value_map(Dict[ModbusRTUNodeType,Callable]):Dispatch table mapping node types to value decoding functions.
    Class attributes:
        MAX_BATCH_SPAN(int):Maximum number of consecutive Modbus data units read in a single batch request.
        MAX_ADDRESS_GAP(int):Maximum allowed gap between node addresses within a batch group.
    """

    MAX_BATCH_SPAN = 16
    MAX_ADDRESS_GAP = 2

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

        self.nodes: Set[Node] = nodes if nodes else set()
        self.modbus_rtu_nodes: Set[ModbusRTUNode] = {node for node in self.nodes if isinstance(node, ModbusRTUNode)}

        self.run_connection_task = False
        self.run_receiver_task = False

        self.connection_task: asyncio.Task | None = None
        self.receiver_task: asyncio.Task | None = None

        self.modbus_function_map: Dict[ModbusRTUFunction, ModbusCall] = {
            ModbusRTUFunction.READ_COILS: lambda client, address, size, device_id, no_response_expected: client.read_coils(
                address, count=size, device_id=device_id, no_response_expected=no_response_expected
            ),
            ModbusRTUFunction.READ_DISCRETE_INPUTS: lambda client, address, size, device_id, no_response_expected: client.read_discrete_inputs(
                address, count=size, device_id=device_id, no_response_expected=no_response_expected
            ),
            ModbusRTUFunction.READ_HOLDING_REGISTERS: lambda client, address, size, device_id, no_response_expected: client.read_holding_registers(
                address, count=size, device_id=device_id, no_response_expected=no_response_expected
            ),
            ModbusRTUFunction.READ_INPUT_REGISTERS: lambda client, address, size, device_id, no_response_expected: client.read_input_registers(
                address, count=size, device_id=device_id, no_response_expected=no_response_expected
            ),
        }

        self.get_value_map: Dict[ModbusRTUNodeType, Callable[[ModbusRTUNodeOptions, ModbusPDU, int, int], float | int | bool]] = {
            ModbusRTUNodeType.BOOL: self.get_bool,
            ModbusRTUNodeType.INT_16: self.get_int,
            ModbusRTUNodeType.UINT_16: self.get_int,
            ModbusRTUNodeType.INT_32: self.get_int,
            ModbusRTUNodeType.UINT_32: self.get_int,
            ModbusRTUNodeType.INT_64: self.get_int,
            ModbusRTUNodeType.UINT_64: self.get_int,
            ModbusRTUNodeType.FLOAT_32: self.get_float,
            ModbusRTUNodeType.FLOAT_64: self.get_float,
        }

    async def start(self) -> None:
        """
        Starts the Modbus RTU energy meter background tasks for connection management and data polling.
        """

        if self.client is not None:
            raise RuntimeError(f"Modbus RTU Client for device {self.name} is already running")

        self.__renew_client()
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

    def __renew_client(self) -> None:
        """
        Re-initializes the Modbus RTU client with the current communication options.
        """

        self.client = ModbusRTUClient(
            port=self.communication_options.port,
            baudrate=self.communication_options.baudrate,
            stopbits=self.communication_options.stopbits,
            parity=self.communication_options.parity,
            bytesize=self.communication_options.bytesize,
            timeout=float(self.communication_options.timeout),
            retries=self.communication_options.retries,
        )

    async def manage_connection(self):
        """
        Manages the RTU connection lifecycle. Continuously tries to connect to the client,
        monitors its status, and handles reconnection if the link is lost.
        Raises:
            RuntimeError: If the Modbus RTU client is not initialized.
        """

        logger = LoggerManager.get_logger(__name__)

        while self.run_connection_task:
            if self.client is None:
                raise RuntimeError(f"Client {self.name} with id {self.id} is not initialized.")
            try:
                logger.info(f"Trying to connect to client {self.name} with id {self.id}...")
                self.network_connected = self.client.connect()

                if not self.network_connected:
                    logger.warning(f"Failed to connect to client {self.name} with id {self.id}")
                    await asyncio.sleep(3)
                    continue
                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.network_connected: # This loop will run forever because the driver connects to the virtual serial port, not the device itself
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

        while self.run_receiver_task:
            try:
                if self.network_connected and self.client:
                    enabled_nodes = [node for node in self.modbus_rtu_nodes if node.config.enabled]
                    batch_read_nodes = [node for node in enabled_nodes if node.enable_batch_read]
                    single_read_nodes = [node for node in enabled_nodes if not node.enable_batch_read]

                    await self.process_batch_read(self.client, batch_read_nodes, single_read_nodes)
                    await self.process_single_reads(self.client, single_read_nodes)

                    if not enabled_nodes or any(node.connected for node in enabled_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                await self.process_nodes()

            except ModbusException as e:
                logger.error(f"{e}")
                self.set_connection_state(False)

            except Exception as e:
                logger.exception(f"{e}")
                self.set_connection_state(False)

            await asyncio.sleep(self.communication_options.read_period)

    async def process_batch_read(
        self, client: ModbusRTUClient, batch_read_nodes: List[ModbusRTUNode], single_read_nodes: List[ModbusRTUNode]
    ) -> None:
        """
        Perform batch reads for eligible Modbus RTU nodes.

        Nodes are grouped by Modbus function and address range, then read using
        batch Modbus requests. If a batch read fails, all nodes in the affected
        batch group are scheduled for fallback single reads.
        """

        logger = LoggerManager.get_logger(__name__)

        if not batch_read_nodes:
            return

        batch_by_function: Dict[ModbusRTUFunction, List[ModbusRTUBatchGroup]] = {}
        batch_by_function[ModbusRTUFunction.READ_COILS] = self.create_batch_groups(
            [node for node in batch_read_nodes if node.options.function is ModbusRTUFunction.READ_COILS]
        )
        batch_by_function[ModbusRTUFunction.READ_DISCRETE_INPUTS] = self.create_batch_groups(
            [node for node in batch_read_nodes if node.options.function is ModbusRTUFunction.READ_DISCRETE_INPUTS]
        )
        batch_by_function[ModbusRTUFunction.READ_HOLDING_REGISTERS] = self.create_batch_groups(
            [node for node in batch_read_nodes if node.options.function is ModbusRTUFunction.READ_HOLDING_REGISTERS]
        )
        batch_by_function[ModbusRTUFunction.READ_INPUT_REGISTERS] = self.create_batch_groups(
            [node for node in batch_read_nodes if node.options.function is ModbusRTUFunction.READ_INPUT_REGISTERS]
        )

        for function, batch_groups in batch_by_function.items():
            for group in batch_groups:
                try:
                    results = await self.batch_read_nodes(client, function, group)

                    for node, value in results.items():
                        node.processor.set_value(value)

                except Exception as e:
                    single_read_nodes.extend(group.nodes)
                    logger.warning(
                        f"Batch read failed for {function.name} (addr={group.start_addr}, size={group.size}) on device {self.name}: {e}"
                    )

    async def process_single_reads(self, client: ModbusRTUClient, single_read_nodes: List[ModbusRTUNode]) -> None:
        """
        Perform individual Modbus reads for nodes not handled by batch reads.

        Executes per-node Modbus reads concurrently and updates node values.
        Failed reads are logged and result values are set to None.
        """

        logger = LoggerManager.get_logger(__name__)

        if not single_read_nodes:
            return

        tasks = [asyncio.to_thread(self.read_node, client, node) for node in single_read_nodes]
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

    def create_batch_groups(self, nodes: List[ModbusRTUNode]) -> List[ModbusRTUBatchGroup]:
        """
        Create Modbus RTU batch read groups from a list of nodes.

        Groups nodes into contiguous Modbus address ranges that can be read
        using a single Modbus request. Nodes are first sorted by address and
        then incrementally merged into batch groups while respecting the
        maximum allowed address gap and maximum batch span constraints.

        Each batch group contains the full node definitions whose data is
        covered by the computed address range, allowing values to be
        decoded directly from a shared Modbus response.

        This method operates at the protocol level and assumes that all
        provided nodes use the same Modbus function and belong to the same
        Modbus address space.

        Args:
            nodes (List[ModbusRTUNode]):
                List of Modbus RTU nodes eligible for batch reading.

        Returns:
            List[ModbusRTUBatchGroup]:
                List of batch groups defining contiguous address ranges and
                the nodes associated with each Modbus read operation.
        """

        if not nodes:
            return []

        nodes = sorted(nodes, key=lambda node: node.options.address)
        groups: List[ModbusRTUBatchGroup] = []

        start_addr = nodes[0].options.address
        end_addr = start_addr + MODBUS_RTU_TYPE_TO_SIZE_MAP[nodes[0].options.type]
        current_group: List[ModbusRTUNode] = [nodes[0]]

        for node in nodes[1:]:
            addr = node.options.address
            size = MODBUS_RTU_TYPE_TO_SIZE_MAP[node.options.type]

            gap = addr - end_addr
            new_end = addr + size
            new_span = new_end - start_addr

            if gap <= ModbusRTUEnergyMeter.MAX_ADDRESS_GAP and new_span <= ModbusRTUEnergyMeter.MAX_BATCH_SPAN:
                end_addr = max(end_addr, new_end)
                current_group.append(node)
            else:
                groups.append(ModbusRTUBatchGroup(start_addr=start_addr, size=end_addr - start_addr, nodes=current_group))
                start_addr = addr
                end_addr = addr + size
                current_group = [node]

        groups.append(ModbusRTUBatchGroup(start_addr=start_addr, size=end_addr - start_addr, nodes=current_group))
        return groups

    async def batch_read_nodes(
        self, client: ModbusRTUClient, function: ModbusRTUFunction, batch_group: ModbusRTUBatchGroup
    ) -> Dict[ModbusRTUNode, float | int | bool]:
        """
        Perform a batch Modbus RTU read for a group of nodes.

        Executes a single Modbus request covering the address range defined
        by the batch group and decodes each node value from the shared
        response payload.

        Args:
            client (ModbusRTUClient):
                Connected Modbus RTU client.

            function (ModbusRTUFunction):
                Modbus function code used for the batch read.

            batch_group (ModbusRTUBatchGroup):
                Batch group defining the read range and associated nodes.

        Returns:
            Dict[ModbusRTUNode, float | int | bool]:
                Mapping of nodes to their decoded values.

        Raises:
            ModbusException:
                If the batch read or value decoding fails.
        """

        if function not in self.modbus_function_map:
            raise ModbusException(f"Unknown modbus function {function} while trying to read batch group: {batch_group.nodes}.")

        response = await asyncio.to_thread(
            self.modbus_function_map[function], client, batch_group.start_addr, batch_group.size, self.communication_options.slave_id, False
        )
        results: Dict[ModbusRTUNode, float | int | bool] = {}

        for node in batch_group.nodes:
            try:
                index = node.options.address - batch_group.start_addr
                size = MODBUS_RTU_TYPE_TO_SIZE_MAP[node.options.type]
                value = self.get_value_map[node.options.type](node.options, response, index, size)
                results[node] = value
                node.set_connection_state(True)

            except Exception as e:
                raise ModbusException(f"Batch read failed for {self.name} on batch group {batch_group.nodes}: {e}")

        return results

    def read_node(self, client: ModbusRTUClient, node: ModbusRTUNode) -> float | int | bool:
        """
        Read and decode a single Modbus address for the given node.

        Performs a single Modbus read starting at the node’s configured address,
        reads the required number of data units based on the node type, and decodes
        the value from the response payload.

        Args:
            client (ModbusRTUClient):
                Connected Modbus RTU client.

            node (ModbusRTUNode):
                Node configuration defining the Modbus function, address, and type.

        Returns:
            float | int | bool:
                Decoded value read from the Modbus device.

        Raises:
            ModbusException:
                If the read operation or value decoding fails.
        """

        try:
            size = MODBUS_RTU_TYPE_TO_SIZE_MAP[node.options.type]
            if node.options.function not in self.modbus_function_map:
                raise ModbusException(f"Unknown modbus function {node.options.function} while trying to read node {node.config.name}.")

            response = self.modbus_function_map[node.options.function](
                client, node.options.address, size, self.communication_options.slave_id, False
            )
            value = self.get_value_map[node.options.type](node.options, response, 0, size)
            node.set_connection_state(True)
            return value

        except ModbusException as e:
            node.set_connection_state(False)
            raise ModbusException(f"Couldn't read node {node.config.name} from device {self.name} with id {self.id}") from e

        except Exception as e:
            node.set_connection_state(False)
            raise ModbusException(f"Unexpected error reading node {node.config.name} from device {self.name} with id {self.id}") from e

    def build_buffer(self, registers: List[int], endian_mode: ModbusRTUNodeMode, index: int, size: int) -> bytes:
        """
        Build a byte buffer from a slice of Modbus registers using the specified endian mode.

        Each Modbus register is treated as a big-endian word. Starting at ``index``, the
        method combines ``size`` consecutive registers and applies the configured endian
        mode (word order and/or byte order) to produce a byte buffer suitable for decoding
        multi-register values.

        Args:
            registers (List[int]):
                List of unsigned Modbus register values.

            endian_mode (ModbusRTUNodeMode):
                Endian mode defining how registers and bytes are reordered across
                multiple registers (e.g. big-endian, word swap, byte swap, or
                word+byte swap).

            index (int):
                Starting index within the register list.

            size (int):
                Number of consecutive registers to combine.

        Returns:
            bytes:
                Byte buffer representing the combined register values, ready for
                unpacking with ``struct.unpack``.

        Raises:
            ModbusException:
                If an unsupported endian mode is specified or the register slice is invalid.
        """

        words = [reg.to_bytes(2, "big") for reg in registers[index : index + size]]

        if endian_mode == ModbusRTUNodeMode.BIG_ENDIAN:
            ordered = words

        elif endian_mode == ModbusRTUNodeMode.WORD_SWAP:
            ordered = words[::-1]

        elif endian_mode == ModbusRTUNodeMode.BYTE_SWAP:
            ordered = [w[::-1] for w in words]

        elif endian_mode == ModbusRTUNodeMode.WORD_BYTE_SWAP:
            ordered = [w[::-1] for w in words[::-1]]

        else:
            raise ModbusException(f"Unsupported endian mode {endian_mode}")

        return b"".join(ordered)

    def get_float(self, options: ModbusRTUNodeOptions, value: ModbusPDU, index: int, size: int) -> float:
        """
        Extract a floating-point value from a Modbus register response.

        Supports decoding 32-bit and 64-bit floating-point values stored across
        multiple consecutive Modbus registers. The registers are first assembled
        according to the configured endian mode and then decoded using the
        appropriate floating-point format.

        Args:
            options (ModbusRTUNodeOptions):
                Node configuration defining the Modbus function, floating-point type,
                and endian mode.

            value (ModbusPDU):
                Modbus response containing register data.

            index (int):
                Starting register index within the response payload.

            size (int):
                Number of registers used to represent the floating-point value
                (2 for FLOAT32, 4 for FLOAT64).

        Returns:
            float:
                The decoded floating-point value.

        Raises:
            ModbusException:
                If the Modbus function is invalid, the register data is missing or
                out of bounds, the endian mode is undefined, or the size or type
                is unsupported.
        """

        if options.function in (ModbusRTUFunction.READ_HOLDING_REGISTERS, ModbusRTUFunction.READ_INPUT_REGISTERS):
            if not hasattr(value, "registers") or value.registers is None:
                raise ModbusException("Empty register response from Modbus device.")

            if index < 0 or (index + size) > len(value.registers):
                raise ModbusException(f"Register index {index} is out of bound for float value extraction with size {size}.")

            if options.endian_mode is None:
                raise ModbusException(f"Endian Mode needs to be defined for float value extraction.")

            buffer = self.build_buffer(value.registers, options.endian_mode, index, size)

            if size == 2:
                if options.type is ModbusRTUNodeType.FLOAT_32:
                    return float(struct.unpack(">f", buffer)[0])
                else:
                    raise ModbusException(f"Incorrect type {options.type} for float value extraction of size 2.")

            elif size == 4:
                if options.type is ModbusRTUNodeType.FLOAT_64:
                    return float(struct.unpack(">d", buffer)[0])
                else:
                    raise ModbusException(f"Incorrect type {options.type} for float value extraction of size 4.")

            else:
                raise ModbusException(f"Incompatible size {size} for float value extraction.")

        else:
            raise ModbusException(f"Invalid modbus function {options.function} while trying to extract float value.")

    def get_int(self, options: ModbusRTUNodeOptions, value: ModbusPDU, index: int, size: int) -> int:
        """
        Extract an integer value from a Modbus register response.

        Supports signed and unsigned integers encoded in one or more consecutive
        Modbus registers. Single-register values (INT16/UINT16) are decoded directly,
        while multi-register values (INT32/UINT32/INT64/UINT64) are assembled using
        the configured endian mode before decoding.

        Args:
            options (ModbusRTUNodeOptions):
                Node configuration defining the Modbus function, integer type,
                and endian mode (for multi-register values).

            value (ModbusPDU):
                Modbus response containing register data.

            index (int):
                Starting register index within the response payload.

            size (int):
                Number of registers used to represent the integer value
                (1, 2, or 4).

        Returns:
            int:
                The decoded integer value.

        Raises:
            ModbusException:
                If the Modbus function is invalid, the register data is missing or
                out of bounds, the node configuration is inconsistent, or the size
                or type is unsupported.
        """

        if options.function in (ModbusRTUFunction.READ_HOLDING_REGISTERS, ModbusRTUFunction.READ_INPUT_REGISTERS):
            if not hasattr(value, "registers") or value.registers is None:
                raise ModbusException("Empty register response from Modbus device.")

            if index < 0 or (index + size) > len(value.registers):
                raise ModbusException(f"Register index {index} is out of bound for int value extraction with size {size}.")

            if size == 1:
                if options.endian_mode is not None:
                    raise ModbusException(f"Endian Mode is not applicable for single register value extraction.")

                if options.type is ModbusRTUNodeType.INT_16:
                    return int(struct.unpack(">h", struct.pack(">H", value.registers[index]))[0])

                elif options.type is ModbusRTUNodeType.UINT_16:
                    return int(struct.unpack(">H", struct.pack(">H", value.registers[index]))[0])
                else:
                    raise ModbusException(f"Incorrect type {options.type} for int value extraction with single register.")

            else:
                if options.endian_mode is None:
                    raise ModbusException(f"Endian Mode needs to be defined for multiple registers value extraction.")

                buffer = self.build_buffer(value.registers, options.endian_mode, index, size)

                if size == 2:
                    if options.type is ModbusRTUNodeType.INT_32:
                        return int(struct.unpack(">i", buffer)[0])
                    elif options.type is ModbusRTUNodeType.UINT_32:
                        return int(struct.unpack(">I", buffer)[0])
                    else:
                        raise ModbusException(f"Incorrect type {options.type} for int value extraction of size 2.")

                elif size == 4:
                    if options.type is ModbusRTUNodeType.INT_64:
                        return int(struct.unpack(">q", buffer)[0])
                    elif options.type is ModbusRTUNodeType.UINT_64:
                        return int(struct.unpack(">Q", buffer)[0])
                    else:
                        raise ModbusException(f"Incorrect type {options.type} for int value extraction of size 4.")

                else:
                    raise ModbusException(f"Incompatible size {size} for int value extraction.")

        else:
            raise ModbusException(f"Invalid modbus function {options.function} while trying to extract int value.")

    def get_bool(self, options: ModbusRTUNodeOptions, value: ModbusPDU, index: int, size: int = 1) -> bool:
        """
        Extract a boolean value from a Modbus RTU response.

        Supports boolean reads from coils and discrete inputs, as well as bit
        extraction from holding or input registers. The ``index`` parameter
        selects the bit or register position in batch-read responses.

        Endianness is not applicable to boolean values.

        Raises ModbusException on invalid configuration, response payload,
        or out-of-range access.
        """

        if size != 1:
            raise ModbusException(f"Wrong size {size} for boolean value extraction.")

        if options.endian_mode is not None:
            raise ModbusException(f"Endian Mode is not applicable for boolean value extraction.")

        if options.function in (ModbusRTUFunction.READ_COILS, ModbusRTUFunction.READ_DISCRETE_INPUTS):

            if not hasattr(value, "bits") or value.bits is None:
                raise ModbusException("Empty bit response from Modbus device")

            if index < 0 or index >= len(value.bits):
                raise ModbusException(f"Index {index} is out of range for boolean value extraction.")

            return bool(value.bits[index])

        elif options.function in (ModbusRTUFunction.READ_HOLDING_REGISTERS, ModbusRTUFunction.READ_INPUT_REGISTERS):

            if options.bit is None or options.bit < 0 or options.bit > 15:
                raise ModbusException(f"The modbus register bit needs to be an integer between 0 and 15.")

            if not hasattr(value, "registers") or value.registers is None:
                raise ModbusException("Empty register response from Modbus device")

            if index < 0 or index >= len(value.registers):
                raise ModbusException(f"Couldn't find the register with index {index} for boolean value extraction.")

            return bool((value.registers[index] >> options.bit) & 1)

        else:
            raise ModbusException(f"Unknown modbus function {options.function} while trying to extract boolean value.")

    async def close_connection(self) -> None:
        """
        Closes the Modbus RTU client connection and updates connection state.
        """

        if self.client is not None:
            self.client.close()
        self.set_network_state(False)
        self.client = None

    def get_extended_info(self, get_history_method: Callable[[int], DeviceHistoryStatus], additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Extends the base device information with Modbus RTU–specific data.

        Adds communication parameters and connection statistics, then delegates
        to the parent implementation.

        Returns:
            Dict[str, Any]:
                Base extended device info plus:
                    - read_period: Modbus RTU polling period
        """

        output: Dict[str, Any] = additional_data.copy()
        output["read_period"] = self.communication_options.read_period
        return super().get_extended_info(get_history_method, additional_data=output)