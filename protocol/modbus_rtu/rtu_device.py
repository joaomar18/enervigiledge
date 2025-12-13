###########EXTERNAL IMPORTS############

import asyncio
import struct
from pymodbus.pdu import ModbusPDU
from pymodbus.client import ModbusSerialClient as ModbusRTUClient
from pymodbus import ModbusException
from typing import Optional, Set, Dict, List, Callable, Any
import logging
from typing import Set, Optional

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node, ModbusRTUNode
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions
from model.controller.protocol.modbus_rtu import ModbusRTUOptions, ModbusRTUNodeOptions, ModbusRTUNodeType, ModbusRTUFunction, ModbusRTUNodeMode, MODBUS_RTU_TYPE_TO_SIZE_MAP 
from controller.meter.meter import EnergyMeter

#######################################


LoggerManager.get_logger(__name__).setLevel(logging.ERROR)
ModbusCall = Callable[[ModbusRTUClient, int, int, int, bool], ModbusPDU]


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
        
        self.modbus_function_map: Dict[ModbusRTUFunction, ModbusCall] = {
            ModbusRTUFunction.READ_COILS:
                lambda client, address, size, device_id, no_response_expected:
                    client.read_coils(address, count=size, device_id=device_id, no_response_expected=no_response_expected),

            ModbusRTUFunction.READ_DISCRETE_INPUTS:
                lambda client, address, size, device_id, no_response_expected:
                    client.read_discrete_inputs(address, count=size, device_id=device_id, no_response_expected=no_response_expected),

            ModbusRTUFunction.READ_HOLDING_REGISTERS:
                lambda client, address, size, device_id, no_response_expected:
                    client.read_holding_registers(address, count=size, device_id=device_id, no_response_expected=no_response_expected),

            ModbusRTUFunction.READ_INPUT_REGISTERS:
                lambda client, address, size, device_id, no_response_expected:
                    client.read_input_registers(address, count=size, device_id=device_id, no_response_expected=no_response_expected),
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
                    enabled_nodes = [node for node in self.modbus_rtu_nodes if node.config.enabled]
                    batch_read_nodes = [node for node in enabled_nodes if node.enable_batch_read]
                    single_read_nodes = [node for node in enabled_nodes if not node.enable_batch_read]
                    
                    tasks = [asyncio.to_thread(self.read_single_address, client, node) for node in enabled_nodes]
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

                    if not enabled_nodes or any(node.connected for node in enabled_nodes):
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

    def read_single_address(self, client: ModbusRTUClient, node: ModbusRTUNode) -> float | int | bool:
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
            
            response = self.modbus_function_map[node.options.function](client, node.options.address, size, self.communication_options.slave_id, False)
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
        
        words = [reg.to_bytes(2, "big") for reg in registers[index:index+size]]

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
