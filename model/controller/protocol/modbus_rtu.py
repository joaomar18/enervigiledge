###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Any, TYPE_CHECKING

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import BaseNodeProtocolOptions, NodeType
from model.controller.device import BaseCommunicationOptions

if TYPE_CHECKING:
    from controller.node.node import ModbusRTUNode

#######################################


class ModbusRTUNodeType(str, Enum):
    """
    Enumeration of supported Modbus RTU numeric data types.

    Attributes:
        BOOL (str): Boolean value (coil or discrete input).
        INT_16 (str): Signed 16-bit integer (1 register).
        UINT_16 (str): Unsigned 16-bit integer (1 register).
        INT_32 (str): Signed 32-bit integer (2 registers).
        UINT_32 (str): Unsigned 32-bit integer (2 registers).
        FLOAT_32 (str): 32-bit IEEE-754 float (2 registers).
        INT_64 (str): Signed 64-bit integer (4 registers).
        UINT_64 (str): Unsigned 64-bit integer (4 registers).
        FLOAT_64 (str): 64-bit IEEE-754 float (4 registers).
    """

    BOOL = "BOOL"
    INT_16 = "INT_16"
    UINT_16 = "UINT_16"
    INT_32 = "INT_32"
    UINT_32 = "UINT_32"
    FLOAT_32 = "FLOAT_32"
    INT_64 = "INT_64"
    UINT_64 = "UINT_64"
    FLOAT_64 = "FLOAT_64"


"""Mapping from ModbusRTUNodeType enum values to their corresponding internal type NodeType."""
MODBUS_RTU_TO_INTERNAL_TYPE_MAP = {
    ModbusRTUNodeType.BOOL: NodeType.BOOL,
    ModbusRTUNodeType.INT_16: NodeType.INT,
    ModbusRTUNodeType.UINT_16: NodeType.INT,
    ModbusRTUNodeType.INT_32: NodeType.INT,
    ModbusRTUNodeType.UINT_32: NodeType.INT,
    ModbusRTUNodeType.FLOAT_32: NodeType.FLOAT,
    ModbusRTUNodeType.INT_64: NodeType.INT,
    ModbusRTUNodeType.UINT_64: NodeType.INT,
    ModbusRTUNodeType.FLOAT_64: NodeType.FLOAT,
}

"""
Mapping from ModbusRTUNodeType enum values to the number of Modbus data units
required to represent their raw value. For register-based types, the value
indicates the number of 16-bit registers; boolean types represent either a
single coil/discrete input or a single bit within a register.
"""
MODBUS_RTU_TYPE_TO_SIZE_MAP = {
    ModbusRTUNodeType.BOOL: 1,
    ModbusRTUNodeType.INT_16: 1,
    ModbusRTUNodeType.UINT_16: 1,
    ModbusRTUNodeType.INT_32: 2,
    ModbusRTUNodeType.UINT_32: 2,
    ModbusRTUNodeType.FLOAT_32: 2,
    ModbusRTUNodeType.INT_64: 4,
    ModbusRTUNodeType.UINT_64: 4,
    ModbusRTUNodeType.FLOAT_64: 4,
}


class ModbusRTUNodeMode(str, Enum):
    """
    Enumeration of endianness modes for multi-register Modbus values.

    Attributes:
        BIG_ENDIAN (str): Standard order (A1 A2 B1 B2).
        WORD_SWAP (str): Swap 16-bit words (B1 B2 A1 A2).
        BYTE_SWAP (str): Swap bytes within each word (A2 A1 B2 B1).
        WORD_BYTE_SWAP (str): Swap both words and bytes (B2 B1 A2 A1).
    """

    BIG_ENDIAN = "BIG_ENDIAN"
    WORD_SWAP = "WORD_SWAP"
    BYTE_SWAP = "BYTE_SWAP"
    WORD_BYTE_SWAP = "WORD_BYTE_SWAP"


class ModbusRTUFunction(str, Enum):
    """
    Modbus RTU function codes supported for reading node values.

    Represents the four standard Modbus memory areas, each accessed through
    its corresponding read function.

    Attributes:
        READ_COILS: Read writable boolean coils (FC1).
        READ_DISCRETE_INPUTS: Read read-only boolean inputs (FC2).
        READ_HOLDING_REGISTERS: Read 16-bit holding registers (FC3).
        READ_INPUT_REGISTERS: Read 16-bit input registers (FC4).
    """

    READ_COILS = "READ_COILS"
    READ_DISCRETE_INPUTS = "READ_DISCRETE_INPUTS"
    READ_HOLDING_REGISTERS = "READ_HOLDING_REGISTERS"
    READ_INPUT_REGISTERS = "READ_INPUT_REGISTERS"


@dataclass
class ModbusRTUBatchGroup:
    """
    Defines a Modbus RTU batch read group.

    Represents a contiguous range of Modbus addresses that can be read
    in a single Modbus request, along with the node option definitions
    that map to values within that address range.

    Attributes:
        start_addr (int): Starting Modbus address of the batch read.
        size (int): Number of consecutive Modbus registers or data units to read.
        nodes (List[ModbusRTUNode]): List of Modbus RTU nodes whose values are contained within this batch read range.
    """

    start_addr: int
    size: int
    nodes: List["ModbusRTUNode"]


@dataclass
class ModbusRTUNodeOptions(BaseNodeProtocolOptions):
    """
    Protocol-specific configuration for a Modbus RTU node.

    Defines how a node value is read from a Modbus device, including the
    function code, address, data type, and optional bit extraction or
    endianness rules for multi-register values.

    Attributes:
        function (ModbusRTUFunction): Modbus read function to use
            (coils, discrete inputs, holding registers, input registers).
        address (int): Address used with the selected function (coil or register index).
        type (ModbusRTUNodeType): Modbus data type (e.g., INT_16, FLOAT_32, BOOL).
        endian_mode (ModbusRTUNodeMode | None): Byte/word ordering for multi-register
            numeric types. None for single-register or coil values.
        bit (int | None): Optional bit index for boolean flags stored within registers.
    """

    function: ModbusRTUFunction
    address: int
    type: ModbusRTUNodeType
    endian_mode: ModbusRTUNodeMode | None = None
    bit: int | None = None

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "ModbusRTUNodeOptions":
        """
        Construct a ModbusRTUNodeOptions instance from a persisted options dictionary.

        Converts stored primitive values (e.g. strings, integers) into their
        corresponding domain enums and types. Assumes the input dictionary has
        already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid Modbus RTU
            node options (e.g. due to invalid enum values or corrupted data).
        """

        try:
            function = ModbusRTUFunction(options_dict["function"])
            address = int(options_dict["address"])
            type = ModbusRTUNodeType(options_dict["type"])
            endian_mode = ModbusRTUNodeMode(options_dict["endian_mode"]) if options_dict["endian_mode"] is not None else None
            bit = int(options_dict["bit"]) if options_dict["bit"] is not None else None
            return ModbusRTUNodeOptions(function, address, type, endian_mode, bit)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into Modbus RTU Node Options: {e}.")


@dataclass
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
        read_period (int): Interval in seconds between read operations. Defaults to 5.
        timeout (int): Timeout in seconds for communication attempts. Defaults to 5.
    """

    slave_id: int
    port: str
    baudrate: int
    stopbits: int
    parity: str
    bytesize: int
    read_period: int = 5
    timeout: int = 5
    retries: int = 3

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "ModbusRTUOptions":
        """
        Construct ModbusRTUOptions from a persisted communication options dictionary.

        Converts stored primitive values into strongly typed Modbus RTU
        communication options. Assumes the input dictionary has already been
        validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid Modbus RTU
            communication options (e.g. due to corrupted or incompatible data).
        """

        try:
            slave_id = int(options_dict["slave_id"])
            port = str(options_dict["port"])
            baudrate = int(options_dict["baudrate"])
            stopbits = int(options_dict["stopbits"])
            parity = str(options_dict["parity"])
            bytesize = int(options_dict["bytesize"])
            read_period = int(options_dict["read_period"])
            timeout = int(options_dict["timeout"])
            retries = int(options_dict["retries"])
            return ModbusRTUOptions(slave_id, port, baudrate, stopbits, parity, bytesize, read_period, timeout, retries)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into Modbus RTU Device Options: {e}.")
