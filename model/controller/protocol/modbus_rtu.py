###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import BaseNodeProtocolOptions, NodeType
from model.controller.device import BaseCommunicationOptions

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