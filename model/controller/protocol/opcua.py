###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import Optional

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import BaseNodeProtocolOptions, NodeType
from model.controller.device import BaseCommunicationOptions

#######################################


class OPCUANodeType(str, Enum):
    """
    Enumeration of supported OPC UA data types.

    Attributes:
        BOOL (str): Boolean value.
        INT (str): Integer value.
        FLOAT (str): Floating-point value.
        STRING (str): String value.
    """
    
    BOOL = "BOOL"
    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING"
    
    
"""Mapping from OPCUANodeType enum values to their corresponding internal type NodeType."""
OPCUA_TO_INTERAL_TYPE_MAP = {
    OPCUANodeType.BOOL: NodeType.BOOL,
    OPCUANodeType.INT: NodeType.INT,
    OPCUANodeType.FLOAT: NodeType.FLOAT,
    OPCUANodeType.STRING: NodeType.STRING,
}
    

@dataclass
class OPCUANodeOptions(BaseNodeProtocolOptions):
    """
    Protocol-specific configuration for an OPC UA node.

    Defines the OPC UA identifier and expected data type used when reading
    values from an OPC UA server.

    Attributes:
        node_id (str): The OPC UA NodeId string (e.g., "ns=2;s=EnergyMeter/VoltageL1").
        type (OPCUANodeType): Expected data type of the node (BOOL, INT, FLOAT, STRING).
    """
    
    node_id: str
    type: OPCUANodeType
    
@dataclass(kw_only=True)
class OPCUAOptions(BaseCommunicationOptions):
    """
    Configuration options for OPC UA communication.

    Attributes:
        url (str): Endpoint URL of the OPC UA server.
        username (Optional[str]): Username for authentication. Defaults to None.
        password (Optional[str]): Password for authentication. Defaults to None.
    """

    url: str
    username: Optional[str] = None
    password: Optional[str] = None