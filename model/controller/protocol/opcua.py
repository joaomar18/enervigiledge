###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import Optional

#######################################

#############LOCAL IMPORTS#############

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
    

@dataclass
class OPCUANodeOptions():
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