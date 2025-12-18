###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional

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
OPCUA_TO_INTERNAL_TYPE_MAP = {
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

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "OPCUANodeOptions":
        """
        Construct OPCUANodeOptions from a persisted options dictionary.

        Converts stored primitive values into OPC UA–specific domain types.
        Assumes the input dictionary has already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid OPC UA
            node options (e.g. due to corrupted or incompatible data).
        """

        try:
            node_id = str(options_dict["node_id"])
            type = OPCUANodeType(options_dict["type"])
            return OPCUANodeOptions(node_id, type)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into OPC UA Node Options: {e}.")


@dataclass
class OPCUAOptions(BaseCommunicationOptions):
    """
    Configuration options for OPC UA communication.

    Attributes:
        url (str): Endpoint URL of the OPC UA server.
        read_period (int): Interval in seconds between read operations. Defaults to 5.
        timeout (int): Timeout in seconds for communication attempts. Defaults to 5.
        username (Optional[str]): Username for authentication. Defaults to None.
        password (Optional[str]): Password for authentication. Defaults to None.
    """

    url: str
    read_period: int = 5
    timeout: int = 5
    username: Optional[str] = None
    password: Optional[str] = None

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "OPCUAOptions":
        """
        Construct OPCUAOptions from a persisted communication options dictionary.

        Converts stored primitive values into strongly typed OPC UA communication
        options. Assumes the input dictionary has already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid OPC UA
            communication options (e.g. due to corrupted or incompatible data).
        """

        try:
            url = str(options_dict["url"])
            read_period = int(options_dict["read_period"])
            timeout = int(options_dict["timeout"])
            username = str(options_dict["username"]) if options_dict["username"] is not None else None
            password = str(options_dict["password"]) if options_dict["password"] is not None else None
            return OPCUAOptions(url, read_period, timeout, username, password)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into OPC UA Device Options: {e}.")
