###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import BaseNodeProtocolOptions, NodeType

#######################################


class NoProtocolType(str, Enum):
    """
    Supported data types for nodes without an external protocol
    (e.g., calculated or internal nodes).

    Attributes:
        BOOL (str): Boolean value.
        INT (str): Integer value.
        FLOAT (str): Floating-point value.
        STRING (str): Text value.
    """

    BOOL = "BOOL"
    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING"


"""Mapping from NoProtocolType to the internal unified NodeType."""
NONE_TO_INTERNAL_TYPE_MAP = {
    NoProtocolType.BOOL: NodeType.BOOL,
    NoProtocolType.INT: NodeType.INT,
    NoProtocolType.FLOAT: NodeType.FLOAT,
    NoProtocolType.STRING: NodeType.STRING,
}


@dataclass
class NoProtocolNodeOptions(BaseNodeProtocolOptions):
    """
    Options for nodes whose values are computed internally rather than read
    from a protocol.

    Attributes:
        type (NoProtocolType): Output type of the calculated value.
    """

    type: NoProtocolType

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "NoProtocolNodeOptions":
        """
        Construct NoProtocolNodeOptions from a persisted options dictionary.

        Converts stored primitive values into no-protocol domain options.
        Assumes the input dictionary has already been validated.

        Raises:
            ValueError: If the dictionary cannot be cast into valid no-protocol
            node options (e.g. due to corrupted or incompatible data).
        """

        try:
            type = NoProtocolType(options_dict["type"])
            return NoProtocolNodeOptions(type)

        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into No Protocol Node Options: {e}.")
