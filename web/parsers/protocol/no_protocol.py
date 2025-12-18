###########EXTERNAL IMPORTS############

from typing import Dict, List, Any
from types import NoneType

#######################################

#############LOCAL IMPORTS#############

from model.controller.protocol.no_protocol import NoProtocolNodeOptions, NoProtocolType
import web.parsers.helpers as parse_helper
import web.exceptions as api_exception

#######################################


def parse_no_protocol_node_protocol_options(dict_protocol_options: Dict[str, Any]) -> NoProtocolNodeOptions:
    """
    Parse and validate no-protocol node options from an API payload.

    Extracts the node type from the input dictionary, converts the string value
    into the corresponding NoProtocolType enum, and returns a fully constructed
    NoProtocolNodeOptions instance.

    Args:
        dict_protocol_options: Raw node protocol options dictionary provided
            by the API request.

    Returns:
        NoProtocolNodeOptions: Parsed and type-safe no-protocol node options.

    Raises:
        InvalidRequestPayload: If required fields are missing or cannot be
            parsed from the request.
        ValueError: If parsed values violate expected internal types,
            indicating an unexpected internal state.
    """

    missing: List[str] = []

    # Parse Type
    type = parse_helper.parse_str_field_from_dict(dict_protocol_options, "type", missing)
    if type is not None:
        try:
            type = NoProtocolType(type)
        except Exception as e:
            type = None
            missing.append("type")

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS_FIELDS, None, details={"missing_fields": missing}
        )

    if not isinstance(type, NoProtocolType):
        raise ValueError(f"Invalid types in No Protocol Node Protocol options.")

    return NoProtocolNodeOptions(type=type)
