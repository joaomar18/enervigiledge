###########EXTERNAL IMPORTS############

from typing import Dict, List, Any
from types import NoneType

#######################################

#############LOCAL IMPORTS#############

from model.controller.protocol.opc_ua import OPCUAOptions, OPCUANodeOptions, OPCUANodeType
import web.parsers.helpers as parse_helper
import web.exceptions as api_exception

#######################################


def parse_opc_ua_meter_comm_options(dict_communication_options: Dict[str, Any]) -> OPCUAOptions:
    """
    Parse and validate OPC UA communication options from an API payload.

    Extracts required OPC UA communication fields, performs basic type
    coercion, and constructs a fully typed OPCUAOptions instance. Optional
    authentication fields are supported. Any missing or malformed fields
    result in an API-level validation error.

    Args:
        dict_communication_options: Raw OPC UA communication options dictionary
            provided by the API request.

    Returns:
        OPCUAOptions: Parsed and type-safe OPC UA communication options.

    Raises:
        InvalidRequestPayload: If required fields are missing or cannot be
            parsed from the request.
        ValueError: If parsed values violate expected internal types,
            indicating an unexpected internal state.
    """

    missing: List[str] = []

    # Parse URL
    url = parse_helper.parse_str_field_from_dict(dict_communication_options, "url", missing)

    # Parse Read Period
    read_period = parse_helper.parse_int_field_from_dict(dict_communication_options, "read_period", missing)

    # Parse Timeout
    timeout = parse_helper.parse_int_field_from_dict(dict_communication_options, "timeout", missing)

    # Parse Username
    username = parse_helper.parse_str_field_from_dict(dict_communication_options, "username", missing, True)

    # Parse Password
    password = parse_helper.parse_str_field_from_dict(dict_communication_options, "password", missing, True)

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.DEVICE.MISSING_DEVICE_COMUNICATION_FIELDS, None, details={"missing_fields": missing}
        )

    if (
        not isinstance(url, str)
        or not isinstance(read_period, int)
        or not isinstance(timeout, int)
        or not isinstance(username, (int, NoneType))
        or not isinstance(password, (str, NoneType))
    ):
        raise ValueError(f"Invalid types in OPC UA communication options.")

    return OPCUAOptions(url=url, read_period=read_period, timeout=timeout, username=username, password=password)


def parse_opc_ua_node_protocol_options(dict_protocol_options: Dict[str, Any]) -> OPCUANodeOptions:
    """
    Parse and validate OPC UA node protocol options from an API payload.

    Extracts the OPC UA NodeId and expected data type from the input dictionary,
    converts string values into their corresponding OPC UA enums, and returns
    a fully constructed OPCUANodeOptions instance.

    Args:
        dict_protocol_options: Raw OPC UA node protocol options dictionary
            provided by the API request.

    Returns:
        OPCUANodeOptions: Parsed and type-safe OPC UA node protocol options.

    Raises:
        InvalidRequestPayload: If required fields are missing or cannot be
            parsed from the request.
        ValueError: If parsed values violate expected internal types,
            indicating an unexpected internal state.
    """

    missing: List[str] = []

    # Parse Node ID
    node_id = parse_helper.parse_str_field_from_dict(dict_protocol_options, "node_id", missing)

    # Parse Type
    type = parse_helper.parse_str_field_from_dict(dict_protocol_options, "type", missing)
    if type is not None:
        try:
            type = OPCUANodeType(type)
        except Exception as e:
            type = None
            missing.append("type")

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS_FIELDS, None, details={"missing_fields": missing}
        )

    if not isinstance(node_id, str) or not isinstance(type, OPCUANodeType):
        raise ValueError(f"Invalid types in OPC UA Node Protocol options.")

    return OPCUANodeOptions(node_id=node_id, type=type)
