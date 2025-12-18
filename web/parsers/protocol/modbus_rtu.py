###########EXTERNAL IMPORTS############

from typing import Dict, List, Any
from types import NoneType

#######################################

#############LOCAL IMPORTS#############

from model.controller.protocol.modbus_rtu import (
    ModbusRTUOptions,
    ModbusRTUNodeOptions,
    ModbusRTUFunction,
    ModbusRTUNodeType,
    ModbusRTUNodeMode,
)
import web.parsers.helpers as parse_helper
import web.exceptions as api_exception

#######################################


def parse_modbus_rtu_meter_comm_options(dict_communication_options: Dict[str, Any]) -> ModbusRTUOptions:
    """
    Parse and validate Modbus RTU communication options from an API payload.

    Extracts required communication fields from the input dictionary, performs
    type coercion and validation, and constructs a ModbusRTUOptions domain
    object. Missing or invalid fields result in an API-level validation error.

    Args:
        dict_communication_options: Raw communication options dictionary
            provided by the API request.

    Returns:
        ModbusRTUOptions: Fully constructed and type-safe Modbus RTU
        communication options.

    Raises:
        InvalidRequestPayload: If one or more required fields are missing or
            cannot be parsed.
        ValueError: If parsed values do not conform to the expected types
            (indicating an unexpected internal state).
    """

    missing: List[str] = []

    # Parse Slave ID
    slave_id = parse_helper.parse_int_field_from_dict(dict_communication_options, "slave_id", missing)

    # Parse Port
    port = parse_helper.parse_str_field_from_dict(dict_communication_options, "port", missing)

    # Parse Baudrate
    baudrate = parse_helper.parse_int_field_from_dict(dict_communication_options, "baudrate", missing)

    # Parse Stop bits
    stopbits = parse_helper.parse_int_field_from_dict(dict_communication_options, "stopbits", missing)

    # Parse Parity
    parity = parse_helper.parse_str_field_from_dict(dict_communication_options, "parity", missing)

    # Parse Byte Size
    bytesize = parse_helper.parse_int_field_from_dict(dict_communication_options, "bytesize", missing)

    # Parse Read Period
    read_period = parse_helper.parse_int_field_from_dict(dict_communication_options, "read_period", missing)

    # Parse Timeout
    timeout = parse_helper.parse_int_field_from_dict(dict_communication_options, "timeout", missing)

    # Parse Retries
    retries = parse_helper.parse_int_field_from_dict(dict_communication_options, "retries", missing)

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.DEVICE.MISSING_DEVICE_COMUNICATION_FIELDS, None, details={"missing_fields": missing}
        )

    if (
        not isinstance(slave_id, int)
        or not isinstance(port, str)
        or not isinstance(baudrate, int)
        or not isinstance(stopbits, int)
        or not isinstance(parity, str)
        or not isinstance(bytesize, int)
        or not isinstance(read_period, int)
        or not isinstance(timeout, int)
        or not isinstance(retries, int)
    ):
        raise ValueError(f"Invalid types in Modbus RTU communication options.")

    return ModbusRTUOptions(
        slave_id=slave_id,
        port=port,
        baudrate=baudrate,
        stopbits=stopbits,
        parity=parity,
        bytesize=bytesize,
        read_period=read_period,
        timeout=timeout,
        retries=retries,
    )


def parse_modbus_rtu_node_protocol_options(dict_protocol_options: Dict[str, Any]) -> ModbusRTUNodeOptions:
    """
    Parse and validate Modbus RTU node protocol options from an API payload.

    Extracts and converts protocol-specific node options, including function
    code, address, data type, and optional endian mode or bit index. String
    values are normalized into the corresponding Modbus RTU enums and a
    fully constructed ModbusRTUNodeOptions instance is returned.

    Args:
        dict_protocol_options: Raw node protocol options dictionary provided
            by the API request.

    Returns:
        ModbusRTUNodeOptions: Parsed and type-safe Modbus RTU node protocol
        options.

    Raises:
        InvalidRequestPayload: If required fields are missing or cannot be
            parsed.
        ValueError: If parsed values do not conform to the expected types,
            indicating an unexpected internal state.
    """

    missing: List[str] = []

    # Parse Function
    function = parse_helper.parse_str_field_from_dict(dict_protocol_options, "function", missing)
    if function is not None:
        try:
            function = ModbusRTUFunction(function)
        except Exception as e:
            function = None
            missing.append("function")

    # Parse Address
    address = parse_helper.parse_int_field_from_dict(dict_protocol_options, "address", missing)

    # Parse Type
    type = parse_helper.parse_str_field_from_dict(dict_protocol_options, "type", missing)
    if type is not None:
        try:
            type = ModbusRTUNodeType(type)
        except Exception as e:
            type = None
            missing.append("type")

    # Parse Endian Mode
    endian_mode = parse_helper.parse_str_field_from_dict(dict_protocol_options, "endian_mode", missing, True)
    if endian_mode is not None:
        try:
            endian_mode = ModbusRTUNodeMode(endian_mode)
        except Exception as e:
            endian_mode = None
            missing.append("endian_mode")

    # Parse Bit
    bit = parse_helper.parse_int_field_from_dict(dict_protocol_options, "bit", missing, True)

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS_FIELDS, None, details={"missing_fields": missing}
        )

    if (
        not isinstance(function, ModbusRTUFunction)
        or not isinstance(address, int)
        or not isinstance(type, ModbusRTUNodeType)
        or not isinstance(endian_mode, (ModbusRTUNodeMode, NoneType))
        or not isinstance(bit, (int, NoneType))
    ):
        raise ValueError(f"Invalid types in Modbus RTU Node Protocol options.")

    return ModbusRTUNodeOptions(function=function, address=address, type=type, endian_mode=endian_mode, bit=bit)
