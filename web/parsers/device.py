###########EXTERNAL IMPORTS############

import json
from typing import Dict, Tuple, List, Any, Optional
from fastapi import Request
from starlette.datastructures import UploadFile, QueryParams

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions, EnergyMeterRecord
from web.parsers.nodes import parse_nodes
import web.exceptions as api_exception

#######################################


async def parse_device_request(request: Request) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[UploadFile]]:
    """
    Parses and validates a device creation or update request.

    This helper supports both JSON requests and multipart form-data requests.
    It extracts device configuration data, node definitions, and an optional
    uploaded device image, performing structural and format validation.

    Args:
        request (Request): Incoming HTTP request containing either JSON or
            multipart form-data.

    Returns:
        Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[UploadFile]]:
            - device_data: Parsed device configuration data.
            - device_nodes: Parsed list of device node definitions.
            - device_image: Uploaded device image if provided, otherwise None.

    Raises:
        InvalidRequestPayload: If the request body is malformed, required
            fields are missing, or JSON/form data is invalid.
    """

    content_type = request.headers.get("content-type", "")

    if content_type.startswith("multipart/form-data"):
        try:
            form = await request.form()
        except Exception as e:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_FORM_DATA)

        device_data_json = form.get("device_data")
        if device_data_json is None or not isinstance(device_data_json, str):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_DATA)

        device_nodes_json = form.get("device_nodes")
        if device_data_json is None or not isinstance(device_nodes_json, str):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_NODES_DATA)

        device_image = form.get("device_image")
        if device_image is None or not isinstance(device_image, UploadFile):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_UPLOADED_IMAGE)

        try:
            device_data: Dict[str, Any] = json.loads(device_data_json)
        except Exception as e:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_DEVICE_DATA_JSON)
        try:
            device_nodes: List[Dict[str, Any]] = json.loads(device_nodes_json)
        except Exception as e:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_DEVICE_NODES_JSON)

    else:
        try:
            payload: Dict[str, Any] = await request.json()  # request payload
        except Exception as e:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

        device_data_req: Optional[Dict[str, Any]] = payload.get("device_data")
        if device_data_req is None or not isinstance(device_data_req, dict):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_DATA)

        device_nodes_req: Optional[List[Dict[str, Any]]] = payload.get("device_nodes")
        if device_nodes_req is None or not isinstance(device_nodes_req, list):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_NODES_DATA)

        device_data: Dict[str, Any] = device_data_req
        device_nodes: List[Dict[str, Any]] = device_nodes_req
        device_image = None

    return device_data, device_nodes, device_image


def parse_device_id(request_dict: Dict[str, Any] | QueryParams) -> int:
    """
    Parse and validate a device ID from request data.

    Ensures the ``id`` field exists, is of a valid type, and can be converted
    to an integer. Invalid or missing values raise an API-level error.

    Args:
        request_dict: Request data containing the device ID.

    Returns:
        int: Validated device identifier.

    Raises:
        InvalidRequestPayload: If the device ID is missing or invalid.
    """

    device_id = request_dict.get("id")
    if device_id is None or not isinstance(device_id, (int, str)):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_ID)

    try:
        device_id = int(device_id)
    except Exception:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_DEVICE_ID)

    return device_id


def parse_device_options(dict_meter_options: Dict[str, Any]) -> EnergyMeterOptions:
    """
    Parse meter-level configuration options from a raw dictionary.

    Converts the provided dictionary into an EnergyMeterOptions instance
    using explicit casting logic. Any failure during casting is reported
    as an invalid request payload.

    Args:
        dict_meter_options: Raw meter options dictionary, typically provided
            by an external source such as an API request or database record.

    Returns:
        EnergyMeterOptions: Parsed meter options instance.

    Raises:
        InvalidRequestPayload: If the options dictionary cannot be cast into
            a valid EnergyMeterOptions object.
    """

    missing_fields: List[str] = []
    try:
        return EnergyMeterOptions()

    except Exception as e:
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.DEVICE.MISSING_DEVICE_OPTIONS_FIELDS, details={"fields": missing_fields}
        )


def parse_device(new_device: bool, dict_device: Dict[str, Any], dict_nodes: List[Dict[str, Any]]) -> EnergyMeterRecord:
    """
    Parse and validate an energy meter device definition.

    Validates the top-level device configuration, parses protocol, meter type,
    device options, communication options, and associated node definitions,
    and returns a constructed EnergyMeterRecord instance. Validation failures
    are translated into API-level errors.

    Args:
        new_device: Indicates whether the device is being created (True) or
            updated (False). Controls whether an ID is required.
        dict_device: Raw device configuration data from an external source.
        dict_nodes: List of raw node definition dictionaries associated with
            the device.

    Returns:
        EnergyMeterRecord: Parsed and validated energy meter record.

    Raises:
        InvalidRequestPayload:
            - If required device fields are missing.
            - If protocol or meter type is missing or invalid.
            - If device options, communication options, or node definitions
              are missing or invalid.
    """

    # Device ID Parsing
    if not new_device:
        device_id = parse_device_id(dict_device)

    else:
        device_id = None

    # Name Parsing
    device_name = dict_device.get("name")
    if device_name is None or not isinstance(device_name, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_NAME)

    # Protocol Parsing
    protocol = dict_device.get("protocol")
    if protocol is None or not isinstance(protocol, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_PROTOCOL)
    try:
        protocol = Protocol(protocol)
        parser_comm_method = ProtocolRegistry.get_protocol_plugin(protocol).meter_comm_options_parser_method
        if parser_comm_method is None:
            raise RuntimeError(f"No communication options parser registered for protocol {protocol}.")

    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_PROTOCOL)

    # Type Parsing
    device_type: str = dict_device["type"]
    if device_type is None or not isinstance(device_type, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_TYPE)
    try:
        device_type = EnergyMeterType(device_type)
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_TYPE)

    # Device Options
    device_options_dict_req = dict_device.get("options")
    if device_options_dict_req is None or not isinstance(device_options_dict_req, dict):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_OPTIONS)
    device_options_dict: Dict[str, Any] = device_options_dict_req
    device_options = parse_device_options(device_options_dict)

    # Communication Options
    comm_options_dict_req = dict_device.get("communication_options")
    if comm_options_dict_req is None or not isinstance(comm_options_dict_req, dict):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_COMUNICATION)
    comm_options_dict: Dict[str, Any] = comm_options_dict_req
    comm_options = parser_comm_method(comm_options_dict)

    return EnergyMeterRecord(
        name=device_name,
        id=device_id,
        protocol=protocol,
        type=device_type,
        options=device_options,
        communication_options=comm_options,
        nodes=parse_nodes(dict_nodes, device_type),
    )
