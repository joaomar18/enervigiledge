###########EXTERNAL IMPORTS############

import json
from typing import Dict, Tuple, List, Any, Optional
from fastapi import Request
from starlette.datastructures import UploadFile

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions, BaseCommunicationOptions, EnergyMeterRecord
import util.functions.objects as objects
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
        
        device_data_json = objects.require_field(form, "device_data", str)
        device_nodes_json = objects.require_field(form, "device_nodes", str)
        device_image = objects.require_field(form, "device_image", UploadFile)
        
        if not device_data_json:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_DATA)
        if not device_nodes_json:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_NODES_DATA)
        if not device_image:
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
        
        device_data_req: Optional[Dict[str, Any]] = objects.require_field(payload, "device_data", Dict[str, Any])
        device_nodes_req: Optional[List[Dict[str, Any]]] = objects.require_field(payload, "device_nodes", List[Dict[str, Any]])
        device_image = None

        if not device_data_req:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_DATA)

        if not device_nodes_req:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_NODES_DATA)
        
        device_data: Dict[str, Any] = device_data_req
        device_nodes: List[Dict[str, Any]] = device_nodes_req
        
    return device_data, device_nodes, device_image


def parse_device_options(dict_meter_options: Dict[str, Any]) -> EnergyMeterOptions:
    """
    Parse and validate energy meter options from a raw input dictionary.

    This function validates the provided meter options against the
    EnergyMeterOptions schema, ensuring all required fields are present
    and correctly typed. On validation failure, a structured
    InvalidRequestPayload API exception is raised with detailed error
    information suitable for frontend consumption.

    Args:
        dict_meter_options (Dict[str, Any]): Raw meter options payload,
            typically originating from an API request.

    Returns:
        EnergyMeterOptions: A validated and fully constructed meter
        options object.

    Raises:
        InvalidRequestPayload:
            - If required option fields are missing.
            - If one or more option fields have invalid types or values.
    """

    try:
        dataclass_fields, optional_fields = objects.check_required_keys(dict_meter_options, EnergyMeterOptions)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_DEVICE_OPTIONS_FIELDS, details={"fields": missing_fields})

    try:
        arguments = objects.create_dict_from_fields(dict_meter_options, dataclass_fields, optional_fields)
    except ValueError as e:
        invalid_field = e.args[0] if e.args else None
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.INVALID_DEVICE_OPTIONS_FIELDS, details={"field": invalid_field})
        
    return EnergyMeterOptions(**arguments)


def parse_communication_options(dict_communication_options: Dict[str, Any], protocol: Protocol) -> BaseCommunicationOptions:
    """
    Parse and validate communication options for a specific protocol.

    This function resolves the protocol-specific communication options
    schema, validates the provided options against it, and returns a
    fully constructed communication options instance. Validation errors
    are reported using structured API exceptions suitable for frontend
    consumption.

    Args:
        dict_communication_options (Dict[str, Any]): Raw communication
            options payload, typically provided via an API request.
        protocol (Protocol): Protocol used to select the appropriate
            communication options schema.

    Returns:
        BaseCommunicationOptions: A validated communication options
        instance corresponding to the given protocol.

    Raises:
        InvalidRequestPayload:
            - If the protocol is not supported.
            - If required communication option fields are missing.
            - If one or more communication option fields are invalid.
    """

    # Get protocol plugin from registry
    try:
        plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    except NotImplementedError as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_PROTOCOL)
    
    try:
        dataclass_fields, optional_fields = objects.check_required_keys(dict_communication_options, plugin.options_class)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_DEVICE_COMMUNICATION_FIELDS, details={"fields": missing_fields})
    
    try:
        arguments = objects.create_dict_from_fields(dict_communication_options, dataclass_fields, optional_fields)
    except ValueError as e:
        invalid_field = e.args[0] if e.args else None
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.INVALID_DEVICE_COMMUNICATION_FIELDS, details={"field": invalid_field})

    return plugin.options_class(**arguments)


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
    
    # Check for Configuration fields
    try:
        objects.check_required_keys(dict_device, EnergyMeterRecord, ("nodes",))
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_DEVICE_FIELDS, details={"fields": missing_fields})
    
    # Device ID Parsing
    if not new_device:
        device_id = objects.require_field(dict_device, "id", int)
        if device_id is None:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_ID)
    else:
        device_id = None
    
    # Name Parsing
    device_name = objects.require_field(dict_device, "name", str)
    if device_name is None:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_NAME)
    
    # Protocol Parsing
    protocol = objects.require_field(dict_device, "protocol", str)
    if protocol is None:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_PROTOCOL)
    try:
        protocol = Protocol(protocol)
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_PROTOCOL)
    
    # Type Parsing
    device_type = objects.require_field(dict_device, "type", str)
    if device_type is None:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_TYPE)
    try:
        device_type = EnergyMeterType(device_type)
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_TYPE)
    
    # Device Options
    device_options = objects.require_field(dict_device, "options", Dict[str, Any])
    if device_options is None:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_OPTIONS)
    device_options = parse_device_options(device_options)
    
    # Communication Options
    communication_options = objects.require_field(dict_device, "communication_options", Dict[str, Any])
    if communication_options is None:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_OPTIONS)
    communication_options = parse_communication_options(communication_options, protocol)
    
    return EnergyMeterRecord(
        name=device_name,
        id=device_id,
        protocol=protocol,
        type=device_type,
        options=device_options,
        communication_options=communication_options,
        nodes=parse_nodes(dict_nodes, device_type),
    )
