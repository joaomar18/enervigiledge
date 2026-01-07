###########EXTERNAL IMPORTS############

import asyncio
from fastapi import APIRouter, Request, Depends
from typing import Optional, Dict, Any
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

import controller.meter.extraction as meter_extraction
from web.safety import HTTPSafety
from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from controller.manager import DeviceManager
from db.timedb import TimeDBClient
from model.controller.node import NodePhase, NodeDirection
import util.functions.objects as objects
import util.functions.date as date
import web.exceptions as api_exception
import web.parsers.device as device_parser
import web.parsers.nodes as nodes_parser

#######################################


router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("/get_nodes_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_state(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves current state of device nodes with optional filtering."""

    device_id = device_parser.parse_device_id(request.query_params)
    filter: Optional[str] = request.query_params.get("filter")  # Optional
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    if filter:
        nodes_state = {
            node.config.name: node.get_publish_format() for node in device.meter_nodes.nodes.values() if node.config.publish and filter in node.config.name
        }
    else:
        nodes_state = {node.config.name: node.get_publish_format() for node in device.meter_nodes.nodes.values() if node.config.publish}

    return JSONResponse(content={"meter_type": device.get_device().get("type", None), "nodes_state": nodes_state})


@router.get("/get_node_extended_info")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_node_extended_info(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Returns extended runtime and status information for a specific device node."""

    device_id = device_parser.parse_device_id(request.query_params)
    name = request.query_params.get("node_name")
    if name is None or not isinstance("name", str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_NAME)

    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    node = next((n for n in device.meter_nodes.nodes.values() if n.config.name == name), None)

    if not node:
        raise api_exception.NodeNotFound(api_exception.Errors.NODES.NOT_FOUND)

    node_detailed_state = node.get_extended_info()
    return JSONResponse(content=node_detailed_state)


@router.get("/get_nodes_config")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_config(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves configuration of device nodes with optional filtering."""

    device_id = device_parser.parse_device_id(request.query_params)
    filter: Optional[str] = request.query_params.get("filter")  # Optional
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    if filter:
        nodes_config = {}
        for node in device.meter_nodes.nodes.values():
            if filter in node.config.name:
                record = node.get_node_record()
                record.device_id = device_id
                nodes_config[node.config.name] = record.get_attributes()
    else:
        nodes_config = {}
        for node in device.meter_nodes.nodes.values():
            record = node.get_node_record()
            record.device_id = device_id
            nodes_config[node.config.name] = record.get_attributes()

    return JSONResponse(content=nodes_config)


@router.get("/get_logs_from_node")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_logs_from_node(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Retrieves historical logs from a specific device node within time range."""

    device_id = device_parser.parse_device_id(request.query_params)
    name = request.query_params.get("node_name")
    if name is None or not isinstance(name, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_NAME)
    formatted = objects.check_bool_str(request.query_params.get("formatted"))
    time_span = await nodes_parser.parse_formatted_time_span(request, formatted)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    node = next((n for n in device.meter_nodes.nodes.values() if n.config.name == name), None)
    if not node:
        raise api_exception.NodeNotFound(api_exception.Errors.NODES.NOT_FOUND)

    date.process_time_span(time_span)
    response = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.get_variable_logs, device.name, device_id, node, time_span)
    return JSONResponse(content=response.get_logs())


@router.get("/get_energy_consumption")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_energy_consumption(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Retrieves active and reactive energy data for a specific device phase and direction,
    and computes the corresponding average power factor within the selected time range."""

    device_id = device_parser.parse_device_id(request.query_params)

    phase = request.query_params.get("phase")
    if phase is None or not isinstance(phase, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_PHASE)

    direction = request.query_params.get("direction")
    if direction is None or not isinstance(direction, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_ENERGY_DIRECTION)

    try:
        phase = NodePhase(phase)
    except Exception:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_PHASE)

    try:
        direction = NodeDirection(direction)
    except Exception:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_ENERGY_DIRECTION)

    formatted = objects.check_bool_str(request.query_params.get("formatted"))
    time_span = await nodes_parser.parse_formatted_time_span(request, formatted)

    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    date.process_time_span(time_span)
    response = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, meter_extraction.get_meter_energy_consumption, device, phase, direction, timedb, time_span)
    return JSONResponse(content=response)


@router.get("/get_peak_power")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_peak_demand_power(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Retrieves peak power metrics (min, max, avg) for active and apparent power
    of a specific device phase within the selected time range."""

    device_id = device_parser.parse_device_id(request.query_params)
    phase = request.query_params.get("phase")
    if phase is None or not isinstance(phase, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_PHASE)

    try:
        phase = NodePhase(phase)
    except Exception:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_PHASE)

    time_span = await nodes_parser.parse_formatted_time_span(request, False, False)

    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    date.process_time_span(time_span)
    response = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, meter_extraction.get_meter_peak_power, device, phase, timedb, time_span)
    return JSONResponse(content=response)


@router.delete("/delete_logs_from_node")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_logs_from_node(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Deletes all historical logs from a specific device node."""

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    device_id = device_parser.parse_device_id(payload)
    name = payload.get("node_name")
    if name is None or not isinstance(name, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_NAME)

    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    node = next((n for n in device.meter_nodes.nodes.values() if n.config.name == name), None)
    if not node:
        raise api_exception.NodeNotFound(api_exception.Errors.NODES.NOT_FOUND)

    has_logs = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.check_variable_has_logs, device.name, device_id, node)

    if has_logs:
        result = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.delete_variable_data, device.name, device_id, node)
        if not result:
            raise api_exception.DeviceDeleteError(api_exception.Errors.NODES.DELETE_LOGS_FAILED)

    return JSONResponse(content={"result": f"Successfully deleted logs for node '{name}' from device '{device.name}' with id {device_id}."})


@router.delete("/delete_all_logs")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_all_logs(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Deletes all historical logs from a specific device."""

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    device_id = device_parser.parse_device_id(payload)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    result = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.delete_all_data, device.name, device_id)
    if not result:
        raise api_exception.DeviceDeleteError(api_exception.Errors.NODES.DELETE_ALL_LOGS_FAILED)

    return JSONResponse(content={"result": f"Successfully deleted all logs from device '{device.name}' with id {device_id}."})
