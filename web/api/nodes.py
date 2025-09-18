###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Depends
from typing import Optional
from fastapi.responses import JSONResponse
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from controller.manager import DeviceManager
from db.timedb import TimeDBClient
import util.functions.objects as objects
import util.functions.date as date

#######################################

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("/get_nodes_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state of device nodes with optional filtering."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    filter: Optional[str] = request.query_params.get("filter")  # Optional

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    if filter:
        nodes_state = {node.config.name: node.processor.get_publish_format() for node in device.nodes if filter in node.config.name}
    else:
        nodes_state = {node.config.name: node.processor.get_publish_format() for node in device.nodes}

    return JSONResponse(content=nodes_state)


@router.get("/get_node_detailed_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_node_detailed_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:

    device_id = int(objects.require_field(request.query_params, "device_id", str))
    node_name = objects.require_field(request.query_params, "node_name", str)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    nodes = [node for node in device.nodes if node.config.name == node_name]

    if len(nodes) == 0:
        raise ValueError(f"Device with id {device_id} does not have a node with name {node_name}.")
    elif len(nodes) != 1:
        raise ValueError(f"Device with id {device_id} has more than 1 node with name {node_name}.")

    node_detailed_state = nodes[0].processor.get_detailed_state()

    return JSONResponse(content=node_detailed_state)


@router.get("/get_nodes_config")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_config(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves configuration of device nodes with optional filtering."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    filter: Optional[str] = request.query_params.get("filter")  # Optional

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    if filter:
        nodes_config = {}
        for node in device.nodes:
            if filter in node.config.name:
                record = node.get_node_record()
                record.device_id = device_id
                nodes_config[node.config.name] = record.__dict__
    else:
        nodes_config = {}
        for node in device.nodes:
            record = node.get_node_record()
            record.device_id = device_id
            nodes_config[node.config.name] = record.__dict__

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

    device_id = int(objects.require_field(request.query_params, "id", str))
    name = objects.require_field(request.query_params, "node", str)
    formatted = request.query_params.get("formatted")

    if formatted:
        time_step = objects.require_field(request.query_params, "time_step", int)
        start_time = objects.require_field(request.query_params, "start_time", str)
        end_time = request.query_params.get("end_time")
        end_time = end_time if end_time is not None else datetime.isoformat(datetime.now())  # If None accounts end time is now
    else:
        start_time = request.query_params.get("start_time")  # Optional
        end_time = request.query_params.get("end_time")  # Optional
        time_step = None

    start_time = datetime.fromisoformat(start_time) if start_time else None
    end_time = datetime.fromisoformat(end_time) if end_time else None

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    if not any(name == node.config.name for node in device.nodes):
        raise ValueError(f"Node with name {name} does not exist in device {device.name} with id {device_id}")

    if formatted and start_time and end_time and time_step:
        (start_time, end_time) = date.process_time_span(start_time, end_time, time_step)
        pass

    response = timedb.get_measurement_data_between(
        device_name=device.name, device_id=device_id, measurement=name, start_time=start_time, end_time=end_time
    )
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

    data = await request.json()

    device_id = objects.require_field(data, "id", int)
    name = objects.require_field(data, "node", str)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    if not any(name == node.config.name for node in device.nodes):
        raise ValueError(f"Node with name {name} does not exist in device {device.name} with id {device_id}.")

    result = timedb.delete_measurement_data(device_name=device.name, device_id=device_id, variable=device.nodes)

    if result:
        message = f"Successfully deleted logs for node '{name}' from device '{device.name}' with id {device_id}."
        return JSONResponse(content={"result": message})

    raise ValueError(f"Could not delete logs for node '{name}' from device '{device.name}' with id {device_id}.")


@router.delete("/delete_all_logs")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_all_logs(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Deletes all historical logs from a specific device."""

    data = await request.json()
    device_id = objects.require_field(data, "id", int)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    result = timedb.delete_db(device_name=device.name, device_id=device_id)

    if result:
        message = f"Successfully deleted all logs from device '{device.name}' with id {device_id}."
        return JSONResponse(content={"result": message})

    raise ValueError(f"Could not delete all logs from from device '{device.name}' with id {device_id}.")
