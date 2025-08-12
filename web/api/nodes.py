###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from controller.manager import DeviceManager
from db.timedb import TimeDBClient

#######################################

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("/get_nodes_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state of device nodes with optional filtering."""

    id_raw = request.query_params.get("id")
    filter_str = request.query_params.get("filter")  # Optional

    if not id_raw:
        raise ValueError("Missing required query parameters: 'id'")

    device = device_manager.get_device(int(id_raw))

    if not device:
        raise ValueError(f"Device with id {id_raw!r} does not exist.")

    if filter_str:
        nodes_state = {node.name: node.get_publish_format() for node in device.nodes if filter_str in node.name}
    else:
        nodes_state = {node.name: node.get_publish_format() for node in device.nodes}

    return JSONResponse(content=nodes_state)


@router.get("/get_nodes_config")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_nodes_config(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves configuration of device nodes with optional filtering."""

    id_raw = request.query_params.get("id")
    filter_str = request.query_params.get("filter")  # Optional

    if not id_raw:
        raise ValueError("Missing required query parameters: 'id'")

    device = device_manager.get_device(int(id_raw))

    if not device:
        raise ValueError(f"Device with id {id_raw!r} does not exist.")

    if filter_str:
        nodes_config = {}
        for node in device.nodes:
            if filter_str in node.name:
                record = node.get_node_record()
                record.device_id = int(id_raw)
                nodes_config[node.name] = record.__dict__
    else:
        nodes_config = {}
        for node in device.nodes:
            record = node.get_node_record()
            record.device_id = int(id_raw)
            nodes_config[node.name] = record.__dict__

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

    id_raw = request.query_params.get("id")
    node_name = request.query_params.get("node")
    start_time_str = request.query_params.get("start_time")
    end_time_str = request.query_params.get("end_time")

    if not all([id_raw, node_name]):
        raise ValueError("Missing one or more required fields: 'id', 'node'")

    # Optional time range parsing
    start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
    end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

    device = device_manager.get_device(int(id_raw))

    if not device:
        raise ValueError(f"Device with id {id_raw!r} does not exist.")

    if not any(node_name == node.name for node in device.nodes):
        raise ValueError(f"Node with name {node_name} does not exist in device {device.name if device else 'not found'} with id {id_raw!r}")

    response = timedb.get_measurement_data_between(
        device_name=device.name, device_id=int(id_raw), measurement=node_name, start_time=start_time, end_time=end_time
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

    id_raw = data.get("id")
    node_name = data.get("node")

    if not all([id_raw, node_name]):
        raise ValueError("Missing one or more required fields: 'id', 'node'")

    device = device_manager.get_device(int(id_raw))
    if not device:
        raise ValueError(f"Device with id {id_raw!r} does not exist.")

    if not any(node_name == node.name for node in device.nodes):
        raise ValueError(f"Node with name {node_name} does not exist in device {device.name if device else 'not found'} with id {id_raw!r}.")

    result = timedb.delete_measurement_data(device_name=device.name, device_id=int(id_raw), measurement=node_name)

    if result:
        message = f"Successfully deleted logs for node '{node_name}' from device '{device.name}' with id {id_raw!r}."
        return JSONResponse(content={"result": message})

    raise ValueError(f"Could not delete logs for node '{node_name}' from device '{device.name}' with id {id_raw!r}.")


@router.delete("/delete_all_logs")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_all_logs(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), timedb: TimeDBClient = Depends(services.get_timedb)
) -> JSONResponse:
    """Deletes all historical logs from a specific device."""

    data = await request.json()

    name = data.get("name")
    id_raw = data.get("id")

    if not all([name, id_raw]):
        raise ValueError("Missing one or more required fields: 'name', 'id'.")

    result = timedb.delete_db(device_name=name, device_id=int(id_raw))

    if result:
        message = f"Successfully deleted all logs from device '{name}' with id {id_raw!r}."
        return JSONResponse(content={"result": message})

    raise ValueError(f"Could not delete all logs from from device '{name}' with id {id_raw!r}.")
