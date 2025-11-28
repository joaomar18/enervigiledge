###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Depends
from typing import Optional
from fastapi.responses import JSONResponse
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.meter.meter import EnergyMeter
import controller.meter.extraction as meter_extraction
from web.safety import HTTPSafety
from web.dependencies import services
from web.api.decorator import auth_endpoint, AuthConfigs
from controller.manager import DeviceManager
from db.timedb import TimeDBClient
from model.controller.node import NodePhase, NodeDirection
from model.date import FormattedTimeStep, TimeSpanParameters
import util.functions.objects as objects
import util.functions.date as date

#######################################


router = APIRouter(prefix="/nodes", tags=["nodes"])


##########     P A R S E     M E T H O D S     ##########


async def _parse_formatted_time_span(request: Request, formatted: bool, force_aggregation: Optional[bool] = None) -> TimeSpanParameters:
    """Parse time span parameters from request query params.
    
    Extracts and converts start_time, end_time, time_step, formatted flag, and time_zone
    from query parameters. When formatted=true, start_time is required and end_time defaults
    to now. Returns datetime objects with second precision removed.
    
    Args:
        request: The HTTP request containing query parameters.
        formatted: If True, enables formatted time span behavior with required start_time
            and optional time_step. If False, all time parameters are optional.
        force_aggregation: Optional flag to force aggregation of data when True.
    
    Returns:
        TimeSpanParameters: Containing the parsed time span configuration with start_time,
            end_time, time_step, formatted flag, time_zone, and force_aggregation settings.
    """

    time_zone = date.get_time_zone_info(request.query_params.get("time_zone"))

    if formatted:
        time_step = request.query_params.get("time_step")
        time_step = FormattedTimeStep(time_step) if time_step is not None else None
        start_time = objects.require_field(request.query_params, "start_time", str)
        end_time = request.query_params.get("end_time")
        end_time = end_time if end_time is not None else datetime.isoformat(datetime.now())  # If None accounts end time is now
    else:
        start_time = request.query_params.get("start_time")  # Optional
        end_time = request.query_params.get("end_time")  # Optional
        time_step = None

    start_time = date.remove_sec_precision(date.convert_isostr_to_date(start_time)) if start_time else None
    end_time = date.remove_sec_precision(date.convert_isostr_to_date(end_time)) if end_time else None

    return TimeSpanParameters(start_time, end_time, time_step, formatted, time_zone, force_aggregation)


#########################################################


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
        nodes_state = {node.config.name: node.get_publish_format() for node in device.nodes if node.config.publish and filter in node.config.name}
    else:
        nodes_state = {node.config.name: node.get_publish_format() for node in device.nodes if node.config.publish}

    return JSONResponse(content={"meter_type": device.get_device().get("type", None), "nodes_state": nodes_state})


@router.get("/get_node_additional_info")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_node_additional_info(
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

    node_detailed_state = nodes[0].get_additional_info()

    if isinstance(device, EnergyMeter):
        node_detailed_state.update({"read_period": device.communication_options.read_period})

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

    device_id = int(objects.require_field(request.query_params, "device_id", str))
    name = objects.require_field(request.query_params, "node_name", str)
    formatted = objects.check_bool_str(request.query_params.get("formatted"))
    time_span = await _parse_formatted_time_span(request, formatted)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    node = next((n for n in device.nodes if n.config.name == name), None)
    if not node:
        raise ValueError(f"Node with name {name} does not exist in device {device.name} with id {device_id}")

    date.process_time_span(time_span)
    response = timedb.get_variable_logs(device.name, device_id, node, time_span).get_logs()
    return JSONResponse(content=response)


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

    device_id = int(objects.require_field(request.query_params, "device_id", str))
    phase = NodePhase(objects.require_field(request.query_params, "phase", str))
    direction = NodeDirection(objects.require_field(request.query_params, "direction", str))
    formatted = objects.check_bool_str(request.query_params.get("formatted"))
    time_span = await _parse_formatted_time_span(request, formatted)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")
    
    date.process_time_span(time_span)
    response = meter_extraction.get_meter_energy_consumption(device, phase, direction, timedb, time_span)
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

    device_id = int(objects.require_field(request.query_params, "device_id", str))
    phase = NodePhase(objects.require_field(request.query_params, "phase", str))
    time_span = await _parse_formatted_time_span(request, False, False)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    date.process_time_span(time_span)
    response = meter_extraction.get_meter_peak_power(device, phase, timedb, time_span)
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

    device_id = objects.require_field(data, "device_id", int)
    name = objects.require_field(data, "node_name", str)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    node = next((n for n in device.nodes if n.config.name == name), None)
    if not node:
        raise ValueError(f"Node with name {name} does not exist in device {device.name} with id {device_id}")

    result = timedb.delete_variable_data(device_name=device.name, device_id=device_id, variable=node)

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
    device_id = objects.require_field(data, "device_id", int)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    result = timedb.delete_db(device_name=device.name, device_id=device_id)

    if result:
        message = f"Successfully deleted all logs from device '{device.name}' with id {device_id}."
        return JSONResponse(content={"result": message})

    raise ValueError(f"Could not delete all logs from from device '{device.name}' with id {device_id}.")
