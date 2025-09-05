###########EXTERNAL IMPORTS############

import json
from typing import Dict, Tuple
from fastapi import APIRouter, Request, UploadFile, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from web.api.decorator import auth_endpoint, AuthConfigs
from util.functions.images import process_and_save_image, get_device_image, delete_device_image
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from controller.conversion import convert_dict_to_energy_meter

#######################################

##########     P A R S E     M E T H O D S     ##########


async def _parse_device_request(request: Request) -> Tuple[Dict | None, Dict | None, UploadFile | None]:
    """Extract device data, nodes, and optional image from the request.

    Supports both JSON payloads and multipart form submissions with an image.

    Args:
        request: Incoming FastAPI request containing device information.

    Returns:
        Tuple containing ``device_data``, ``device_nodes``, and ``device_image``.

    Raises:
        ValueError: If required fields are missing in multipart form data.
    """

    content_type = request.headers.get("content-type", "")

    if content_type.startswith("multipart/form-data"):
        form = await request.form()

        device_data_str = form.get("deviceData")
        device_nodes_str = form.get("deviceNodes")
        device_image = form.get("deviceImage")

        if not device_data_str or not device_nodes_str:
            raise ValueError("Device data and device nodes are required")

        device_data = json.loads(device_data_str)
        device_nodes = json.loads(device_nodes_str)
    else:
        payload = await request.json()
        device_data = payload.get("deviceData")
        device_nodes = payload.get("deviceNodes")
        device_image = None

    return device_data, device_nodes, device_image


#########################################################

router = APIRouter(prefix="/device", tags=["device"])


@router.post("/add_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def add_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Adds a new device with configuration and optional image."""

    device_data, device_nodes, device_image = await _parse_device_request(request)
    device_name = device_data.get("name") if device_data else None

    if not all([device_data, device_nodes]):
        raise ValueError("All fields are required")

    # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
    energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(
        device_data, device_nodes, device_manager.publish_queue, device_manager.measurements_queue
    )
    energy_meter_record = energy_meter.get_meter_record()

    device_id = database.insert_energy_meter(energy_meter_record)
    if device_id is not None:
        energy_meter.id = device_id

        if device_image:
            process_and_save_image(device_image, device_id, 200, "db/device_img/")

        await device_manager.add_device(energy_meter)
        return JSONResponse(content={"message": "Device added sucessfully."})

    raise ValueError(f"Could not add device with name {device_name} and id {device_id} in the database.")


@router.post("/edit_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def edit_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Updates existing device configuration and optional image."""

    device_data, device_nodes, device_image = await _parse_device_request(request)
    device_id = device_data.get("id") if device_data else None

    if not all([device_data, device_nodes]):
        raise ValueError("All fields are required")

    # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
    energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(
        device_data, device_nodes, device_manager.publish_queue, device_manager.measurements_queue
    )

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device not found with id {device_id}")

    await device_manager.delete_device(device)

    if database.update_energy_meter(energy_meter.get_meter_record()):

        # Process and save image if provided
        if device_image:
            process_and_save_image(device_image, device_id, 200, "db/device_img/")

        await device_manager.add_device(energy_meter)
        return JSONResponse(content={"message": "Device edited sucessfully."})

    raise ValueError(f"Could not update device with name {device.name if device else 'not found'} and id {device_id} in the database.")


@router.delete("/delete_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Removes device from system and deletes associated data."""

    payload = await request.json()

    device_name = payload.get("deviceName")
    device_id = payload.get("deviceID")

    if not all([device_name, device_id]):
        raise ValueError("All fields are required")

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device not found with id {device_id}")

    if device.name != device_name:
        raise ValueError(f"Device name does not match request device_name {device_name} for id {device_id}")

    await device_manager.delete_device(device)
    if database.delete_energy_meter(device.get_meter_record()):

        if not delete_device_image(device_id, "db/device_img/"):
            raise ValueError(f"Could not delete device image of device id: {device_id}")

        return JSONResponse(content={"message": "Device deleted sucessfully."})

    raise Exception(f"Could not delete device with name {device.name if device else 'not found'} and id {device_id} from the database.")


@router.get("/get_device_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state of a specific device."""

    id_raw = request.query_params.get("id")

    if not id_raw:
        raise ValueError("Missing required query parameters: 'id'")

    try:
        device_id = int(id_raw)
    except ValueError:
        raise ValueError(f"Invalid device id: {id_raw!r}")

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")
    device_state = device.get_device_state()
    return JSONResponse(content=device_state)


@router.get("/get_all_devices_state")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state of all devices."""

    all_states = []
    for device in device_manager.devices:
        device_state = device.get_device_state()
        all_states.append(device_state)

    return JSONResponse(content=all_states)


@router.get("/get_device_state_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state and image of a specific device."""

    id_raw = request.query_params.get("id")

    if not id_raw:
        raise ValueError("Missing required query parameters: 'id'")

    try:
        device_id = int(id_raw)
    except ValueError:
        raise ValueError(f"Invalid device id: {id_raw!r}")

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    device_state = device.get_device_state()
    device_state["image"] = get_device_image(device.id, "default", "db/device_img/")
    return JSONResponse(content=device_state)


@router.get("/get_all_devices_state_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices_state(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state and images of all devices."""

    all_states = []
    for device in device_manager.devices:
        device_state = device.get_device_state()
        device_state["image"] = get_device_image(device.id, "default", "db/device_img/")
        all_states.append(device_state)

    return JSONResponse(content=all_states)


@router.get("/get_default_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_default_image(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Retrieves the default device image."""

    image = get_device_image(device_id=0, default_image_str="default", directory="db/device_img/", force_default=True)
    return JSONResponse(content=image)
