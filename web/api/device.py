###########EXTERNAL IMPORTS############

import json
from typing import Dict, Tuple, List, Any, Optional
from fastapi import APIRouter, Request, Depends
from starlette.datastructures import UploadFile
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from web.api.decorator import auth_endpoint, AuthConfigs
from util.functions.images import process_and_save_image, get_device_image, delete_device_image
from controller.conversion import convert_dict_to_energy_meter
import util.functions.objects as objects

#######################################

##########     P A R S E     M E T H O D S     ##########


async def _parse_device_request(request: Request) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[UploadFile]]:
    """
    Parses a device request, handling both JSON and multipart form data.

    Args:
        request (Request): The incoming HTTP request.

    Returns:
        Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[UploadFile]]:
            - device_data: Parsed device data as a dictionary.
            - device_nodes: Parsed device nodes as a list of dictionaries.
            - device_image: Uploaded image file if present, otherwise None.
    """

    content_type = request.headers.get("content-type", "")

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        device_data: Dict[str, Any] = json.loads(objects.require_field(form, "deviceData", str))
        device_nodes: List[Dict[str, Any]] = json.loads(objects.require_field(form, "deviceNodes", str))
        device_image = objects.require_field(form, "deviceImage", UploadFile)
    else:
        payload: Dict[str, Any] = await request.json()
        device_data = objects.require_field(payload, "deviceData", Dict[str, Any])
        device_nodes = objects.require_field(payload, "deviceNodes", List[Dict[str, Any]])
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
    device_name = objects.require_field(device_data, "name", str)

    # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
    energy_meter = convert_dict_to_energy_meter(device_data, device_nodes, device_manager.publish_queue, device_manager.measurements_queue)
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
    device_id = objects.require_field(device_data, "id", int)

    # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
    energy_meter = convert_dict_to_energy_meter(device_data, device_nodes, device_manager.publish_queue, device_manager.measurements_queue)

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

    payload: Dict[str, Any] = await request.json()
    device_id = objects.require_field(payload, "deviceID", int)

    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device not found with id {device_id}")

    await device_manager.delete_device(device)
    if database.delete_device(device_id):

        if not delete_device_image(device_id, "db/device_img/"):
            raise ValueError(f"Could not delete device image of device id: {device_id}")

        return JSONResponse(content={"message": "Device deleted sucessfully."})

    raise Exception(f"Could not delete device with name {device.name if device else 'not found'} and id {device_id} from the database.")


@router.get("/get_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves object with configuration and state of a specific device."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")
    return JSONResponse(content=device.get_device())


@router.get("/get_device_info")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_info(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves comprehensive device information including history status of the device."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")
    return JSONResponse(content=device.get_device_info(database.get_device_history))


@router.get("/get_all_devices")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves objects of all devices."""

    all_obj = [device.get_device() for device in device_manager.devices]
    return JSONResponse(content=all_obj)


@router.get("/get_device_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_with_image(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves object and image of a specific device."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    device_obj = device.get_device()
    device_obj["image"] = get_device_image(device.id, "default", "db/device_img/")
    return JSONResponse(content=device_obj)


@router.get("/get_device_info_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_info_with_image(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves device information including history status and image of a specific device."""

    device_id = int(objects.require_field(request.query_params, "id", str))
    device = device_manager.get_device(device_id)
    if not device:
        raise ValueError(f"Device with id {device_id} does not exist.")

    device_info = device.get_device_info(database.get_device_history)
    device_info["image"] = get_device_image(device.id, "default", "db/device_img/")
    return JSONResponse(content=device_info)


@router.get("/get_all_devices_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices_with_image(
    request: Request, safety: HTTPSafety = Depends(services.get_safety), device_manager: DeviceManager = Depends(services.get_device_manager)
) -> JSONResponse:
    """Retrieves current state and images of all devices."""

    all_obj = []
    for device in device_manager.devices:
        device_obj = device.get_device()
        device_obj["image"] = get_device_image(device.id, "default", "db/device_img/")
        all_obj.append(device_obj)

    return JSONResponse(content=all_obj)


@router.get("/get_default_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_default_image(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Retrieves the default device image."""

    image = get_device_image(device_id=0, default_image_str="default", directory="db/device_img/", force_default=True)
    return JSONResponse(content=image)
