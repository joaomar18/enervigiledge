###########EXTERNAL IMPORTS############

import asyncio
from typing import Dict, List, Any
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient
from web.api.decorator import auth_endpoint, AuthConfigs
import util.functions.images as img
import web.exceptions as api_exception
import web.parsers.device as device_parser
from util.debug import LoggerManager

#######################################


router = APIRouter(prefix="/device", tags=["device"])


@router.post("/add_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def add_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Adds a new device with configuration and optional image."""

    logger = LoggerManager.get_logger(__name__)

    device_data, device_nodes, device_image = await device_parser.parse_device_request(request)
    device_name = device_data.get("name")
    if not device_name or not isinstance(device_name, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.MISSING_DEVICE_NAME)

    record = device_parser.parse_device(new_device=True, dict_device=device_data, dict_nodes=device_nodes)
    # NEEDS VALIDATION BETWEEN PARSING AND INSTANTIATION. MOVE CREATION TO END OF TRY BLOCK WHEN VALIDATION EXISTS

    # DB Update
    conn = database.require_client()
    device_id = None

    try:
        await conn.execute("BEGIN")
        device_id = await database.insert_energy_meter(record, conn)
        if device_id is None:
            raise api_exception.DeviceCreationError(api_exception.Errors.DEVICE.DEVICE_STORAGE_FAILED)

        record.id = device_id
        new_device = device_manager.create_device_from_record(record)


        if device_image:
            image_result = await asyncio.get_running_loop().run_in_executor(img.api_executor, img.process_and_save_image, device_image, device_id, 200, "db/device_img/")
            if not image_result:
                raise api_exception.DeviceCreationError(api_exception.Errors.DEVICE.SAVE_IMAGE_FAILED)

        timedb_result = await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.create_db, device_name, device_id)
        if not timedb_result:
            raise api_exception.DeviceCreationError(api_exception.Errors.DEVICE.DEVICE_STORAGE_FAILED)

        await conn.commit()

    except Exception:
        await conn.rollback()
        if device_id:
            await asyncio.get_running_loop().run_in_executor(img.api_executor, img.delete_device_image, device_id, "db/device_img/")
        raise

    await device_manager.add_device(new_device)
    logger.info(f"Added new device '{new_device.name}' with ID {new_device.id}.")
    return JSONResponse(content={"message": "Device added sucessfully."})


@router.post("/edit_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def edit_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Updates existing device configuration and optional image."""

    logger = LoggerManager.get_logger(__name__)

    device_data, device_nodes, device_image = await device_parser.parse_device_request(request)
    device_id = device_parser.parse_device_id(device_data)
    record = device_parser.parse_device(new_device=False, dict_device=device_data, dict_nodes=device_nodes)
    # NEEDS VALIDATION BETWEEN PARSING AND INSTANTIATION. MOVE CREATION TO END OF TRY BLOCK WHEN VALIDATION EXISTS
    updated_device = device_manager.create_device_from_record(record)

    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    # DB Update
    conn =database.require_client()

    try:
        await conn.execute("BEGIN")
        if not await database.update_energy_meter(record, conn):
            raise api_exception.DeviceUpdateError(api_exception.Errors.DEVICE.UPDATE_STORAGE_FAILED)
        
        if device_image:
            image_result = await asyncio.get_running_loop().run_in_executor(img.api_executor, img.process_and_save_image, device_image, device_id, 200, "db/device_img/", "db/device_img/.bin/")
            if not image_result:
                raise api_exception.DeviceUpdateError(api_exception.Errors.DEVICE.SAVE_IMAGE_FAILED)

        await conn.commit()
        await asyncio.get_running_loop().run_in_executor(img.api_executor, img.flush_bin_images, "db/device_img/.bin/")

    except Exception:
        await conn.rollback()
        await asyncio.get_running_loop().run_in_executor(img.api_executor, img.rollback_image, device_id, "db/device_img/", "db/device_img/.bin/")
        raise

    await device_manager.delete_device(device)
    await device_manager.add_device(updated_device)
    logger.info(f"Updated device '{updated_device.name}' with ID {updated_device.id}.")
    return JSONResponse(content={"message": "Device edited sucessfully."})


@router.delete("/delete_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def delete_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
    timedb: TimeDBClient = Depends(services.get_timedb),
) -> JSONResponse:
    """Removes device from system and deletes associated data."""

    logger = LoggerManager.get_logger(__name__)

    try:
        payload: Dict[str, Any] = await request.json()  # request payload
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.INVALID_JSON)

    device_id = device_parser.parse_device_id(payload)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    # DB Update
    conn = database.require_client()

    try:
        await conn.execute("BEGIN")
        if not await database.delete_device(device.id, conn):
            raise api_exception.DeviceDeleteError(api_exception.Errors.DEVICE.DELETE_STORAGE_FAILED)

        await conn.commit()

    except Exception:
        await conn.rollback()
        raise

    await asyncio.get_running_loop().run_in_executor(img.api_executor, img.delete_device_image, device_id, "db/device_img/")
    await asyncio.get_running_loop().run_in_executor(timedb.api_executor, timedb.delete_db, device.name, device_id)
    await device_manager.delete_device(device)
    logger.info(f"Deleted device '{device.name}' with ID {device.id}.")
    return JSONResponse(content={"message": "Device deleted sucessfully."})


@router.get("/get_device")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves object with configuration and state of a specific device."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")
    return JSONResponse(content=device.get_device())


@router.get("/get_device_extended_info")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_extended_info(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves comprehensive device information including history status of the device."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")
    device_info = await device.get_extended_info(database.get_device_history)
    return JSONResponse(content=device_info)


@router.get("/get_device_identification")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_identification(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves device identification."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    device_identification: Dict[str, Any] = {}
    device_identification["id"] = device.id
    device_identification["name"] = device.name
    return JSONResponse(content=device_identification)


@router.get("/get_all_devices_status")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves status of all devices."""

    all_status = []
    for device in device_manager.devices:
        current_status: Dict[str, Any] = {}
        current_status["id"] = device.id
        current_status["name"] = device.name
        current_status["connected"] = device.connected
        current_status["alarm"] =  any([node for node in device.meter_nodes.nodes.values() if node.config.enabled and node.processor.in_alarm()])
        current_status["warning"] = any([node for node in device.meter_nodes.nodes.values() if node.config.enabled and node.processor.in_warning()])
        all_status.append(current_status)

    return JSONResponse(content=all_status)


@router.get("/get_device_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_with_image(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves object and image of a specific device."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    device_obj = device.get_device()
    device_obj["image"] = await asyncio.get_running_loop().run_in_executor(img.api_executor, img.get_device_image, device.id, "default", "db/device_img/")

    return JSONResponse(content=device_obj)


@router.get("/get_device_extended_info_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_extended_info_with_image(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves device information including history status and image of a specific device."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    device_info = await device.get_extended_info(database.get_device_history)
    device_info["image"] = await asyncio.get_running_loop().run_in_executor(img.api_executor, img.get_device_image, device.id, "default", "db/device_img/")
    return JSONResponse(content=device_info)


@router.get("/get_device_identification_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_device_identification_with_image(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
    database: SQLiteDBClient = Depends(services.get_db),
) -> JSONResponse:
    """Retrieves device identification including image."""

    device_id = device_parser.parse_device_id(request.query_params)
    device = device_manager.get_device(device_id)
    if not device:
        raise api_exception.DeviceNotFound(api_exception.Errors.DEVICE.NOT_FOUND, f"Device with id {device_id} not found.")

    device_identification: Dict[str, Any] = {}
    device_identification["id"] = device.id
    device_identification["name"] = device.name
    device_identification["image"] =await asyncio.get_running_loop().run_in_executor(img.api_executor, img.get_device_image, device.id, "default", "db/device_img/")
    return JSONResponse(content=device_identification)


@router.get("/get_all_devices_status_with_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_all_devices_with_image(
    request: Request,
    safety: HTTPSafety = Depends(services.get_safety),
    device_manager: DeviceManager = Depends(services.get_device_manager),
) -> JSONResponse:
    """Retrieves current status and images of all devices."""

    all_status = []
    image_tasks: List[asyncio.Future[Dict[str, str]]] = []
    for device in device_manager.devices:
        current_status: Dict[str, Any] = {}
        current_status["id"] = device.id
        current_status["name"] = device.name
        current_status["connected"] = device.connected
        current_status["alarm"] =  any([node for node in device.meter_nodes.nodes.values() if node.config.enabled and node.processor.in_alarm()])
        current_status["warning"] = any([node for node in device.meter_nodes.nodes.values() if node.config.enabled and node.processor.in_warning()])
        image_tasks.append(asyncio.get_running_loop().run_in_executor(img.api_executor, img.get_device_image, device.id, "default", "db/device_img/"))
        all_status.append(current_status)
    
    images = await asyncio.gather(*image_tasks)
    for status, image in zip(all_status, images):
        status["image"] = image

    return JSONResponse(content=all_status)


@router.get("/get_default_image")
@auth_endpoint(AuthConfigs.PROTECTED)
async def get_default_image(request: Request, safety: HTTPSafety = Depends(services.get_safety)) -> JSONResponse:
    """Retrieves the default device image."""

    image = await asyncio.get_running_loop().run_in_executor(img.api_executor, img.get_device_image, 0, "default", "db/device_img/", "png", "utf-8", True)
    return JSONResponse(content=image)
