###########EXTERNAL IMPORTS############

import json
from fastapi import APIRouter, Request, Header
from fastapi.responses import JSONResponse

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from util.debug import LoggerManager
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from util.functions import process_and_save_image, get_device_image, delete_device_image
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from controller.conversion import convert_dict_to_energy_meter

#######################################

router = APIRouter(prefix="/device", tags=["device"])


@router.post("/add_device")
async def add_device(
    safety: HTTPSafety, device_manager: DeviceManager, database: SQLiteDBClient, request: Request, authorization: str = Header(None)
) -> JSONResponse:
    """
    Creates and registers a new energy meter device with optional image upload.

    Args:
        safety: HTTPSafety instance for token validation and rate limiting
        device_manager: DeviceManager instance for device registration and lifecycle management
        database: SQLiteDBClient instance for persisting device configuration
        request: FastAPI request with device data (JSON or multipart form with image)
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message with device creation confirmation (200),
        or error message (400) if validation fails, unauthorized, or creation fails

    Raises:
        ValueError: When required fields are missing or device data is invalid
        Exception: For authentication errors, database issues, or device initialization failures
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        # Check if token is valid
        safety.check_authorization_token(authorization, request)

        content_type = request.headers.get("content-type", "")

        # Image included
        if content_type.startswith("multipart/form-data"):
            form = await request.form()

            device_data_str = form.get("deviceData")
            device_nodes_str = form.get("deviceNodes")
            device_image = form.get("deviceImage")

            if not device_data_str or not device_nodes_str:
                raise ValueError("Device data and device nodes are required")

            device_data = json.loads(device_data_str)
            device_nodes = json.loads(device_nodes_str)

        # Image not included
        else:
            payload = await request.json()
            device_data = payload.get("deviceData")
            device_nodes = payload.get("deviceNodes")
            device_image = None

        device_name = device_data.get("name") if device_data else None

        if not all([device_data, device_nodes]):
            raise ValueError("All fields are required")

        # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
        energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(device_data, device_nodes)
        energy_meter_record = energy_meter.get_meter_record()

        device_id = database.insert_energy_meter(energy_meter_record)
        if device_id is not None:
            energy_meter.id = device_id

            if device_image:
                process_and_save_image(device_image, device_id, 200, "db/device_img/")

            device_manager.add_device(energy_meter)

            safety.clean_failed_requests(ip, request.url.path)
            return JSONResponse(content={"message": "Device added sucessfully."})
        else:
            raise Exception(f"Could not add device with name {device_name} and id {device_id} in the database.")

    except Exception as e:
        safety.increment_failed_requests(ip, request.url.path)
        logger.exception(f"Failed add device attempt from IP {ip}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.post("/edit_device")
async def edit_device(
    safety: HTTPSafety, device_manager: DeviceManager, database: SQLiteDBClient, request: Request, authorization: str = Header(None)
) -> JSONResponse:
    """
    Updates an existing energy meter device configuration with optional image replacement.

    Args:
        safety: HTTPSafety instance for token validation and rate limiting
        device_manager: DeviceManager instance for device lifecycle management
        database: SQLiteDBClient instance for persisting updated configuration
        request: FastAPI request with updated device data (JSON or multipart form with image)
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message with device update confirmation (200),
        or error message (400) if validation fails, device not found, or update fails

    Raises:
        ValueError: When required fields are missing, device not found, or data is invalid
        Exception: For authentication errors, database issues, or device reconfiguration failures
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        # Check if token is valid
        safety.check_authorization_token(authorization, request)

        content_type = request.headers.get("content-type", "")

        # Image included
        if content_type.startswith("multipart/form-data"):
            form = await request.form()

            device_data_str = form.get("deviceData")
            device_nodes_str = form.get("deviceNodes")
            device_image = form.get("deviceImage")  # This will be the file

            if not device_data_str or not device_nodes_str:
                raise ValueError("Device data and device nodes are required")

            device_data = json.loads(device_data_str)
            device_nodes = json.loads(device_nodes_str)

            if device_image and hasattr(device_image, 'filename'):
                logger.info(f"Received image file: {device_image.filename}")

        # Image not included
        else:
            payload = await request.json()
            device_data = payload.get("deviceData")
            device_nodes = payload.get("deviceNodes")
            device_image = None

        device_id = device_data.get("id") if device_data else None

        if not all([device_data, device_nodes]):
            raise ValueError("All fields are required")

        # Tries to initialize a new energy meter with the given configuration. Throws exception if an error is found in the configuration
        energy_meter: ModbusRTUEnergyMeter | OPCUAEnergyMeter = convert_dict_to_energy_meter(device_data, device_nodes)

        device = device_manager.get_device(device_id)
        if not device:
            raise ValueError(f"Device not found with id {device_id}")

        device.stop()
        device_manager.delete_device(device)

        if database.update_energy_meter(energy_meter.get_meter_record()):

            # Process and save image if provided
            if device_image:
                process_and_save_image(device_image, device_id, 200, "db/device_img/")

            device_manager.add_device(energy_meter)

            safety.clean_failed_requests(ip, request.url.path)
            return JSONResponse(content={"message": "Device edited sucessfully."})

        else:
            raise Exception(f"Could not update device with name {device.name if device else 'not found'} and id {device_id} in the database.")

    except Exception as e:
        safety.increment_failed_requests(ip, request.url.path)
        logger.exception(f"Failed edit device attempt from IP {ip}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.delete("/delete_device")
async def delete_device(
    safety: HTTPSafety, device_manager: DeviceManager, database: SQLiteDBClient, request: Request, authorization: str = Header(None)
) -> JSONResponse:
    """
    Permanently removes an energy meter device and all associated data including images.

    Args:
        safety: HTTPSafety instance for token validation and rate limiting
        device_manager: DeviceManager instance for device lifecycle management
        database: SQLiteDBClient instance for removing device configuration from storage
        request: FastAPI request with JSON containing device name and ID for verification
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message with device deletion confirmation (200),
        or error message (400) if validation fails, device not found, or deletion fails

    Raises:
        ValueError: When required fields are missing, device not found, or name/ID mismatch
        Exception: For authentication errors, database issues, or image deletion failures
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        # Check if token is valid
        safety.check_authorization_token(authorization, request)

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

        device.stop()
        device_manager.delete_device(device)
        if database.delete_energy_meter(device.get_meter_record()):

            if not delete_device_image(device_id, "db/device_img/"):
                raise ValueError(f"Could not delete device image of device id: {device_id}")

            safety.clean_failed_requests(ip, request.url.path)
            return JSONResponse(content={"message": "Device deleted sucessfully."})

        else:
            raise Exception(f"Could not delete device with name {device.name if device else 'not found'} and id {device_id} from the database.")

    except Exception as e:
        safety.increment_failed_requests(ip, request.url.path)
        logger.warning(f"Failed delete device attempt from IP {ip}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.get("/get_device_state")
async def get_device_state(device_manager: DeviceManager, request: Request) -> JSONResponse:
    """
    Retrieves current state and metadata for a specific energy meter device including its image.

    Args:
        device_manager: DeviceManager instance for device lookup and state access
        request: FastAPI request with 'id' query parameter specifying the device

    Returns:
        JSONResponse: Device state dictionary with metadata and base64-encoded image (200),
        or error message (400) if device not found or invalid ID provided

    Raises:
        ValueError: When 'id' query parameter is missing or not a valid integer
        KeyError: When device with specified ID doesn't exist in the device manager
        Exception: For image processing errors or other state retrieval issues
    """

    logger = LoggerManager.get_logger(__name__)

    try:
        # Read parameters from the query string, not the JSON body
        id_raw = request.query_params.get("id")

        if not id_raw:
            raise ValueError("Missing required query parameters: 'id'")

        try:
            device_id = int(id_raw)
        except ValueError:
            raise ValueError(f"Invalid device id: {id_raw!r}")

        device = device_manager.get_device(device_id)
        if not device:
            raise KeyError(f"Device with id {device_id} does not exist.")

        device_state = device.get_device_state()
        device_state["image"] = get_device_image(device.id, "default", "db/device_img/")
        return JSONResponse(content=device_state)

    except Exception as e:
        logger.error(f"Failed to get device state for id={id_raw!r}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.get("/get_all_devices_state")
async def get_all_devices_state(device_manager: DeviceManager) -> JSONResponse:
    """
    Retrieves current state and metadata for all registered energy meter devices with their images.

    Args:
        device_manager: DeviceManager instance containing all registered devices

    Returns:
        JSONResponse: List of device state dictionaries, each with metadata and base64-encoded image (200),
        or error message (400) if state retrieval fails for any device

    Raises:
        Exception: For image processing errors, device state access issues, or JSON serialization problems
    """

    logger = LoggerManager.get_logger(__name__)

    try:
        all_states = []
        for device in device_manager.devices:
            device_state = device.get_device_state()
            device_state["image"] = get_device_image(device.id, "default", "db/device_img/")
            all_states.append(device_state)

        return JSONResponse(content=all_states)

    except Exception as e:
        logger.error(f"Failed to retrieve all device states: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.get("/get_default_image")
async def get_default_image() -> JSONResponse:
    """
    Retrieves the default device image used as fallback when no custom image is available.

    Args:
        None

    Returns:
        JSONResponse: Base64-encoded default device image (200),
        or error message (400) if default image retrieval fails

    Raises:
        Exception: For image file access errors, encoding issues, or file system problems
    """

    logger = LoggerManager.get_logger(__name__)

    try:
        image = get_device_image(device_id=0, default_image_str="default", directory="db/device_img/", force_default=True)
        return JSONResponse(content=image)

    except Exception as e:
        logger.error(f"Failed to get device default image")
        return JSONResponse(status_code=400, content={"error": str(e)})
