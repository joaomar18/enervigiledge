###########EXTERNAL IMPORTS############

from fastapi import APIRouter, Request, Header
from fastapi.responses import JSONResponse
from typing import Dict, Any
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from util.debug import LoggerManager
from controller.manager import DeviceManager
from db.timedb import TimeDBClient

#######################################

router = APIRouter(prefix="/nodes", tags=["nodes"])


@router.get("/get_nodes_state")
async def get_nodes_state(device_manager: DeviceManager, request: Request) -> JSONResponse:
    """
    Retrieves current values and states of nodes for a specified device.

    Args:
        device_manager: DeviceManager instance for device lookup and node access
        request: FastAPI request containing JSON with device ID and optional filter

    Returns:
        JSONResponse: Dictionary of node names to their publish format data (200),
        or error message (400) if device not found or invalid parameters

    Raises:
        ValueError: When required 'id' parameter is missing
        KeyError: When device with specified ID doesn't exist
        Exception: For other processing errors or invalid data types
    """

    logger = LoggerManager.get_logger(__name__)
    data: Dict[str, Any] = {}

    try:
        data = await request.json()
        id_raw = data.get("id")
        filter_str = data.get("filter")  # Optional

        if not id_raw:
            raise ValueError("Missing required query parameters: 'id'")

        device = device_manager.get_device(int(id_raw))

        if not device:
            raise KeyError(f"Device with id {id_raw!r} does not exist.")

        if filter_str:
            nodes_state = {node.name: node.get_publish_format() for node in device.nodes if filter_str in node.name}
        else:
            nodes_state = {node.name: node.get_publish_format() for node in device.nodes}

        return JSONResponse(content=nodes_state)

    except Exception as e:
        logger.error(f"Failed to get node states for device with id {id_raw!r}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.get("/get_nodes_config")
async def get_nodes_config(device_manager: DeviceManager, request: Request) -> JSONResponse:
    """
    Retrieves configuration details of nodes for a specified device.

    Args:
        device_manager: DeviceManager instance for device lookup and node access
        request: FastAPI request with query parameters 'id' and optional 'filter'

    Returns:
        JSONResponse: Dictionary of node names to their configuration records (200),
        or error message (400) if device not found or invalid parameters

    Raises:
        ValueError: When required 'id' query parameter is missing
        KeyError: When device with specified ID doesn't exist
        Exception: For other processing errors or invalid data types
    """

    logger = LoggerManager.get_logger(__name__)

    try:

        id_raw = request.query_params.get("id")
        filter_str = request.query_params.get("filter")  # Optional

        if not id_raw:
            raise ValueError("Missing required query parameters: 'id'")

        device = device_manager.get_device(int(id_raw))

        if not device:
            raise KeyError(f"Device with id {id_raw!r} does not exist.")

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

    except Exception as e:
        logger.error(f"Failed to get device nodes configuration for device with id {id_raw!r}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.get("/get_logs_from_node")
async def get_logs_from_node(device_manager: DeviceManager, timedb: TimeDBClient, request: Request) -> JSONResponse:
    """
    Retrieves historical log data for a specific node within an optional time range.

    Args:
        device_manager: DeviceManager instance for device and node validation
        timedb: TimeDBClient instance for querying historical measurement data
        request: FastAPI request with JSON containing device ID, node name, and optional time range

    Returns:
        JSONResponse: Historical measurement data for the specified node (200),
        or error message (400) if device/node not found or invalid parameters

    Raises:
        ValueError: When required fields ('id', 'node') are missing
        KeyError: When device or node with specified names/IDs don't exist
        Exception: For time parsing errors, database issues, or other processing errors
    """

    logger = LoggerManager.get_logger(__name__)
    data: Dict[str, Any] = {}

    try:
        data = await request.json()

        id_raw = data.get("id")
        node_name = data.get("node")
        start_time_str = data.get("start_time")
        end_time_str = data.get("end_time")

        if not all([id_raw, node_name]):
            raise ValueError("Missing one or more required fields: 'id', 'node'")

        # Optional time range parsing
        start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
        end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

        device = device_manager.get_device(int(id_raw))

        if not device:
            raise KeyError(f"Device with id {id_raw!r} does not exist.")

        if not any(node_name == node.name for node in device.nodes):
            raise KeyError(f"Node with name {node_name} does not exist in device {device.name if device else 'not found'} with id {id_raw!r}")

        response = timedb.get_measurement_data_between(
            device_name=device.name, device_id=int(id_raw), measurement=node_name, start_time=start_time, end_time=end_time
        )
        return JSONResponse(content=response)

    except Exception as e:
        logger.error(
            f"Failed to retrieve logs for device '{device.name if device else 'not found'}' with id {id_raw!r}, " f"node '{data.get('node', 'unknown')}': {e}"
        )
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.delete("/delete_logs_from_node")
async def delete_logs_from_node(
    safety: HTTPSafety, device_manager: DeviceManager, timedb: TimeDBClient, request: Request, authorization: str = Header(None)
) -> JSONResponse:
    """
    Deletes all historical log data for a specific node after authentication.

    Args:
        safety: HTTPSafety instance for token validation and rate limiting
        device_manager: DeviceManager instance for device and node validation
        timedb: TimeDBClient instance for deleting measurement data
        request: FastAPI request with JSON containing device ID and node name
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message with deletion confirmation (200),
        or error message (400) if validation fails, unauthorized, or deletion fails

    Raises:
        ValueError: When required fields ('id', 'node') are missing
        KeyError: When device or node with specified names/IDs don't exist
        Exception: For authentication errors, database issues, or deletion failures
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host
    data: Dict[str, Any] = {}

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        safety.check_authorization_token(authorization, request)

        data = await request.json()

        id_raw = data.get("id")
        node_name = data.get("node")

        if not all([id_raw, node_name]):
            raise ValueError("Missing one or more required fields: 'id', 'node'")

        device = device_manager.get_device(int(id_raw))
        if not device:
            raise KeyError(f"Device with id {id_raw!r} does not exist.")

        if not any(node_name == node.name for node in device.nodes):
            raise KeyError(f"Node with name {node_name} does not exist in device {device.name if device else 'not found'} with id {id_raw!r}.")

        result = timedb.delete_measurement_data(device_name=device.name, device_id=int(id_raw), measurement=node_name)

        if result:
            message = f"Successfully deleted logs for node '{node_name}' from device '{device.name}' with id {id_raw!r}."
            safety.clean_failed_requests(ip, request.url.path)
            return JSONResponse(content={"result": message})
        else:
            raise Exception(f"Could not delete logs for node '{node_name}' from device '{device.name}' with id {id_raw!r}.")

    except Exception as e:

        safety.increment_failed_requests(ip, request.url.path)
        logger.error(
            f"Failed to delete logs for device '{device.name if device else 'not found'}' with id {data.get('id', 'unknown')}, "
            f"measurement '{data.get('measurement', 'unknown')}': {e}"
        )
        return JSONResponse(status_code=400, content={"error": str(e)})


@router.delete("/delete_all_logs")
async def delete_all_logs(safety: HTTPSafety, timedb: TimeDBClient, request: Request, authorization: str = Header(None)) -> JSONResponse:
    """
    Deletes entire database of historical logs for a device after authentication.

    Args:
        safety: HTTPSafety instance for token validation and rate limiting
        timedb: TimeDBClient instance for database deletion operations
        request: FastAPI request with JSON containing device name and ID
        authorization: Authorization header with Bearer token for authentication

    Returns:
        JSONResponse: Success message with deletion confirmation (200),
        or error message (400) if validation fails, unauthorized, or deletion fails

    Raises:
        ValueError: When required fields ('name', 'id') are missing
        Exception: For authentication errors, database issues, or deletion failures
    """

    logger = LoggerManager.get_logger(__name__)
    ip = request.client.host
    data: Dict[str, Any] = {}

    try:
        if safety.is_blocked(ip, request.url.path):
            return JSONResponse(status_code=400, content={"error": "Too many failed attempts. Try again later."})

        safety.check_authorization_token(authorization, request)

        data = await request.json()

        name = data.get("name")
        id_raw = data.get("id")

        if not all([name, id_raw]):
            raise ValueError("Missing one or more required fields: 'name', 'id'.")

        result = timedb.delete_db(device_name=name, device_id=int(id_raw))

        if result:
            message = f"Successfully deleted all logs from device '{name}' with id {id_raw!r}."
            safety.clean_failed_requests(ip, request.url.path)
            return JSONResponse(content={"result": message})
        else:
            raise Exception(f"Could not delete all logs from from device '{name}' with id {id_raw!r}.")

    except Exception as e:
        safety.increment_failed_requests(ip, request.url.path)
        logger.error(f"Failed to delete all logs for device {data.get('name', 'unknown')} with id {id_raw!r}: {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})
