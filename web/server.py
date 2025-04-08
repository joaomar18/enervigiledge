###########EXTERNAL IMPORTS############

import asyncio
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from uvicorn import Config, Server
from datetime import datetime
from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.device import DeviceManager
from db.timedb import TimeDBClient

#######################################


class HTTPServer:
    """
    Asynchronous HTTP server built with FastAPI for interacting with device data and logs.

    This server provides endpoints to:
    - Retrieve historical log data for specific nodes (measurements) from TimeDB.
    - Delete historical log data for specific nodes.

    Key Components:
        - `device_manager`: Used to validate device existence and access node configurations.
        - `timedb`: Interface for querying and deleting data from the time-series database.
        - `FastAPI` server: Manages asynchronous HTTP requests.
        - `Uvicorn` server: Hosts the FastAPI application asynchronously within the event loop.

    Endpoints:
        - POST `/get_logs`: Retrieve logs for a specified device/node.
        - POST `/delete_logs`: Delete logs for a specified device/node.

    Notes:
        - The server is launched automatically as a background task on instantiation.
        - Log messages are managed via `LoggerManager` for centralized logging.
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.timedb = timedb
        self.server = FastAPI()
        self.setup_routes()
        self.start()

    def start(self) -> None:
        """
        Starts the HTTP server asynchronously using the current event loop.

        This method creates a background task that runs the FastAPI server using `asyncio.create_task`.
        It should be called once during initialization or startup of the HTTP server component.
        """

        loop = asyncio.get_event_loop()
        self.run_task = loop.create_task(self.run_server())

    async def run_server(self):
        """
        Asynchronously starts the FastAPI HTTP server using Uvicorn.

        This method builds a Uvicorn `Server` with the provided configuration:
            - Binds the server to the specified host and port.
            - Disables live reload.
            - Suppresses default logging output.

        It runs the server within the asyncio event loop.
        """

        config = Config(app=self.server, host=self.host, port=self.port, reload=False, log_level=logging.CRITICAL + 1)
        server = Server(config)
        await server.serve()

    def setup_routes(self):

        @self.server.get("/get_device_state")
        async def get_device_state(request: Request):
            """
            Endpoint to retrieve the current state of a device.

            Expects a JSON payload with:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.

            Validates the request payload and ensures the specified device exists. If valid,
            returns a JSON response with the current device state, including metadata, protocol,
            connection status, and configuration.

            Returns:
                JSONResponse:
                    - 200 OK with the device state if successful.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                return JSONResponse(content=device.get_device_state())

            except Exception as e:
                logger.error(f"Failed to get device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')} state: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_all_device_state")
        async def get_all_device_state():
            """
            Endpoint to retrieve the state of all registered devices.

            This endpoint returns a list of all device states currently managed by the DeviceManager.
            Each device state includes:
                - ID
                - Name
                - Protocol
                - Connection status
                - Meter options
                - Meter type

            Returns:
                JSONResponse:
                    - 200 OK with a list of device state dictionaries.
                    - 400 Bad Request with an error message if an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)

            try:
                all_states = [device.get_device_state() for device in self.device_manager.devices]
                return JSONResponse(content=all_states)
            except Exception as e:
                logger.error(f"Failed to retrieve all device states: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_nodes_state")
        async def get_nodes_state(request: Request):
            """
            Endpoint to retrieve the state of all nodes in a specific device.

            Expects a JSON payload with:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.
                - filter (str, optional): If provided, only return nodes whose names contain this string.

            For each node, it returns:
                - name
                - value
                - unit
                - alarms state

            Returns:
                JSONResponse:
                    - 200 OK with a list of node state dictionaries.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")
                filter_str = data.get("filter")  # Optional

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if filter_str:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes if filter_str in node.name}
                else:
                    nodes_state = {node.name: node.get_publish_format() for node in device.nodes}

                return JSONResponse(content=nodes_state)

            except Exception as e:
                logger.error(f"Failed to get node states for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.get("/get_logs")
        async def get_logs_from_measurement(request: Request):
            """
            Endpoint to retrieve logged measurement data for a specific device node.

            Expects a JSON payload with the following fields:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.
                - measurement (str): The name of the node to retrieve logs for.
                - start_time (str, optional): ISO datetime string (e.g., '2025-04-05 14:00').
                - end_time (str, optional): ISO datetime string (e.g., '2025-04-05 14:01').

            Validates that the device and node exist, then queries the time-series database
            for logs associated with the specified measurement.

            Returns:
                JSONResponse:
                    - 200 OK with the measurement data if successful.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")
                measurement = data.get("measurement")
                start_time_str = data.get("start_time")
                end_time_str = data.get("end_time")

                if not all([name, id, measurement]):
                    raise ValueError("Missing one or more required fields: 'name', 'id', 'measurement'.")

                # Optional time range parsing
                start_time = datetime.fromisoformat(start_time_str) if start_time_str else None
                end_time = datetime.fromisoformat(end_time_str) if end_time_str else None

                device = self.device_manager.get_device(name, id)

                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}")

                response = self.timedb.get_measurement_data_between(
                    device_name=name, device_id=id, measurement=measurement, start_time=start_time, end_time=end_time
                )
                return JSONResponse(content=response)

            except Exception as e:
                logger.error(
                    f"Failed to retrieve logs for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}, "
                    f"measurement '{data.get('measurement', 'unknown')}': {e}"
                )
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/delete_logs")
        async def delete_logs_from_measurement(request: Request):
            """
            Endpoint to delete log data for a specific node from a device.

            Expects a JSON payload with the following fields:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.
                - measurement (str): The name of the node to delete logs for.

            Returns:
                JSONResponse:
                    - 200 OK with success/failure message.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """
            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")
                measurement = data.get("measurement")

                if not all([name, id, measurement]):
                    raise ValueError("Missing one or more required fields: 'name', 'id', 'measurement'.")

                device = self.device_manager.get_device(name, id)
                if not device:
                    raise KeyError(f"Device with name {name} and id {id} does not exist.")

                if not any(measurement == node.name for node in device.nodes):
                    raise KeyError(f"Node with name {measurement} does not exist in device {name} with id {id}.")

                result = self.timedb.delete_measurement_data(device_name=name, device_id=id, measurement=measurement)

                message = (
                    f"Successfully deleted logs for node '{measurement}' from device '{name}' (id {id})."
                    if result
                    else f"Failed to delete logs for node '{measurement}' from device '{name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                logger.error(
                    f"Failed to delete logs for device '{data.get('name', 'unknown')}' with id {data.get('id', 'unknown')}, "
                    f"measurement '{data.get('measurement', 'unknown')}': {e}"
                )
                return JSONResponse(status_code=400, content={"error": str(e)})

        @self.server.post("/delete_all_logs")
        async def delete_all_logs(request: Request):
            """
            Endpoint to delete all logging data from a device.

            Expects a JSON payload with the following fields:
                - name (str): The name of the device.
                - id (int): The unique ID of the device.

            Returns:
                JSONResponse:
                    - 200 OK with success/failure message.
                    - 400 Bad Request with an error message if validation fails or an exception occurs.
            """

            logger = LoggerManager.get_logger(__name__)
            data: Dict[str, Any] = {}

            try:
                data = await request.json()
                name = data.get("name")
                id = data.get("id")

                if not all([name, id]):
                    raise ValueError("Missing one or more required fields: 'name', 'id'.")

                result = self.timedb.delete_db(device_name=name, device_id=id)

                message = (
                    f"Successfully deleted all logs from device '{name}' (id {id})." if result else f"Failed to delete logs from device '{name}' (id {id})."
                )
                return JSONResponse(content={"result": message})

            except Exception as e:
                logger.error(f"Failed to delete all logs for device {data.get('name', 'unknown')} with id {data.get('id', 'unknown')}: {e}")
                return JSONResponse(status_code=400, content={"error": str(e)})
