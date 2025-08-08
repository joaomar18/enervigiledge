###########EXTERNAL IMPORTS############

import asyncio
import logging
import json
from fastapi import FastAPI, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from uvicorn import Config, Server
from datetime import datetime
from typing import Dict, Set, Optional, Any

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
import web.login_api as login_api
import web.device_api as device_api
import web.nodes_api as nodes_api
from util.debug import LoggerManager
from util.functions import process_and_save_image, get_device_image, delete_device_image
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from controller.conversion import convert_dict_to_energy_meter

#######################################


class HTTPServer:
    """
    Asynchronous HTTP server built with FastAPI for secure and efficient management of energy meter devices.

    This server provides a REST API for managing devices, retrieving real-time and historical data,
    handling secure user authentication, and protecting sensitive operations through token validation
    and request rate limiting.

    Core Responsibilities:
        - Manage a set of energy meters and their nodes via the DeviceManager.
        - Serve historical log data through integration with a TimeDB (InfluxDB) client.
        - Authenticate users using a one-time credential file and JWT-based sessions.
        - Secure endpoints using IP-based blocking after repeated failed attempts.
        - Expose RESTful endpoints for device state, log access, and critical operations (e.g., password change, log deletion).

    Components:
        - `device_manager` (DeviceManager): Interface for device registration, validation, and data retrieval.
        - `timedb` (TimeDBClient): Used to query, filter, and delete time-series logs.
        - `safety` (HTTPSafety): Handles security logic including token validation, password rules, and failed request tracking.
        - `server` (FastAPI): FastAPI application that registers and serves HTTP endpoints.
        - Runs as a background task on instantiation using asyncio's event loop and Uvicorn.

    Endpoints:
        - `POST /login`: Authenticates a user and returns a JWT token.
        - `POST /logout`: Invalidates the session token.
        - `POST /create_login`: Creates the initial credential config with hashed password and signing key.
        - `POST /change_password`: Securely updates the stored password (requires current credentials and token).
        - `GET /get_device_state`: Returns the state metadata of a specified device.
        - `GET /get_all_device_state`: Lists all currently active device states.
        - `GET /get_nodes_state`: Lists node values of a given device, with optional filtering.
        - `GET /get_logs`: Retrieves historical logs from a specific node.
        - `DELETE /delete_logs`: Deletes logs for a specific node on a device.
        - `DELETE /delete_all_logs`: Wipes all logs associated with a device.

    Security Features:
        - JWT-based authentication with in-memory token tracking.
        - IP-based request blocking for brute-force protection.
        - Password policy enforcement (minimum length, non-whitespace).
        - Token/session validation for all sensitive routes.
        - Detailed logging via LoggerManager for auditability and debugging.

    Notes:
        - Only one user is supported (admin-level) to simplify local/edge deployments.
        - The server is intended to be deployed as a local configuration and monitoring endpoint.
        - Authentication is mandatory for any operation that alters or deletes data.
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, db: SQLiteDBClient, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.safety = HTTPSafety()
        self.server = FastAPI()
        self.server.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
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

        ##########     L O G I N     E N D P O I N T S     ##########

        @self.server.post("/auto_login")
        async def auto_login(request: Request):
            return await login_api.auto_login(self.safety, request)

        @self.server.post("/login")
        async def login(request: Request):
            return await login_api.login(self.safety, request)

        @self.server.post("/logout")
        async def logout(request: Request = None, authorization: str = Header(None)):
            return await login_api.logout(self.safety, request, authorization)

        @self.server.post("/create_login")
        async def create_login(request: Request):
            return await login_api.create_login(self.safety, request)

        @self.server.post("/change_password")
        async def change_password(request: Request, authorization: str = Header(None)):
            return await login_api.change_password(self.safety, request, authorization)

        ##########     D E V I C E     E N D P O I N T S     ##########

        @self.server.post("/add_device")
        async def add_device(request: Request, authorization: str = Header(None)):
            return await device_api.add_device(self.safety, self.device_manager, self.db, request, authorization)

        @self.server.post("/edit_device")
        async def edit_device(request: Request, authorization: str = Header(None)):
            return await device_api.edit_device(self.safety, self.device_manager, self.db, request, authorization)

        @self.server.delete("/delete_device")
        async def delete_device(request: Request, authorization: str = Header(None)):
            return await device_api.delete_device(self.safety, self.device_manager, self.db, request, authorization)

        @self.server.get("/get_device_state")
        async def get_device_state(request: Request):
            return await device_api.get_device_state(self.device_manager, request)

        @self.server.get("/get_all_device_state")
        async def get_all_device_state():
            return await device_api.get_all_devices_state(self.device_manager)

        @self.server.get("/get_default_image")
        async def get_default_image():
            return await device_api.get_default_image()

        ##########     N O D E S     E N D P O I N T S     ##########

        @self.server.get("/get_nodes_state")
        async def get_nodes_state(request: Request):
            return await nodes_api.get_nodes_state(self.device_manager, request)

        @self.server.get("/get_nodes_config")
        async def get_nodes_config(request: Request):
            return await nodes_api.get_nodes_config(self.device_manager, request)

        @self.server.get("/get_logs_from_node")
        async def get_logs_from_node(request: Request):
            return await nodes_api.get_logs_from_node(self.device_manager, self.timedb, request)

        @self.server.delete("/delete_logs_from_node")
        async def delete_logs_from_node(request: Request, authorization: str = Header(None)):
            return await nodes_api.delete_logs_from_node(self.safety, self.device_manager, self.timedb, request, authorization)

        @self.server.delete("/delete_all_logs")
        async def delete_all_logs(request: Request, authorization: str = Header(None)):
            return await nodes_api.delete_all_logs(self.safety, self.timedb, request, authorization)
