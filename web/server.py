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
    Asynchronous HTTP server built with FastAPI for comprehensive energy meter device management and monitoring.

    This server provides a secure REST API for managing energy meter devices, retrieving real-time and historical data,
    handling user authentication, and protecting sensitive operations through token validation and request security.

    Architecture:
        - Built on FastAPI with asynchronous request handling for high performance
        - Uses JWT-based authentication with session management
        - Implements security features including IP-based blocking and password policies
        - Integrates with SQLite for device configuration and InfluxDB for time-series data
        - Supports CORS for web frontend integration

    Core Responsibilities:
        - Device Management: Add, edit, delete, and monitor energy meter devices
        - Real-time Data: Serve current device states and node values
        - Historical Data: Query and manage time-series logs from InfluxDB
        - Authentication: Secure user login/logout with JWT tokens
        - Security: Rate limiting, IP blocking, and password policy enforcement
        - Image Management: Handle device images with fallback to default images

    Components:
        - device_manager (DeviceManager): Manages device lifecycle, validation, and real-time data
        - db (SQLiteDBClient): Handles device configuration persistence
        - timedb (TimeDBClient): Manages time-series data queries and operations
        - safety (HTTPSafety): Implements security policies and token validation
        - server (FastAPI): Core web application with registered endpoints

    API Endpoints:

        Authentication:
            - POST /auto_login: Automatic login for seamless user experience
            - POST /login: User authentication with credentials
            - POST /logout: Session termination and token invalidation
            - POST /create_login: Initial user account creation
            - POST /change_password: Secure password update

        Device Management:
            - POST /add_device: Register new energy meter devices
            - POST /edit_device: Update existing device configurations
            - DELETE /delete_device: Remove devices and associated data
            - GET /get_device_state: Retrieve specific device status and metadata
            - GET /get_all_device_state: List all active devices with current states
            - GET /get_default_image: Serve default device image for UI

        Node Operations:
            - GET /get_nodes_state: Fetch current values from device nodes
            - GET /get_nodes_config: Retrieve node configuration details
            - GET /get_logs_from_node: Query historical data for specific nodes
            - DELETE /delete_logs_from_node: Remove logs for individual nodes
            - DELETE /delete_all_logs: Bulk delete all logs for a device

    Security Features:
        - JWT-based authentication with secure token generation and validation
        - IP-based request blocking for brute-force attack protection
        - Password policy enforcement (minimum length, complexity requirements)
        - Session management with automatic token expiration
        - Request rate limiting and suspicious activity detection
        - Comprehensive audit logging for security monitoring

    Configuration:
        - CORS enabled for frontend integration (localhost:5173)
        - Uvicorn server with configurable host and port
        - Asynchronous operation using asyncio event loop
        - Structured logging via LoggerManager for debugging and monitoring

    Usage:
        The server is designed for edge deployment scenarios where a single administrator
        manages local energy meter infrastructure. It provides both programmatic API access
        and supports web-based frontends for device monitoring and configuration.

    Notes:
        - Single-user authentication model optimized for edge/local deployments
        - Automatic server startup via asyncio background task
        - Graceful error handling with appropriate HTTP status codes
        - Image processing capabilities for device visualization
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
