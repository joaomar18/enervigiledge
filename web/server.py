###########EXTERNAL IMPORTS############

import asyncio
import logging
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from web.dependencies import services
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient
import web.api.auth as auth
import web.api.device as device
import web.api.nodes as nodes
from util.debug import LoggerManager

#######################################


class HTTPServer:
    """
    Asynchronous HTTP server built with FastAPI for comprehensive energy meter device management and monitoring.

    This server orchestrates multiple API modules to provide a secure REST API for managing energy meter devices,
    retrieving real-time and historical data, handling user authentication, and protecting sensitive operations
    through token validation and request security.

    Architecture:
        - Built on FastAPI with asynchronous request handling for high performance
        - Modular design with separate API routers for different functionalities
        - Uses JWT-based authentication with session management
        - Implements security features including IP-based blocking and password policies
        - Integrates with SQLite for device configuration and InfluxDB for time-series data
        - Supports CORS for web frontend integration

    Core Responsibilities:
        - API Orchestration: Registers and manages separate API module routers
        - Component Integration: Connects DeviceManager, databases, and security systems
        - Server Lifecycle: Handles startup, configuration, and shutdown procedures
        - Middleware Management: Configures CORS and other cross-cutting concerns

    Components:
        - device_manager (DeviceManager): Manages device lifecycle, validation, and real-time data
        - db (SQLiteDBClient): Handles device configuration persistence
        - timedb (TimeDBClient): Manages time-series data queries and operations
        - safety (HTTPSafety): Implements security policies and token validation
        - server (FastAPI): Core web application with registered API routers

    API Modules:
        - auth_api: Authentication and authorization endpoints (login, logout, password management)
        - device_api: Device lifecycle management endpoints (add, edit, delete, status)
        - nodes_api: Node operations and data retrieval endpoints (state, logs, configuration)

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
        - Modular API design for maintainable and scalable endpoint management
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, db: SQLiteDBClient, timedb: TimeDBClient):
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.safety = HTTPSafety()
        services.set_dependencies(self.safety, self.device_manager, self.db, self.timedb)  # Set dependencies for routers endpoints
        self.server = FastAPI()
        self.server.include_router(auth.router)  # Authorization router (handles authorization endpoints)
        self.server.include_router(device.router)  # Device router (handles device endpoints)
        self.server.include_router(nodes.router)  # Nodes router (handles nodes endpoints)
        self.server.add_middleware(CORSMiddleware, allow_origins=["http://localhost:8080"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
        self.run_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """
        Starts the HTTP server asynchronously using the current event loop.

        This method creates a background task that runs the FastAPI server using `asyncio.create_task`.
        It should be called once during initialization or startup of the HTTP server component.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.run_task is not None:
                raise RuntimeError("Run task is already instantiated")

            loop = asyncio.get_event_loop()
            self.run_task = loop.create_task(self.run_server())
        except Exception as e:
            logger.exception(f"Failed to start HTTP Server: {str(e)}")

    async def stop(self) -> None:
        """
        Stops the HTTP Server by cancelling the run task.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.run_task:
                self.run_task.cancel()
                await self.run_task
                self.run_task = None

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Failed to stop HTTP Server: {str(e)}")

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
