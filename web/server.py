###########EXTERNAL IMPORTS############

import asyncio
import logging
from typing import Optional
from fastapi import FastAPI, APIRouter
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
from app_config import IS_DEVELOPMENT

#######################################


class HTTPServer:
    """
    Asynchronous HTTP server built with FastAPI for comprehensive energy meter device management and monitoring.

    This server orchestrates multiple API modules to expose a secure REST interface for managing energy meter devices,
    retrieving real-time and historical data, and handling user authentication and authorization. It is designed
    for edge deployment scenarios, prioritizing clear separation of concerns, predictable lifecycle behavior, and
    secure communication.

    Architecture:
        - Built on FastAPI with asynchronous request handling
        - Modular design with feature-specific routers composed under a common `/api` namespace
        - Clear separation between API endpoints and user interface routing
        - Designed to operate behind a reverse proxy that terminates HTTPS
        - Development and deployment concerns explicitly separated

    Core Responsibilities:
        - API Orchestration: Registers and exposes all backend functionality under the `/api` prefix
        - Component Integration: Connects the DeviceManager, databases, and security services
        - Server Lifecycle: Manages startup, execution, and graceful shutdown of the HTTP server
        - Middleware Management: Enables development-only middleware such as CORS when required

    Components:
        - device_manager (DeviceManager): Manages device lifecycle, validation, and real-time data acquisition
        - db (SQLiteDBClient): Handles persistent storage of device configuration
        - timedb (TimeDBClient): Manages time-series data storage and queries
        - safety (HTTPSafety): Implements authentication, authorization, and request security policies
        - server (FastAPI): Core web application composed of modular API routers

    API Structure:
        - All backend endpoints are exposed under the `/api` namespace
        - auth_api: Authentication and authorization endpoints (`/api/auth/*`)
        - device_api: Device lifecycle management endpoints (`/api/device/*`)
        - nodes_api: Node data and state endpoints (`/api/nodes/*`)

    Security Features:
        - Token-based authentication with session management
        - IP-based request blocking for brute-force attack mitigation
        - Password policy enforcement
        - Automatic session expiration
        - Centralized security checks applied across API routers

    Configuration:
        - CORS enabled only in development to allow a decoupled frontend during local development
        - Intended deployment behind a reverse proxy providing HTTPS termination
        - Uvicorn-based ASGI server with configurable host and port
        - Asynchronous execution using the asyncio event loop
        - Structured logging via LoggerManager

    Usage:
        The server is intended for local or edge deployments where a single administrator manages
        energy meter infrastructure. Core data acquisition and logging services operate independently
        of user interface access, ensuring that UI activity does not interfere with system operation.

    Notes:
        - Authentication and configuration endpoints are isolated under the `/api` boundary
        - CORS is disabled in deployment environments where a single-origin reverse proxy is used
        - The design favors explicit configuration over implicit environment detection
        - Modular API structure supports maintainability and future extension
    """

    def __init__(self, host: str, port: int, device_manager: DeviceManager, db: SQLiteDBClient, timedb: TimeDBClient) -> None:
        self.host = host
        self.port = port
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.safety = HTTPSafety()
        services.set_dependencies(self.safety, self.device_manager, self.db, self.timedb)  # Set dependencies for routers endpoints
        self.server = FastAPI()
        api_router = APIRouter(prefix="/api")
        api_router.include_router(auth.router)  # Authorization router (handles authorization endpoints)
        api_router.include_router(device.router)  # Device router (handles device endpoints)
        api_router.include_router(nodes.router)  # Nodes router (handles nodes endpoints)
        self.server.include_router(api_router)
        if IS_DEVELOPMENT:
            self.server.add_middleware(
                CORSMiddleware,
                allow_origins=["http://localhost:8080, http://127.0.0.1:8080"],
                allow_credentials=True,
                allow_methods=["GET", "POST", "PUT", "DELETE"],
                allow_headers=["Authorization", "Content-Type"],
            )
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
            await self.safety.start_cleanup_task()
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
            await self.safety.stop_cleanup_task()

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

        config = Config(app=self.server, host=self.host if IS_DEVELOPMENT else "127.0.0.1", port=self.port, reload=False, log_level=logging.CRITICAL + 1)
        server = Server(config)
        await server.serve()
