"""
Dependency injection module for FastAPI HTTP server components.

This module provides a centralized dependency injection container for managing
service layer components used throughout the FastAPI application. It implements
the dependency injection pattern to ensure clean separation of concerns and
facilitate testing.

Classes:
    HTTPDependencies: Main dependency injection container for HTTP server services

Module Variables:
    services: Global HTTPDependencies instance for application-wide dependency access

Usage:
    # In server initialization
    from web.dependencies import services
    services.set_dependencies(safety, device_manager, db, timedb)

    # In FastAPI route handlers
    from fastapi import Depends

    @router.post("/endpoint")
    async def handler(safety: HTTPSafety = Depends(services.get_safety)):
        ...
"""

###########EXTERNAL IMPORTS############

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient

#######################################


class HTTPDependencies:
    """
    Dependency injection container for HTTP server service components.

    This class manages the lifecycle and access to core service components
    used by the FastAPI HTTP server. It provides a centralized way to inject
    dependencies into route handlers and other components.

    The container follows a singleton-like pattern where dependencies are
    initialized once during server startup and then accessed throughout
    the application lifecycle via getter methods.

    Attributes:
        safety (HTTPSafety | None): Security and authentication service
        device_manager (DeviceManager | None): Device lifecycle management service
        db (SQLiteDBClient | None): SQLite database client for configuration data
        timedb (TimeDBClient | None): InfluxDB client for time-series data
    """

    def __init__(
        self,
        safety: Optional[HTTPSafety] = None,
        device_manager: Optional[DeviceManager] = None,
        db: Optional[SQLiteDBClient] = None,
        timedb: Optional[TimeDBClient] = None,
    ):

        self.safety = safety
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb

    def set_dependencies(self, safety: HTTPSafety, device_manager: DeviceManager, db: SQLiteDBClient, timedb: TimeDBClient):
        """
        Set all dependency instances at once during application startup.

        This method is typically called once during server initialization
        to populate the container with all required service instances.

        Args:
            safety: HTTPSafety instance for authentication and security
            device_manager: DeviceManager instance for device operations
            db: SQLiteDBClient instance for configuration persistence
            timedb: TimeDBClient instance for time-series data operations
        """
        self.safety = safety
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb

    def get_safety(self) -> HTTPSafety:
        """
        Get the HTTPSafety service instance.

        Returns:
            HTTPSafety: Configured security and authentication service

        Raises:
            ValueError: If HTTPSafety has not been initialized
        """
        if self.safety:
            return self.safety
        raise ValueError("HTTP Safety is not yet initialized in HTTP Dependencies")

    def get_device_manager(self) -> DeviceManager:
        """
        Get the DeviceManager service instance.

        Returns:
            DeviceManager: Configured device lifecycle management service

        Raises:
            ValueError: If DeviceManager has not been initialized
        """
        if self.device_manager:
            return self.device_manager
        raise ValueError("Device Manager is not yet initialized in HTTP Dependencies")

    def get_db(self) -> SQLiteDBClient:
        """
        Get the SQLiteDBClient service instance.

        Returns:
            SQLiteDBClient: Configured SQLite database client

        Raises:
            ValueError: If SQLiteDBClient has not been initialized
        """
        if self.db:
            return self.db
        raise ValueError("SQlite DB is not yet initialized in HTTP Dependencies")

    def get_timedb(self) -> TimeDBClient:
        """
        Get the TimeDBClient service instance.

        Returns:
            TimeDBClient: Configured InfluxDB time-series database client

        Raises:
            ValueError: If TimeDBClient has not been initialized
        """
        if self.timedb:
            return self.timedb
        raise ValueError("Time DB is not yet initialized in HTTP Dependencies")


services = HTTPDependencies()  # Global HTTPDependencies instance for application-wide dependency access.
