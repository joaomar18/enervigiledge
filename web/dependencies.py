###########EXTERNAL IMPORTS############

from typing import Optional

#######################################

#############LOCAL IMPORTS#############

from web.safety import HTTPSafety
from controller.manager import DeviceManager
from db.db import SQLiteDBClient
from db.timedb import TimeDBClient
from analytics.system import SystemMonitor

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
        system_monitor (SystemMonitor | None): System performance monitoring service
    """

    def __init__(
        self,
        safety: Optional[HTTPSafety] = None,
        device_manager: Optional[DeviceManager] = None,
        db: Optional[SQLiteDBClient] = None,
        timedb: Optional[TimeDBClient] = None,
        system_monitor: Optional[SystemMonitor] = None,
    ):

        self.safety = safety
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.system_monitor: Optional[SystemMonitor] = None

    def set_dependencies(
        self,
        safety: HTTPSafety,
        device_manager: DeviceManager,
        db: SQLiteDBClient,
        timedb: TimeDBClient,
        system_monitor: SystemMonitor,
    ) -> None:
        """
        Set all dependency instances at once during application startup.

        This method is typically called once during server initialization
        to populate the container with all required service instances.

        Args:
            safety: HTTPSafety instance for authentication and security
            device_manager: DeviceManager instance for device operations
            db: SQLiteDBClient instance for configuration persistence
            timedb: TimeDBClient instance for time-series data operations
            system_monitor: SystemMonitor instance for system performance monitoring
        """
        self.safety = safety
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb
        self.system_monitor = system_monitor

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

    def get_system_monitor(self) -> SystemMonitor:
        """
        Get the SystemMonitor service instance.

        Returns:
            SystemMonitor: Configured system performance monitoring service

        Raises:
            ValueError: If SystemMonitor has not been initialized
        """
        if self.system_monitor:
            return self.system_monitor
        raise ValueError("System Monitor is not yet initialized in HTTP Dependencies")


services = HTTPDependencies()  # Global HTTPDependencies instance for application-wide dependency access.
