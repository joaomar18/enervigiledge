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
        self.safety = safety
        self.device_manager = device_manager
        self.db = db
        self.timedb = timedb

    def get_safety(self) -> HTTPSafety:
        if self.safety:
            return self.safety
        raise ValueError("HTTP Safety is not yet initialized in HTTP Dependencies")

    def get_device_manager(self) -> DeviceManager:
        if self.device_manager:
            return self.device_manager
        raise ValueError("Device Manager is not yet initialized in HTTP Dependencies")

    def get_db(self) -> SQLiteDBClient:
        if self.db:
            return self.db
        raise ValueError("SQlite DB is not yet initialized in HTTP Dependencies")

    def get_timedb(self) -> TimeDBClient:
        if self.timedb:
            return self.timedb
        raise ValueError("Time DB is not yet initialized in HTTP Dependencies")


http_deps = HTTPDependencies()
