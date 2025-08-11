###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager

#######################################


@dataclass
class Measurement:
    """
    Represents a measurement batch to be written to the time-series database.

    Attributes:
        db (str): The name of the database where the data will be stored.
        data (List[Dict[str, Any]]): A list of data points to be written.
    """

    db: str
    data: List[Dict[str, Any]]


class TimeDBClient:
    """
    Asynchronous client interface for interacting with an InfluxDB time-series database.

    This class provides functionality for:
        - Writing measurement data to InfluxDB using a background task and write queue.
        - Querying measurement data between optional time ranges.
        - Deleting historical data for specific measurements.
        - Listing available measurements and series in a given database.
        - Checking for the existence of a database.

    Features:
        - Converts structured measurement dictionaries into InfluxDB's required format.
        - Automatically creates databases if they don't exist before writing.
        - Handles serialization of timestamps into human-readable tags.
        - Supports concurrent write operations via an asyncio-based queue.

    Intended for use in industrial monitoring and logging applications where devices publish
    structured time-series data.

    Attributes:
        client (InfluxDBClient): Connection to the InfluxDB server.
        write_queue (asyncio.Queue): Queue used for async write tasks.
    """

    @staticmethod
    def to_db_format(data: List[Dict[str, Any]]) -> List[Dict[str, Any]] | None:
        """
        Converts a list of structured measurement dictionaries into InfluxDB write format.

        Each item in the input list must include:
            - name (str): The measurement name.
            - unit (str): The unit of the measurement
            - start_time (datetime): Start of the logging period.
            - end_time (datetime): Timestamp for when the log ends.

        Any additional keys will be treated as InfluxDB fields.

        Args:
            data (List[Dict[str, Any]]): Raw measurement data from nodes.

        Returns:
            List[Dict[str, Any]] | None: InfluxDB-compatible points ready to write.
            If measurement values are None returns None

        Raises:
            ValueError: If any required key is missing in a data item.
        """

        formatted = []

        for item in data:
            if not all(k in item for k in ("name", "unit", "start_time", "end_time")):
                raise ValueError(f"Missing required fields in data item: {item}")

            name: str = item["name"]
            unit: str = item["unit"]
            start_time: datetime = item["start_time"]
            end_time: datetime = item["end_time"]

            formatted_start = start_time.strftime("%Y-%m-%d %H:%M")
            formatted_end = end_time.strftime("%Y-%m-%d %H:%M")

            end_time_trimmed = end_time.replace(second=0, microsecond=0)

            fields = {k: v for k, v in item.items() if k not in ("name", "unit", "start_time", "end_time") and v is not None}

            tags = {"unit": unit, "start_time": formatted_start, "end_time": formatted_end}

            if not fields:
                return None

            formatted.append({"measurement": name, "fields": fields, "tags": tags, "time": end_time_trimmed})

        return formatted

    def __init__(self, host: str = "localhost", port: int = 8086, username: str = "root", password: str = "root"):

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client: Optional[InfluxDBClient] = None
        self.write_queue: asyncio.Queue[Measurement] = asyncio.Queue(maxsize=1000)
        self.write_task: Optional[asyncio.Task] = None

    async def init_connection(self) -> None:
        """
        Initiates the InfluxDB connection.
        Should be called during application initialization.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            loop = asyncio.get_event_loop()
            if self.client is not None or self.write_task is not None:
                raise RuntimeError("InfluxDB connection or write task are already instantiated")
            self.client = InfluxDBClient(host=self.host, port=self.port, username=self.username, password=self.password)
            self.write_task: asyncio.Task = loop.create_task(self.db_writer())
        except Exception as e:
            logger.exception(f"Failed to initiate InfluxDB connecion: {e}")

    async def close_connection(self):
        """
        Closes the InfluxDB connection.
        Should be called during application shutdown.
        """

        logger = LoggerManager.get_logger(__name__)

        try:

            if self.write_task:
                self.write_task.cancel()
                await self.write_task
                self.write_task = None

            if self.client:
                self.client.close()
                self.client = None

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Failed to close InfluxDB connection: {e}")

    async def db_writer(self):
        """
        Continuously processes queued measurement data and writes it to InfluxDB.

        This coroutine runs indefinitely, retrieving `Measurement` objects from the `write_queue`
        and passing them to `write_data()` for persistence in the database.

        Notes:
            - Exceptions during processing are caught and logged without crashing the loop.
        """

        logger = LoggerManager.get_logger(__name__)

        while True:
            try:
                measurement: Measurement = await self.write_queue.get()
                await self.write_data(measurement)

            except Exception as e:
                logger.exception(f"Write Task: {e}")

            await asyncio.sleep(0)

    async def write_data(self, measurement: Measurement) -> bool:
        """
        Writes a single measurement batch to InfluxDB.

        This method checks if the database exists (creates it if necessary),
        formats the data using `to_db_format()`, and then writes it.

        Args:
            measurement (Measurement): A dataclass containing the database name and a list of data points.

        Returns:
            bool: True if the write was successful, False if an error occurred.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if {"name": measurement.db} not in self.client.get_list_database():
                self.client.create_database(measurement.db)

            db_data = TimeDBClient.to_db_format(measurement.data)

            if db_data:
                self.client.write_points(points=db_data, database=measurement.db)

            return True

        except Exception as e:
            logger.exception(f"Failed to write data to DB '{measurement.db}': {e}")
            return False

    def get_measurement_data_between(
        self, device_name: str, device_id: int, measurement: str, start_time: datetime = None, end_time: datetime = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieves measurement data from InfluxDB for a given device and node.

        If `start_time` and `end_time` are provided, the query will be time-bounded.
        Otherwise, it will return all available entries for the measurement.

        Args:
            device_name (str): Name of the device (used to construct the database name).
            device_id (int): ID of the device.
            measurement (str): The name of the measurement (usually the node name).
            start_time (datetime, optional): Start of the time range.
            end_time (datetime, optional): End of the time range.

        Returns:
            List[Dict[str, Any]]: A list of data points, with the time field removed.

        Raises:
            ValueError: If the database does not exist, only one time bound is provided,
            or end_time is earlier than start_time.
        """

        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            raise ValueError(f"Database '{db_name}' does not exist.")

        if (start_time and not end_time) or (end_time and not start_time):
            raise ValueError("Both 'start_time' and 'end_time' must be provided together.")

        if start_time and end_time and end_time < start_time:
            raise ValueError("'end_time' must not be earlier than 'start_time'.")

        self.client.switch_database(db_name)

        if start_time and end_time:
            start_str = start_time.isoformat() + "Z"
            end_str = end_time.isoformat() + "Z"
            query = f'SELECT * FROM "{measurement}" WHERE time >= \'{start_str}\' AND time <= \'{end_str}\''
        else:
            query = f'SELECT * FROM "{measurement}"'

        result = self.client.query(query)
        raw_data = list(result.get_points())

        for entry in raw_data:
            entry.pop("time", None)

        return raw_data

    def delete_measurement_data(self, device_name: str, device_id: int, measurement: str) -> bool:
        """
        Deletes all data points for a specific measurement in the corresponding device database.

        Args:
            device_name (str): Name of the device (used to build the database name).
            device_id (int): Unique ID of the device.
            measurement (str): Name of the measurement (typically the node name) to delete.

        Returns:
            bool: True if the deletion was successful, False if an error occurred during execution.

        Raises:
            ValueError: If the database for the specified device does not exist.
        """

        logger = LoggerManager.get_logger(__name__)

        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            raise ValueError(f"Database '{db_name}' does not exist.")

        try:
            self.client.switch_database(db_name)
            self.client.query(f'DELETE FROM "{measurement}"')
            return True

        except Exception as e:
            logger.exception(f"Failed to delete measurement '{measurement}' from DB '{db_name}': {e}")
            return False

    def delete_db(self, device_name: str, device_id: int) -> bool:
        """
        Deletes the entire InfluxDB database associated with a specific device.

        The database name is constructed as "<device_name>_<device_id>". This operation will
        permanently remove all measurements and data associated with the device.

        Args:
            device_name (str): The name of the device.
            device_id (int): The unique ID of the device.

        Returns:
            bool: True if the database was successfully deleted, False otherwise.

        Raises:
            ValueError: If the specified database does not exist.
        """

        logger = LoggerManager.get_logger(__name__)

        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            raise ValueError(f"Database '{db_name}' does not exist.")

        try:
            self.client.drop_database(db_name)
            return True
        except Exception as e:
            logger.exception(f"Failed to delete DB '{db_name}': {e}")
            return False

    def check_db_exists(self, db: str) -> bool:
        """
        Checks whether a given InfluxDB database exists.

        Args:
            db (str): The name of the database.

        Returns:
            bool: True if the database exists, False otherwise.
        """

        return {"name": db} in self.client.get_list_database()
