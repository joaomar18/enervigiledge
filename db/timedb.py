###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet
from typing import List, Dict, Any, Optional, Iterable, Iterator
from datetime import datetime
from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node
from controller.node.processor.numeric_processor import NumericNodeProcessor
import util.functions.date as date
import util.functions.calculation as calculation

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
            start_time: datetime = item["start_time"]
            end_time: datetime = item["end_time"]

            formatted_start = date.get_datestr_up_to_min(start_time)
            formatted_end = date.get_datestr_up_to_min(end_time)

            end_time_trimmed = date.remove_sec_precision(end_time)

            fields: dict[str, Any] = {k: v for k, v in item.items() if k not in ("name", "start_time", "end_time") and v is not None}
            fields["start_time"] = formatted_start
            fields["end_time"] = formatted_end

            if not fields:
                return None

            formatted.append({"measurement": name, "fields": fields, "time": end_time_trimmed})

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
            self.write_task = loop.create_task(self.db_writer())
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

    def __require_client(self) -> InfluxDBClient:
        """
        Return the active InfluxDB client connection.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.client is None:
            raise RuntimeError(f"InfluxDB client is not instantiated properly. ")
        return self.client

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
        client = self.__require_client()

        try:
            if not self.check_db_exists(measurement.db):
                client.create_database(measurement.db)

            db_data = TimeDBClient.to_db_format(measurement.data)

            if db_data:
                client.write_points(points=db_data, database=measurement.db)

            return True

        except Exception as e:
            logger.exception(f"Failed to write data to DB '{measurement.db}': {e}")
            return False

    def __iter_points(self, res: ResultSet | Iterable[ResultSet]) -> Iterator[Dict[str, Any]]:
        """
        Yield points from a single ResultSet or an iterable of ResultSet objects.
        """

        if isinstance(res, ResultSet):
            yield from res.get_points()
            return

        for rs in res:
            if not isinstance(rs, ResultSet):
                raise TypeError(f"Items must be ResultSet. Got type: {type(rs).__name__}")
            yield from rs.get_points()

    def __build_query_without_time_span(self, variable: Node) -> str:
        if isinstance(variable.processor, NumericNodeProcessor):
            unit_factor = f"{calculation.get_unit_factor(variable.config.unit)}"

            if not variable.config.incremental_node:
                query = f"""
                        SELECT "start_time", "end_time",
                        ("mean_sum" / "mean_count") / {unit_factor} AS average_value,
                        "min_value" / {unit_factor} AS min_value, "max_value" / {unit_factor} AS max_value
                        FROM "{variable.config.name}"
                        """.strip()
            else:
                query = f"""
                        SELECT "start_time", "end_time", "value" / {unit_factor} AS value
                        FROM "{variable.config.name}"
                        """.strip()
        else:
            query = f"""
                    SELECT "start_time", "end_time", "value"
                    FROM "{variable.config.name}"
                    """.strip()

        return query

    def __build_query_with_time_span_non_formatted(self, variable: Node, start_time_str: str, end_time_str: str) -> str:
        if isinstance(variable.processor, NumericNodeProcessor):
            unit_factor = f"{calculation.get_unit_factor(variable.config.unit)}"

            if not variable.config.incremental_node:
                query = f"""
                        SELECT "start_time", "end_time",
                        ("mean_sum" / "mean_count") / {unit_factor} AS average_value,
                        "min_value" / {unit_factor} AS min_value, "max_value" / {unit_factor} AS max_value
                        FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        """.strip()
            else:
                query = f"""
                        SELECT "start_time", "end_time", "value" / {unit_factor} AS value
                        FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        """.strip()
        else:
            query = f"""
                    SELECT "start_time", "end_time", "value"
                    FROM "{variable.config.name}"
                    WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                    """.strip()

        return query

    def __build_query_with_time_span_formatted(self, variable: Node, start_time_str: str, end_time_str: str, time_step_ms: Optional[int]) -> str:
        if isinstance(variable.processor, NumericNodeProcessor):
            unit_factor = f"{calculation.get_unit_factor(variable.config.unit)}"

            if not variable.config.incremental_node:
                query = f"""SELECT FIRST("start_time"), LAST("end_time"), (SUM("mean_sum") / SUM("mean_count")) / {unit_factor} AS average_value,
                        MIN("min_value") / {unit_factor} AS min_value, MAX("max_value") / {unit_factor} AS max_value FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        GROUP BY time({time_step_ms}ms) FILL(null)"""
            else:
                query = f"""SELECT FIRST("start_time"), LAST("end_time"), (SUM("value")) / {unit_factor} FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        GROUP BY time({time_step_ms}ms) FILL(null)"""

        else:
            raise ValueError(f"Can't get logs from non numeric variables in formatted time spans")

        return query

    def __build_query_with_time_span(
        self, variable: Node, start_time_str: str, end_time_str: str, formatted: Optional[bool], time_step_ms: Optional[int]
    ) -> str:

        if not formatted:
            query = self.__build_query_with_time_span_non_formatted(variable, start_time_str, end_time_str)

        elif time_step_ms:
            query = self.__build_query_with_time_span_formatted(variable, start_time_str, end_time_str, time_step_ms)

        else:
            raise ValueError(f"Wrong parameters to get logs from node {variable.config.name}.")

        return query

    def __build_query(
        self, variable: Node, start_time: Optional[datetime], end_time: Optional[datetime], formatted: Optional[bool], time_step_ms: Optional[int]
    ) -> str:

        if start_time and end_time:
            start_time_str = start_time.isoformat() + "Z"
            end_time_str = end_time.isoformat() + "Z"
            query = self.__build_query_with_time_span(variable, start_time_str, end_time_str, formatted, time_step_ms)

        else:
            query = self.__build_query_without_time_span(variable)

        return query

    def __fill_buckets(self, variable: Node, points: List[Dict[str, Any]], start_time_ms, end_time_ms, time_step_ms) -> None:
        if not isinstance(variable.processor, NumericNodeProcessor):
            return

        current_start = start_time_ms
        while current_start + time_step_ms <= end_time_ms:
            start_date = date.get_date_from_timestamp(current_start)
            end_date = date.get_date_from_timestamp(current_start + time_step_ms)
            print(f"Start Time: {start_date}, End Time: {end_date}")
            current_start += time_step_ms

    def get_variable_logs_between(
        self,
        device_name: str,
        device_id: int,
        variable: Node,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        formatted: Optional[bool] = None,
        time_step_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:

        client = self.__require_client()
        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            client.create_database(db_name)

        if (start_time and not end_time) or (end_time and not start_time):
            raise ValueError("Both 'start_time' and 'end_time' must be provided together.")

        if start_time and end_time and end_time <= start_time:
            raise ValueError("'end_time' must be a later date than 'start_time'.")

        client.switch_database(db_name)
        query = self.__build_query(variable, start_time, end_time, formatted, time_step_ms)
        result = client.query(query)
        points = [{k: v for k, v in point.items()} for point in self.__iter_points(result)]
        if formatted and start_time and end_time and time_step_ms:
            self.__fill_buckets(variable, points, date.get_timestamp(start_time), date.get_timestamp(end_time), time_step_ms)
        return points

    def delete_variable_data(self, device_name: str, device_id: int, variable: Node) -> bool:

        logger = LoggerManager.get_logger(__name__)
        client = self.__require_client()

        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            raise ValueError(f"Database '{db_name}' does not exist.")

        try:
            client.switch_database(db_name)
            client.query(f'DELETE FROM "{variable.config.name}"')
            return True

        except Exception as e:
            logger.exception(f"Failed to delete measurement '{variable.config.name}' from DB '{db_name}': {e}")
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
        client = self.__require_client()

        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            raise ValueError(f"Database '{db_name}' does not exist.")

        try:
            client.drop_database(db_name)
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

        client = self.__require_client()
        return {"name": db} in client.get_list_database()
