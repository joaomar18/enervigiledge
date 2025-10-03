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
        Converts structured measurement dictionaries into InfluxDB write format.

        Transforms measurement data from the application's internal format into the structure
        required by InfluxDB's write_points() method. This includes:
        - Extracting measurement names and timestamps
        - Formatting datetime fields as minute-precision strings
        - Converting measurement values to InfluxDB fields
        - Filtering out None values to prevent invalid writes
        - Using end_time as the primary timestamp for InfluxDB's time field

        Data Validation:
        - Ensures all required fields (name, unit, start_time, end_time) are present
        - Returns None if critical measurement values are None (prevents writing invalid data)
        - Skips non-essential fields that are None while preserving valid ones

        Args:
            data: List of measurement dictionaries containing:
                - name (str): Measurement name (becomes InfluxDB measurement)
                - start_time (datetime): Start of the logging period
                - end_time (datetime): End of the logging period (becomes InfluxDB timestamp)
                - Additional fields: Any measurement values (value, min_value, max_value, etc.)

        Returns:
            List[Dict[str, Any]] | None: InfluxDB-compatible points ready for write_points(),
            or None if any critical measurement values are None.

        Raises:
            ValueError: If any required field is missing from a data item.

        InfluxDB Format:
            Each returned dictionary contains:
            - "measurement": The series name
            - "fields": Dictionary of measurement values and formatted timestamps
            - "time": Primary timestamp (end_time with second precision removed)
        """

        formatted = []

        for item in data:
            if not all(k in item for k in ("name", "start_time", "end_time")):
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

            if "value" in fields:
                if fields.get("value") is None:
                    return None
            elif "min_value" in fields and "max_value" in fields:
                if fields.get("min_value") is None or fields.get("max_value") is None:
                    return None
            else:
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
                        SELECT "start_time", "end_time",
                        "value" / {unit_factor} AS value
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
                query = f"""SELECT FIRST("start_time") AS start_time, LAST("end_time") AS end_time,
                        (SUM("mean_sum") / SUM("mean_count")) / {unit_factor} AS average_value,
                        MIN("min_value") / {unit_factor} AS min_value, MAX("max_value") / {unit_factor} AS max_value FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        GROUP BY time({time_step_ms}ms) FILL(null)"""
            else:
                query = f"""SELECT FIRST("start_time") AS start_time, LAST("end_time") AS end_time,
                        (SUM("value")) / {unit_factor} AS value FROM "{variable.config.name}"
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

    def __formatted_post_processing(
        self, variable: Node, points: List[Dict[str, Any]], start_time_ms: int, end_time_ms: int, time_step_ms: int
    ) -> None:
        """
        Post-processes InfluxDB formatted query results to ensure proper time bucket alignment and completeness.

        InfluxDB's GROUP BY time() with FILL(null) has limitations:
        1. FIRST("start_time")/LAST("end_time") return original data timestamps, not bucket boundaries
        2. May not fill all buckets in the requested range if data is sparse

        This method fixes both issues by:
        - Mapping data points to their correct time bucket ends using ceiling division
        - Generating complete time series coverage with bucket end timestamps
        - Aligning start_time/end_time fields to actual bucket boundaries
        - Filling missing buckets with None values for all measurement fields

        The algorithm uses bucket end alignment where each bucket is defined by [start, end]:
        - Bucket boundaries: [start_time_ms, start_time_ms + time_step_ms], [start_time_ms + time_step_ms, start_time_ms + 2*time_step_ms], etc.
        - Data points are mapped to the bucket end that contains their timestamp using ceiling division
        - Expected buckets are generated as a sequence of bucket end timestamps

        Args:
            variable: Node configuration defining measurement type and structure
            points: List of data points from InfluxDB query (modified in-place)
            start_time_ms: Query start time in milliseconds since epoch
            end_time_ms: Query end time in milliseconds since epoch
            time_step_ms: Time bucket size in milliseconds
        """

        if not isinstance(variable.processor, NumericNodeProcessor):
            return

        for point in points:
            if point["start_time"] is not None and point["end_time"] is not None:
                current_step_ms = date.get_timestamp(datetime.fromisoformat(point["end_time"])) - date.get_timestamp(
                    datetime.fromisoformat(point["start_time"])
                )
                time_step_ms = max(time_step_ms, current_step_ms)

        expected_buckets: List[int] = []

        current_time = start_time_ms + time_step_ms
        while current_time <= end_time_ms:
            expected_buckets.append(current_time)
            current_time += time_step_ms

        existing_data: Dict[int, Dict[str, Any]] = {}

        for point in points:
            if point["end_time"] is not None:
                point_time_ms = date.get_timestamp(datetime.fromisoformat(point["end_time"]))
                bucket_end_ms = ((point_time_ms + time_step_ms - 1) // time_step_ms) * time_step_ms
                existing_data[bucket_end_ms] = point

        points.clear()

        for bucket_end_ms in expected_buckets:
            bucket_start_ms = bucket_end_ms - time_step_ms
            aligned_start_time = date.get_datestr_up_to_min(date.get_date_from_timestamp(bucket_start_ms))
            aligned_end_time = date.get_datestr_up_to_min(date.get_date_from_timestamp(bucket_end_ms))

            if bucket_end_ms in existing_data:
                point = existing_data[bucket_end_ms]
                point['start_time'] = aligned_start_time
                point['end_time'] = aligned_end_time
            else:
                if not variable.config.incremental_node:
                    point = {
                        'start_time': aligned_start_time,
                        'end_time': aligned_end_time,
                        'average_value': None,
                        'min_value': None,
                        'max_value': None,
                    }
                else:
                    point = {'start_time': aligned_start_time, 'end_time': aligned_end_time, 'value': None}

            points.append(point)

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
        points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
        if formatted and start_time and end_time and time_step_ms:
            self.__formatted_post_processing(variable, points, date.get_timestamp(start_time), date.get_timestamp(end_time), time_step_ms)
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
