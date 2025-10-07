###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet
from typing import List, Dict, Any, Optional, Iterable, Iterator
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node
from controller.node.processor.numeric_processor import NumericNodeProcessor
from model.date import FormattedTimeStep
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
        Yield data points from InfluxDB query results.

        Args:
            res: Single ResultSet or iterable of ResultSet objects from InfluxDB query.

        Yields:
            Dict[str, Any]: Individual data points from the result sets.

        Raises:
            TypeError: If iterable contains non-ResultSet objects.
        """

        if isinstance(res, ResultSet):
            yield from res.get_points()
            return

        for rs in res:
            if not isinstance(rs, ResultSet):
                raise TypeError(f"Items must be ResultSet. Got type: {type(rs).__name__}")
            yield from rs.get_points()

    def __minutes_step_to_influx(self, minutes_step: int) -> str:
        rem = minutes_step * 60

        units = []
        for unit_seconds, suffix in [
            (7 * 24 * 3600, "w"),  # weeks
            (24 * 3600, "d"),  # days
            (3600, "h"),  # hours
            (60, "m"),  # minutes
            (1, "s"),  # seconds
        ]:
            val, rem = divmod(rem, unit_seconds)
            if val:
                units.append(f"{val}{suffix}")
        return "".join(units)

    def __build_query_without_time_span(self, variable: Node) -> str:
        """
        Build InfluxDB query for all data without time constraints.

        Args:
            variable: Node configuration defining measurement type and unit conversion.

        Returns:
            str: InfluxDB query string for retrieving all measurement data.
        """

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
        """
        Build InfluxDB query for raw data within time range.

        Args:
            variable: Node configuration defining measurement type and unit conversion.
            start_time_str: ISO format start time string with 'Z' suffix.
            end_time_str: ISO format end time string with 'Z' suffix.

        Returns:
            str: InfluxDB query string for raw data points within time range.
        """

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

    def __build_query_with_time_span_formatted(self, variable: Node, start_time_str: str, end_time_str: str, minutes_step: int) -> str:

        if isinstance(variable.processor, NumericNodeProcessor):
            unit_factor = f"{calculation.get_unit_factor(variable.config.unit)}"

            print(f"Time: {minutes_step}m")
            if not variable.config.incremental_node:
                query = f"""SELECT FIRST("start_time") AS start_time, LAST("end_time") AS end_time,
                        (SUM("mean_sum") / SUM("mean_count")) / {unit_factor} AS average_value,
                        MIN("min_value") / {unit_factor} AS min_value, MAX("max_value") / {unit_factor} AS max_value FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        GROUP BY time({self.__minutes_step_to_influx(minutes_step)}) FILL(null)"""
            else:
                query = f"""SELECT FIRST("start_time") AS start_time, LAST("end_time") AS end_time,
                        (SUM("value")) / {unit_factor} AS value FROM "{variable.config.name}"
                        WHERE time >= '{start_time_str}' AND time <= '{end_time_str}'
                        GROUP BY time({self.__minutes_step_to_influx(minutes_step)}m) FILL(null)"""

        else:
            raise NotImplementedError(f"Can't get logs from non numeric variables in formatted time spans")

        return query

    def __build_query_with_time_span(
        self, variable: Node, start_time_str: str, end_time_str: str, formatted: Optional[bool], minutes_step: Optional[int]
    ) -> str:

        if not formatted:
            query = self.__build_query_with_time_span_non_formatted(variable, start_time_str, end_time_str)

        elif minutes_step:
            query = self.__build_query_with_time_span_formatted(variable, start_time_str, end_time_str, minutes_step)

        else:
            raise ValueError(f"Wrong parameters to get logs from node {variable.config.name}.")

        return query

    def __build_query(
        self, variable: Node, start_time: Optional[datetime], end_time: Optional[datetime], formatted: Optional[bool], minutes_step: Optional[int]
    ) -> str:

        if start_time and end_time:
            start_time_str = start_time.isoformat() + "Z"
            end_time_str = end_time.isoformat() + "Z"
            query = self.__build_query_with_time_span(variable, start_time_str, end_time_str, formatted, minutes_step)

        else:
            query = self.__build_query_without_time_span(variable)

        return query

    def __formatted_post_processing(
        self,
        variable: Node,
        points: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
        time_step: FormattedTimeStep,
    ) -> None:

        if not isinstance(variable.processor, NumericNodeProcessor):
            return

        for point in points:
            if point["start_time"] is not None and point["end_time"] is not None:
                st = datetime.fromisoformat(point["start_time"])
                et = datetime.fromisoformat(point["end_time"])
                current_time_step = date.get_formatted_time_step(st, et)

                if date.time_step_to_minutes(st, time_step) < date.time_step_to_minutes(st, current_time_step):
                    time_step = current_time_step

        expected_buckets: List[datetime] = []

        current_time = start_time
        while current_time < end_time:
            expected_buckets.append(current_time)
            current_time += date.get_time_step_delta(time_step)

        existing_data: Dict[datetime, Dict[str, Any]] = {}

        for point in points:
            if point["start_time"] is not None:
                point_time = datetime.fromisoformat(point["start_time"])
                bucket_start = date.align_start_time(point_time, time_step)
                existing_data[bucket_start] = point

        points.clear()

        for bucket_start in expected_buckets:
            bucket_end = bucket_start + date.get_time_step_delta(time_step)

            if bucket_start in existing_data:
                point = existing_data[bucket_start]
                point['start_time'] = date.get_datestr_up_to_min(bucket_start)
                point['end_time'] = date.get_datestr_up_to_min(bucket_end)
            else:
                if not variable.config.incremental_node:
                    point = {
                        'start_time': date.get_datestr_up_to_min(bucket_start),
                        'end_time': date.get_datestr_up_to_min(bucket_end),
                        'average_value': None,
                        'min_value': None,
                        'max_value': None,
                    }
                else:
                    point = {
                        'start_time': date.get_datestr_up_to_min(bucket_start),
                        'end_time': date.get_datestr_up_to_min(bucket_end),
                        'value': None,
                    }

            points.append(point)

    def __round_numeric_variables(self, variable: Node, points: List[Dict[str, Any]]) -> None:
        """
        Apply decimal precision rounding to numeric variable values.

        Only applies to non-incremental numeric variables with configured decimal places.
        Rounds the 'average_value' field in-place for each data point.

        Args:
            variable: Node configuration with decimal_places setting.
            points: List of data points to modify (modified in-place).
        """

        if not isinstance(variable.processor, NumericNodeProcessor) or variable.config.incremental_node:
            return

        for point in points:
            if point["average_value"] is not None and variable.config.decimal_places is not None:
                point["average_value"] = round(point["average_value"], variable.config.decimal_places)

    def get_variable_logs_between(
        self,
        device_name: str,
        device_id: int,
        variable: Node,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        formatted: Optional[bool] = None,
        time_step: Optional[FormattedTimeStep] = None,
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
        final_points: List[Dict[str, Any]] = []

        if formatted and start_time and end_time and time_step:

            query_iterator = date.iterate_time_periods(start_time, end_time, time_step)
            if query_iterator:
                for st, minutes_step in query_iterator:
                    query = self.__build_query(variable, st, st + date.get_time_step_delta(time_step), formatted, minutes_step)
                    result = client.query(query)
                    points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
                    final_points.extend(points)
            else:
                query = self.__build_query(variable, start_time, end_time, formatted, date.time_step_to_minutes(start_time, time_step))
                result = client.query(query)
                points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
                final_points.extend(points)

        else:
            query = self.__build_query(variable, start_time, end_time, False, None)
            result = client.query(query)
            points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
            final_points.extend(points)

        if formatted and start_time and end_time and time_step:
            self.__formatted_post_processing(variable, final_points, start_time, end_time, time_step)
        self.__round_numeric_variables(variable, final_points)
        print(f"Final points: {final_points}")
        return final_points

    def delete_variable_data(self, device_name: str, device_id: int, variable: Node) -> bool:
        """
        Delete all measurement data for a specific variable.

        Args:
            device_name: Name of the device containing the variable.
            device_id: Unique ID of the device.
            variable: Node configuration defining the variable to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.

        Raises:
            ValueError: If the device database does not exist.
        """

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
