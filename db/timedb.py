###########EXERTNAL IMPORTS############

import asyncio
from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet
from typing import List, Dict, Tuple, Any, Optional, Iterable, Iterator
from datetime import datetime
from dataclasses import dataclass
from zoneinfo import ZoneInfo

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
from controller.node.node import Node
from controller.node.processor.numeric_processor import NumericNodeProcessor
from model.controller.node import NodeLogs
from model.date import FormattedTimeStep, TimeSpanParameters
from model.db import QueryVariableLogs
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
            - "time": Primary timestamp (start_time with second precision removed)
        """

        formatted = []

        for item in data:
            if not all(k in item for k in ("name", "start_time", "end_time")):
                raise ValueError(f"Missing required fields in data item: {item}")

            name: str = item["name"]
            start_time: datetime = item["start_time"]
            end_time: datetime = item["end_time"]

            formatted_start = date.to_iso_minutes(start_time)
            formatted_end = date.to_iso_minutes(end_time)

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

            formatted.append({"measurement": name, "fields": fields, "time": date.remove_sec_precision(start_time)})

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

    def __extend_query(self, query: QueryVariableLogs, variable: Node, aggregated: bool) -> None:
        """
        Extends an InfluxDB query with field selections based on variable type and query mode.

        Adds appropriate SELECT fields and filters to the query object in-place:
        - Numeric non-incremental: mean_sum, mean_count, average_value, min_value, max_value
        - Numeric incremental: value (with unit conversion)
        - Non-numeric: raw value only (formatted mode not supported)

        For formatted queries, applies aggregation functions (SUM/MIN/MAX).
        For raw queries, selects individual field values.

        Args:
            query: Query builder modified in-place.
            variable: Node configuration with processor type and settings.
            aggregated: If True, applies time-bucket aggregations.

        Raises:
            NotImplementedError: If formatted=True for non-numeric variables.
        """

        if isinstance(variable.processor, NumericNodeProcessor):
            unit_factor = f"{calculation.get_unit_factor(variable.config.unit)}"

            if not variable.config.incremental_node:
                query.where.append('"mean_count" > 0')
                if aggregated: # aggregated/bucketed
                    query.fields.extend([
                        f'SUM("mean_sum") AS mean_sum',
                        f'SUM("mean_count") AS mean_count',
                        f'(SUM("mean_sum") / SUM("mean_count")) / {unit_factor} AS average_value',
                        f'MIN("min_value") / {unit_factor} AS min_value',
                        f'MAX("max_value") / {unit_factor} AS max_value',
                    ])
                else: # raw/non-formatted
                    query.fields.extend([
                        f'"mean_sum" AS mean_sum',
                        f'"mean_count" AS mean_count',
                        f'("mean_sum" / "mean_count") / {unit_factor} AS average_value',
                        f'"min_value" / {unit_factor} AS min_value',
                        f'"max_value" / {unit_factor} AS max_value',
                ])
            else: # incremental node
                if aggregated:
                    query.fields.extend([f'SUM("value") / {unit_factor} AS value'])

                else:
                    query.fields.extend([f'"value" / {unit_factor} AS value'])
        else:
            if aggregated:
                raise NotImplementedError("Can't get logs from non-numeric variables in formatted time spans")
            query.fields.extend(['"value"'])

    def __build_query_without_time_span(self, variable: Node, time_zone: Optional[ZoneInfo]) -> str:
        """
        Builds a raw InfluxDB query to retrieve all logs for a variable without time filtering.

        Args:
            variable: Node configuration with variable name and processor settings.
            time_zone: Optional timezone for timestamp conversion.

        Returns:
            str: Rendered InfluxDB query string.
        """

        query = QueryVariableLogs(variable=variable.config.name, fields=["start_time", "end_time"], timezone=time_zone.key if time_zone else None)
        self.__extend_query(query, variable, False)
        return query.render()

    def __build_query_with_time_span_non_aggregated(self, variable: Node, start_time_str: str, end_time_str: str, time_zone: Optional[ZoneInfo]) -> str:
        """
        Builds a raw InfluxDB query to retrieve variable logs within a time range.

        Args:
            variable: Node configuration with variable name and processor settings.
            start_time_str: ISO format start time (inclusive).
            end_time_str: ISO format end time (exclusive).
            time_zone: Optional timezone for timestamp conversion.

        Returns:
            str: Rendered InfluxDB query string with time range filter.
        """

        query = QueryVariableLogs(variable=variable.config.name, fields=["start_time", "end_time"], where=[f"time >= '{start_time_str}'", f"time < '{end_time_str}'"], timezone=time_zone.key if time_zone else None)
        self.__extend_query(query, variable, False)
        return query.render()

    def __build_query_with_time_span_aggregated(self, variable: Node, start_time_str: str, end_time_str: str, group_by_time: Optional[str], time_zone: Optional[ZoneInfo]) -> str:
        """
        Builds an aggregated InfluxDB query to retrieve variable logs grouped into time buckets.

        Args:
            variable: Node configuration with variable name and processor settings.
            start_time_str: ISO format start time (inclusive).
            end_time_str: ISO format end time (exclusive).
            group_by_time: Optional time bucket interval (e.g., "1h", "15m"). If not provided,
                no time-based grouping is applied.
            time_zone: Optional timezone for timestamp conversion.

        Returns:
            str: Rendered InfluxDB query string with aggregations and optional time bucketing.
        """ 

        query = QueryVariableLogs(variable=variable.config.name, fields=['FIRST("start_time") AS start_time', 'LAST("end_time") AS end_time'], where=[f"time >= '{start_time_str}'", f"time < '{end_time_str}'"], fill="null", timezone=time_zone.key if time_zone else None)
        if group_by_time:
            query.group_by = [f"time({group_by_time})"]
        self.__extend_query(query, variable, True)
        return query.render()

    def __build_query_with_time_span(
        self, variable: Node, start_time_str: str, end_time_str: str, aggregated: Optional[bool], group_by_time: Optional[str], time_zone: Optional[ZoneInfo]
    ) -> str:
        """
        Builds an InfluxDB query with time range filtering, either raw or aggregated.

        Args:
            variable: Node configuration with variable name and processor settings.
            start_time_str: ISO format start time (inclusive).
            end_time_str: ISO format end time (exclusive).
            aggregated: If True, builds aggregated query; if False or None, builds raw query.
            group_by_time: Optional time bucket interval for aggregated queries (e.g., "1h", "15m").
            time_zone: Optional timezone for timestamp conversion.

        Returns:
            str: Rendered InfluxDB query string.
        """

        if not aggregated:
            query = self.__build_query_with_time_span_non_aggregated(variable, start_time_str, end_time_str, time_zone)

        else:
            query = self.__build_query_with_time_span_aggregated(variable, start_time_str, end_time_str, group_by_time, time_zone)

        return query

    def __build_query(
        self, variable: Node, start_time: Optional[datetime], end_time: Optional[datetime], aggregated: Optional[bool], group_by_time: Optional[str], time_zone: Optional[ZoneInfo]
    ) -> str:
        """
        Builds an InfluxDB query for variable logs with optional time filtering and aggregation.

        Converts datetime objects to UTC ISO format and delegates to appropriate query builder
        based on whether time filtering is required.

        Args:
            variable: Node configuration with variable name and processor settings.
            start_time: Optional start time for filtering (inclusive).
            end_time: Optional end time for filtering (exclusive).
            aggregated: If True, builds aggregated query; if False, builds raw query.
            group_by_time: Time bucket interval for formatted queries (e.g., "1h", "15m").
            time_zone: Optional timezone for timestamp conversion in results.

        Returns:
            str: Rendered InfluxDB query string.
        """

        if start_time and end_time:
            start_time_str = start_time.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
            end_time_str = end_time.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
            query = self.__build_query_with_time_span(variable, start_time_str, end_time_str, aggregated, group_by_time, time_zone)

        else:
            query = self.__build_query_without_time_span(variable, time_zone)

        return query

    def __adjust_time_step(self, points: List[Dict[str, Any]], time_step: FormattedTimeStep) -> FormattedTimeStep:
        """
        Adjusts the time step to the largest interval found in the data points.

        Iterates through all points to determine their actual time intervals and returns
        the maximum time step encountered, ensuring the returned step can accommodate all data.

        Args:
            points: List of data points containing start_time and end_time fields.
            time_step: Initial time step to compare against.

        Returns:
            FormattedTimeStep: The largest time step found across all points.
        """

        for point in points:
            if point["start_time"] is not None and point["end_time"] is not None:
                st = date.convert_isostr_to_date(point["start_time"])
                et = date.convert_isostr_to_date(point["end_time"])
                current_time_step = date.get_formatted_time_step(st, et)
                time_step = date.bigger_time_step(time_step, current_time_step)

        return time_step

    def __align_points_start_time(
        self, points: List[Dict[str, Any]], aligned_time_buckets: List[Tuple[datetime, datetime]]
    ) -> Dict[datetime, Dict[str, Any]]:
        """
        Maps data points to their corresponding time bucket start times.

        Creates a dictionary where keys are bucket start times and values are the data points
        that fall within those buckets, based on each point's start_time.

        Args:
            points: List of data points containing start_time fields.
            aligned_time_buckets: List of (start, end) datetime tuples defining time buckets.

        Returns:
            Dict[datetime, Dict[str, Any]]: Mapping of bucket start times to their data points.
        """

        existing_data: Dict[datetime, Dict[str, Any]] = {}

        for point in points:
            if point["start_time"] is not None:
                point_time = date.convert_isostr_to_date(point["start_time"])
                bucket_start = date.find_bucket_for_time(point_time, aligned_time_buckets)
                existing_data[bucket_start] = point
        return existing_data

    def __fill_formatted_time_buckets(
        self,
        variable: Node,
        points: List[Dict[str, Any]],
        aligned_time_buckets: List[Tuple[datetime, datetime]],
        existing_data: Dict[datetime, Dict[str, Any]],
    ) -> None:
        """
        Populates the points list with data for each time bucket, filling gaps with None values.

        For each time bucket, either uses existing data or creates a placeholder point with
        None values. Updates bucket start/end times to align with the bucket boundaries.
        The structure of placeholder points depends on whether the variable is incremental.

        Args:
            variable: Node configuration to determine point structure.
            points: List to populate with aligned data points (modified in-place).
            aligned_time_buckets: List of (start, end) datetime tuples defining time buckets.
            existing_data: Mapping of bucket start times to existing data points.
        """

        for bucket_start, bucket_end in aligned_time_buckets:
            if bucket_start in existing_data:
                point = existing_data[bucket_start]
                point['start_time'] = date.to_iso_minutes(bucket_start)
                point['end_time'] = date.to_iso_minutes(bucket_end)
            else:
                if not variable.config.incremental_node:
                    point = {
                        'start_time': date.to_iso_minutes(bucket_start),
                        'end_time': date.to_iso_minutes(bucket_end),
                        'average_value': None,
                        'min_value': None,
                        'max_value': None,
                    }
                else:
                    point = {
                        'start_time': date.to_iso_minutes(bucket_start),
                        'end_time': date.to_iso_minutes(bucket_end),
                        'value': None,
                    }

            points.append(point)

    def __formatted_post_processing(
        self,
        variable: Node,
        points: List[Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
        time_step: FormattedTimeStep,
        time_zone: Optional[ZoneInfo],
    ) -> Optional[FormattedTimeStep]:
        """
        Post-processes formatted query results to ensure complete time bucket coverage.

        Adjusts the time step based on actual data intervals, aligns points to time buckets,
        and fills gaps with None values to create a continuous time series. Only applies to
        numeric variables.

        Args:
            variable: Node configuration to determine processing logic.
            points: List of data points from query (modified in-place).
            start_time: Start of the query time range.
            end_time: End of the query time range.
            time_step: Initial time step for bucketing.
            time_zone: Optional timezone for bucket alignment.

        Returns:
            Optional[FormattedTimeStep]: Adjusted time step, or None for non-numeric variables.
        """

        if not isinstance(variable.processor, NumericNodeProcessor):
            return None

        time_step = self.__adjust_time_step(points, time_step)
        aligned_time_buckets = date.get_aligned_time_buckets(start_time, end_time, time_step, time_zone)
        existing_data = self.__align_points_start_time(points, aligned_time_buckets)
        points.clear()
        self.__fill_formatted_time_buckets(variable, points, aligned_time_buckets, existing_data)
        return time_step

    def __post_process_points(self, variable: Node, points: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Computes global metrics and applies post-processing to numeric variable data points.

        Calculates aggregate statistics across all data points and performs cleanup operations.
        For non-incremental variables: computes global weighted averages using mean_sum/mean_count,
        tracks global min/max values with their corresponding timestamps, applies decimal rounding,
        and removes internal calculation fields. For incremental variables: sums all values to
        produce a global total.

        Args:
            variable: Node configuration with processor type, unit, and decimal precision settings.
            points: List of data point dictionaries. Internal fields are removed in-place during processing.

        Returns:
            For non-incremental numeric variables, returns dict with:
                - average_value: Weighted average across all points
                - min_value: Global minimum value
                - max_value: Global maximum value
                - min_value_start_time: Start timestamp of minimum value occurrence
                - min_value_end_time: End timestamp of minimum value occurrence
                - max_value_start_time: Start timestamp of maximum value occurrence
                - max_value_end_time: End timestamp of maximum value occurrence

            For incremental numeric variables, returns dict with:
                - value: Sum of all point values

            Returns None for non-numeric variables (no processing applied).
        """

        if not isinstance(variable.processor, NumericNodeProcessor): 
            return None

        global_metrics: Dict[str, Any] = {}

        if not variable.config.incremental_node:
            global_mean_sum = 0
            global_mean_count = 0
            global_mean_value = None
            global_min_value = None
            global_min_st: Optional[str] = None
            global_min_et: Optional[str] = None

            global_max_value = None
            global_max_st: Optional[str] = None
            global_max_et: Optional[str] = None

            for point in points:
                if point["average_value"] is not None and variable.config.decimal_places is not None:
                    point["average_value"] = round(point["average_value"], variable.config.decimal_places)
                
                global_mean_sum += point.pop("mean_sum", 0)
                global_mean_count += point.pop("mean_count", 0)

                if point["min_value"] is not None:
                    if global_min_value is not None:
                        if point["min_value"] < global_min_value:
                            global_min_value = point["min_value"]
                            global_min_st = point["start_time"]
                            global_min_et = point["end_time"]

                    else:
                        global_min_value = point["min_value"]
                        global_min_st = point["start_time"]
                        global_min_et = point["end_time"]
                
                if point["max_value"] is not None:
                    if global_max_value is not None:
                        if point["max_value"] > global_max_value:
                            global_max_value = point["max_value"]
                            global_max_st = point["start_time"]
                            global_max_et = point["end_time"]

                    else:
                        global_max_value = point["max_value"]
                        global_max_st = point["start_time"]
                        global_max_et = point["end_time"]

            global_mean_value = (global_mean_sum / global_mean_count) if global_mean_count != 0 else None
            if global_mean_value is not None:
                global_mean_value /= calculation.get_unit_factor(variable.config.unit)
                global_mean_value = round(global_mean_value, variable.config.decimal_places) if variable.config.decimal_places is not None else global_mean_value
            
            global_metrics["average_value"] = global_mean_value
            global_metrics["min_value"] = global_min_value
            global_metrics["max_value"] = global_max_value
            global_metrics["min_value_start_time"] = global_min_st
            global_metrics["min_value_end_time"] = global_min_et
            global_metrics["max_value_start_time"] = global_max_st
            global_metrics["max_value_end_time"] = global_max_et

        else:
            global_sum = 0

            for point in points:
                global_sum += point["value"] if point["value"] is not None else 0
            
            global_metrics["value"] = global_sum

        return global_metrics      

    def __get_formatted_variable_logs(self, client: InfluxDBClient, variable: Node, start_time: datetime, end_time: datetime, time_step: FormattedTimeStep, time_zone: Optional[ZoneInfo] = None) -> List[Dict[str, Any]]:
        """
        Retrieves aggregated variable logs grouped by time buckets.

        Executes one or more formatted queries depending on the time range size. For large
        time spans, splits the query into multiple periods to handle InfluxDB query limits.
        Filters out the internal 'time' field from results.

        Args:
            client: Active InfluxDB client connection.
            variable: Node configuration with variable name and processor settings.
            start_time: Start of the query time range.
            end_time: End of the query time range.
            time_step: Time bucket interval for aggregation.
            time_zone: Optional timezone for bucket alignment and timestamp conversion.

        Returns:
            List[Dict[str, Any]]: Aggregated data points across all time buckets.
        """

        variable_logs: List[Dict[str, Any]] = []

        query_iterator = date.iterate_time_periods(start_time, end_time, time_step, time_zone)
        if query_iterator:
            for st, group_by_time in query_iterator:
                query = self.__build_query(variable, st, date.calculate_date_delta(st, time_step, time_zone), True, group_by_time, time_zone)

                result = client.query(query)
                points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
                variable_logs.extend(points)
        else:
            query = self.__build_query(variable, start_time, end_time, True, date.time_step_grouping(start_time, time_step, time_zone), time_zone)
            result = client.query(query)
            points = [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]
            variable_logs.extend(points)

        return variable_logs

    def __get_raw_variable_logs(self, client: InfluxDBClient, variable: Node, start_time: Optional[datetime], end_time: Optional[datetime], time_zone: Optional[ZoneInfo], force_aggregation: Optional[bool]) -> List[Dict[str, Any]]:
        """
        Retrieves variable logs, typically without aggregation or time bucketing.

        Executes a non-formatted query to get data points within the optional
        time range. Aggregation can be forced if specified. Filters out the 
        internal 'time' field from results.

        Args:
            client: Active InfluxDB client connection.
            variable: Node configuration with variable name and processor settings.
            start_time: Optional start time for filtering (inclusive).
            end_time: Optional end time for filtering (exclusive).
            time_zone: Optional timezone for timestamp conversion.
            force_aggregation: When True, returns results with aggregation applied.

        Returns:
            List[Dict[str, Any]]: Data points from the database.
        """

        query = self.__build_query(variable, start_time, end_time, force_aggregation, None, time_zone)
        result = client.query(query)
        return [{k: v for k, v in point.items() if k not in {"time"}} for point in self.__iter_points(result)]

    def get_variable_logs(
        self,
        device_name: str,
        device_id: int,
        variable: Node,
        time_span: TimeSpanParameters,
        remove_points: bool = False,
    ) -> NodeLogs:
        """
        Retrieve historical logs for a specific variable from a device's InfluxDB, with optional 
        time filtering, aggregation, and formatting.

        The method ensures the device's database exists, then fetches variable logs either as raw 
        time series points or as time-bucketed (formatted) results. Formatted logs fill missing 
        buckets with `None` and compute global statistics for numeric variables.

        Args:
            device_name (str): Name of the device containing the variable.
            device_id (int): Unique identifier for the device.
            variable (Node): Node instance describing the variable's configuration and processing rules.
            time_span (TimeSpanParameters): Defines the time window and query options, including:
                - start_time (Optional[datetime]): Inclusive start of the time range.
                - end_time (Optional[datetime]): Exclusive end of the time range.
                - formatted (bool): Whether to bucket data into intervals.
                - time_step (Optional[str|timedelta]): Interval for bucketing/aggregation (used if formatted).
                - time_zone (Optional[str]): Time zone for aligning bucket edges.
                - force_aggregation (Optional[bool]): Force aggregation even for raw logs.
            remove_points (bool, default=False): If True, the returned NodeLogs will omit the points list.

        Returns:
            NodeLogs: Object containing the variable's log data and metadata:
                - unit (Optional[str]): Measurement unit of the variable.
                - decimal_places (Optional[int]): Precision of numeric values.
                - type (str): Variable type, e.g., "numeric" or "boolean".
                - incremental (bool): True if the variable is cumulative.
                - points (List[Any]): Raw or bucketed data points (empty if `remove_points=True`).
                - time_step (Optional[str|timedelta]): Actual bucket interval used (if formatted).
                - global_metrics (Optional[dict]): Computed statistics for numeric variables.

        Raises:
            ValueError: If only one of `start_time` or `end_time` is provided, or if `end_time` 
                        is not after `start_time`.
        """

        client = self.__require_client()
        db_name = f"{device_name}_{device_id}"

        if not self.check_db_exists(db_name):
            client.create_database(db_name)

        if (time_span.start_time and not time_span.end_time) or (time_span.end_time and not time_span.start_time):
            raise ValueError("Both 'start_time' and 'end_time' must be provided together.")

        if time_span.start_time and time_span.end_time and time_span.end_time <= time_span.start_time:
            raise ValueError("'end_time' must be a later date than 'start_time'.")

        client.switch_database(db_name)

        if time_span.formatted and time_span.start_time and time_span.end_time and time_span.time_step: # Logs are to be Formatted
            points = self.__get_formatted_variable_logs(client, variable, time_span.start_time, time_span.end_time, time_span.time_step, time_span.time_zone)

        else:
            points = self.__get_raw_variable_logs(client, variable, time_span.start_time, time_span.end_time, time_span.time_zone, time_span.force_aggregation)

        if time_span.formatted and time_span.start_time and time_span.end_time and time_span.time_step: # Apply post logs processing if logs are Formatted
            time_span.time_step = self.__formatted_post_processing(variable, points, time_span.start_time, time_span.end_time, time_span.time_step, time_span.time_zone)
        global_metrics = self.__post_process_points(variable, points)

        variable_logs = NodeLogs(
            unit=variable.config.unit,
            decimal_places=variable.config.decimal_places,
            type=variable.config.type,
            incremental=variable.config.incremental_node,
            points=points if not remove_points else [],
            time_step=time_span.time_step,
            global_metrics=global_metrics
        )

        return variable_logs

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
