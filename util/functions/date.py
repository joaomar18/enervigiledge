###########EXTERNAL IMPORTS############

from typing import Tuple, Optional
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import time

#######################################

#############LOCAL IMPORTS#############

from model.date import FormattedTimeStep

#######################################


def get_ms_difference(date_1: datetime, date_2: datetime) -> int:
    """
    Calculate difference between two datetimes in milliseconds.

    Args:
        date_1: First datetime (earlier time).
        date_2: Second datetime (later time).

    Returns:
        int: Difference in milliseconds (date_2 - date_1).
    """

    return get_timestamp(date_2) - get_timestamp(date_1)


def min_to_ms(mins: int) -> int:
    """
    Convert minutes to milliseconds.

    Args:
        mins: Number of minutes to convert.

    Returns:
        int: Equivalent time duration in milliseconds.
    """

    return mins * 60 * 1000


def min_duration_ms(date: datetime) -> int:
    """Duration in milliseconds until the next minute."""

    next_date = date + timedelta(minutes=1)
    return get_ms_difference(date, next_date)


def min15_duration_ms(date: datetime) -> int:
    """Duration in milliseconds until 15 minutes later."""

    next_date = date + timedelta(minutes=15)
    return get_ms_difference(date, next_date)


def hour_duration_ms(date: datetime) -> int:
    """Duration in milliseconds until the next hour."""

    next_date = date + timedelta(hours=1)
    return get_ms_difference(date, next_date)


def day_duration_ms(date: datetime) -> int:
    """Duration in milliseconds until the next day."""

    next_date = date + timedelta(days=1)
    return get_ms_difference(date, next_date)


def month_duration_ms(date: datetime) -> int:

    next_date = date + relativedelta(months=1)
    return get_ms_difference(date, next_date)


def year_duration_ms(date: datetime) -> int:

    next_date = date + relativedelta(years=1)
    return get_ms_difference(date, next_date)


def get_current_datetime() -> datetime:
    """
    Returns the current date and time.

    Returns:
        datetime: The current timestamp.
    """

    return datetime.now().astimezone()


def get_timestamp(date: datetime) -> int:
    """
    Convert datetime to millisecond timestamp.

    Args:
        date: The datetime to convert.

    Returns:
        int: Timestamp in milliseconds.
    """
    return int(date.timestamp() * 1000)


def get_date_from_timestamp(timestamp: int) -> datetime:
    """
    Convert millisecond timestamp to datetime.

    Args:
        timestamp: Timestamp in milliseconds.

    Returns:
        datetime: The corresponding datetime object.
    """
    return datetime.fromtimestamp(timestamp / 1000)


def convert_isostr_to_timezonedate(date_str: str) -> datetime:
    """
    Convert ISO format string to local timezone datetime.

    Handles both 'Z' suffix (UTC) and timezone-aware ISO strings.
    If the datetime is naive (no timezone), assumes UTC.

    Args:
        date_str: ISO format datetime string (e.g., "2025-10-03T10:25:37.432Z")

    Returns:
        datetime: Datetime object converted to system's local timezone.
    """

    # Handle 'Z' suffix (UTC indicator)
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'

    # Parse the ISO string
    date = datetime.fromisoformat(date_str)

    # If no timezone info, assume UTC
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)

    return date.astimezone()


def remove_sec_precision(date: datetime) -> datetime:
    """
    Remove seconds and microseconds from datetime, keeping only up to minutes.

    Args:
        date: The datetime to truncate.

    Returns:
        datetime: Datetime with seconds and microseconds set to zero.
    """

    return date.replace(second=0, microsecond=0)


def get_datestr_up_to_min(date: datetime) -> str:
    """
    Format datetime as string up to minutes precision.

    Args:
        date: The datetime to format.

    Returns:
        str: Formatted date string in "YYYY-MM-DD HH:MM" format.
    """
    return date.strftime("%Y-%m-%d %H:%M")


def subtract_datetime_mins(date_time_01: datetime, date_time_02: datetime) -> int:
    """
    Calculates the difference in minutes between two datetime objects, ignoring the date.

    Args:
        date_time_01 (datetime): First datetime.
        date_time_02 (datetime): Second datetime.

    Returns:
        int: Difference in minutes between the two times.
    """

    minutes_01 = date_time_01.minute + (date_time_01.hour * 60)
    minutes_02 = date_time_02.minute + (date_time_02.hour * 60)
    return minutes_01 - minutes_02


def get_aligned_start_time(start_time_ms: int, time_step_ms: int, timezone_offset: Optional[timedelta]) -> int:
    """
    Align start timestamp to timezone-aware time step boundary (floor).

    Args:
        start_time_ms: Start timestamp in milliseconds.
        time_step_ms: Time step duration in milliseconds.
        timezone_offset: Timezone offset from UTC, or None for no adjustment.

    Returns:
        int: Aligned start timestamp in milliseconds.
    """

    timezone_offset_ms = int(timezone_offset.total_seconds() * 1000) if timezone_offset is not None else 0
    print(timezone_offset_ms)
    aligned_start_time_ms = (((start_time_ms + timezone_offset_ms) // time_step_ms) * time_step_ms) - timezone_offset_ms
    return aligned_start_time_ms


def get_aligned_end_time(end_time_ms: int, time_step_ms: int, timezone_offset: Optional[timedelta]) -> int:
    """
    Align end timestamp to timezone-aware time step boundary (ceil).

    Args:
        end_time_ms: End timestamp in milliseconds.
        time_step_ms: Time step duration in milliseconds.
        timezone_offset: Timezone offset from UTC, or None for no adjustment.

    Returns:
        int: Aligned end timestamp in milliseconds.
    """

    timezone_offset_ms = int(timezone_offset.total_seconds() * 1000) if timezone_offset is not None else 0
    aligned_end_time_ms = ((((end_time_ms + timezone_offset_ms) + time_step_ms - 1) // time_step_ms) * time_step_ms) - timezone_offset_ms
    return aligned_end_time_ms


def process_time_span(
    start_time: datetime, end_time: datetime, formatted_time_step: Optional[FormattedTimeStep]
) -> Tuple[datetime, datetime, int, bool]:

    start_time_fixed_prec = remove_sec_precision(start_time)
    end_time_fixed_prec = remove_sec_precision(end_time)

    start_time_ms = get_timestamp(start_time_fixed_prec)
    end_time_ms = get_timestamp(end_time_fixed_prec)

    if formatted_time_step is None:
        formatted_time_step = get_formatted_time_step(start_time_fixed_prec, start_time_ms, end_time_ms)

    time_step_ms, step_update = get_time_step_ms(start_time_fixed_prec, formatted_time_step)

    aligned_start_time_ms = get_aligned_start_time(start_time_ms, time_step_ms, start_time.utcoffset())
    aligned_end_time_ms = get_aligned_end_time(end_time_ms, time_step_ms, end_time.utcoffset())

    aligned_start_time = get_date_from_timestamp(aligned_start_time_ms)
    aligned_end_time = get_date_from_timestamp(aligned_end_time_ms)

    return (aligned_start_time, aligned_end_time, time_step_ms, step_update)


def get_formatted_time_step(start_time: datetime, start_time_ms: int, end_time_ms: int) -> FormattedTimeStep:
    """
    Determine appropriate time step based on time span duration.

    Args:
        start_time: Start datetime for duration calculations.
        start_time_ms: Start time in milliseconds.
        end_time_ms: End time in milliseconds.

    Returns:
        FormattedTimeStep: Appropriate time step for the given span.
    """

    span_ms = end_time_ms - start_time_ms

    if span_ms > year_duration_ms(start_time):
        return FormattedTimeStep._1Y
    elif span_ms > month_duration_ms(start_time):
        return FormattedTimeStep._1M
    elif span_ms > day_duration_ms(start_time):
        return FormattedTimeStep._1d
    elif span_ms > hour_duration_ms(start_time):
        return FormattedTimeStep._1h
    elif span_ms > min15_duration_ms(start_time):
        return FormattedTimeStep._15m
    else:
        return FormattedTimeStep._1m


def get_time_step_ms(start_time: datetime, formatted_time_step: FormattedTimeStep) -> Tuple[int, bool]:

    if formatted_time_step is FormattedTimeStep._1m:
        return (min_duration_ms(start_time), False)
    elif formatted_time_step is FormattedTimeStep._15m:
        return (min15_duration_ms(start_time), False)
    elif formatted_time_step is FormattedTimeStep._1h:
        return (hour_duration_ms(start_time), False)
    elif formatted_time_step is FormattedTimeStep._1d:
        return (day_duration_ms(start_time), False)
    elif formatted_time_step is FormattedTimeStep._1M:
        return (month_duration_ms(start_time), True)
    elif formatted_time_step is FormattedTimeStep._1Y:
        return (year_duration_ms(start_time), True)
    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")
