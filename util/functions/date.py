###########EXTERNAL IMPORTS############

from typing import Tuple, Optional
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

#######################################

#############LOCAL IMPORTS#############

from model.date import FormattedTimeStep

#######################################


def min_to_ms(mins: int) -> int:
    """
    Convert minutes to milliseconds.

    Args:
        mins: Number of minutes to convert.

    Returns:
        int: Equivalent time duration in milliseconds.
    """

    return mins * 60 * 1000


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


def process_time_span(
    start_time: datetime, end_time: datetime, formatted_time_step: Optional[FormattedTimeStep]
) -> Tuple[datetime, datetime, FormattedTimeStep]:

    start_time_fixed_prec = remove_sec_precision(start_time)
    end_time_fixed_prec = remove_sec_precision(end_time)

    if formatted_time_step is None:
        formatted_time_step = get_formatted_time_step(start_time_fixed_prec, end_time_fixed_prec)

    aligned_start_time = align_start_time(start_time_fixed_prec, formatted_time_step)
    aligned_end_time = align_end_time(end_time_fixed_prec, formatted_time_step)

    return (aligned_start_time, aligned_end_time, formatted_time_step)


def get_formatted_time_step(start_time: datetime, end_time: datetime) -> FormattedTimeStep:

    if start_time + relativedelta(years=1) > end_time:
        return FormattedTimeStep._1Y
    elif start_time + relativedelta(months=1) > end_time:
        return FormattedTimeStep._1M
    elif start_time + relativedelta(days=1) > end_time:
        return FormattedTimeStep._1d
    elif start_time + relativedelta(hours=1) > end_time:
        return FormattedTimeStep._1h
    elif start_time + relativedelta(minutes=15) > end_time:
        return FormattedTimeStep._15m
    else:
        return FormattedTimeStep._1m


def get_time_step_delta(formatted_time_step: FormattedTimeStep) -> relativedelta:

    if formatted_time_step is FormattedTimeStep._1m:
        return relativedelta(minutes=1)

    elif formatted_time_step is FormattedTimeStep._15m:
        return relativedelta(minutes=15)

    elif formatted_time_step is FormattedTimeStep._1h:
        return relativedelta(hours=1)

    elif formatted_time_step is FormattedTimeStep._1d:
        return relativedelta(days=1)

    elif formatted_time_step is FormattedTimeStep._1M:
        return relativedelta(months=1)

    elif formatted_time_step is FormattedTimeStep._1Y:
        return relativedelta(years=1)
    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")


def align_start_time(start_time: datetime, formatted_time_step: FormattedTimeStep) -> datetime:

    if formatted_time_step is FormattedTimeStep._1m:
        return start_time.replace(second=0, microsecond=0)

    elif formatted_time_step is FormattedTimeStep._15m:
        aligned_minute = (start_time.minute // 15) * 15
        return start_time.replace(minute=aligned_minute, second=0, microsecond=0)

    elif formatted_time_step is FormattedTimeStep._1h:
        return start_time.replace(minute=0, second=0, microsecond=0)

    elif formatted_time_step is FormattedTimeStep._1d:
        return start_time.replace(hour=0, minute=0, second=0, microsecond=0)

    elif formatted_time_step is FormattedTimeStep._1M:
        return start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    elif formatted_time_step is FormattedTimeStep._1Y:
        return start_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")


def align_end_time(end_time: datetime, formatted_time_step: FormattedTimeStep) -> datetime:

    if formatted_time_step is FormattedTimeStep._1m:
        if end_time.second == 0 and end_time.microsecond == 0:
            return end_time
        return end_time.replace(second=0, microsecond=0) + relativedelta(minutes=1)

    elif formatted_time_step is FormattedTimeStep._15m:
        if (end_time.minute % 15 == 0) and end_time.second == 0 and end_time.microsecond == 0:
            return end_time
        aligned_minute = (end_time.minute // 15) * 15
        return end_time.replace(minute=aligned_minute, second=0, microsecond=0) + relativedelta(minutes=15)

    elif formatted_time_step is FormattedTimeStep._1h:
        if end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            return end_time
        return end_time.replace(minute=0, second=0, microsecond=0) + relativedelta(hours=1)

    elif formatted_time_step is FormattedTimeStep._1d:
        if end_time.hour == 0 and end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            return end_time
        return end_time.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(days=1)

    elif formatted_time_step is FormattedTimeStep._1M:
        start_of_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if end_time == start_of_month:
            return end_time
        return start_of_month + relativedelta(months=1)

    elif formatted_time_step is FormattedTimeStep._1Y:
        start_of_year = end_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        if end_time == start_of_year:
            return end_time
        return start_of_year + relativedelta(years=1)

    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")
