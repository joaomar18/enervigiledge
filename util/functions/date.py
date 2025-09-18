###########EXTERNAL IMPORTS############

from datetime import datetime
import time

#######################################

#############LOCAL IMPORTS#############

#######################################


def get_current_date() -> datetime:
    """
    Returns the current date and time.

    Returns:
        datetime: The current timestamp.
    """

    return datetime.fromtimestamp(time.time())


def get_timestamp(date: datetime) -> int:
    return int(date.timestamp() * 1000)


def get_date_from_timestamp(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp / 1000)


def remove_sec_precision(date: datetime) -> datetime:
    date_nosec_precision = datetime(date.year, date.month, date.day, date.hour, date.minute)
    return date_nosec_precision


def get_datestr_up_to_min(date: datetime) -> str:
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


def process_time_span(start_time: datetime, end_time: datetime, time_step_ms: int):
    """
    Align start and end datetimes to a fixed step size.

    - Seconds and microseconds are truncated from both inputs.
    - The start time is floored to the nearest lower multiple of `time_step_ms`.
    - The end time is ceiled to the nearest upper multiple of `time_step_ms`.

    Args:
        start_time: Input start datetime.
        end_time: Input end datetime.
        time_step_ms: Step size in milliseconds.

    Returns:
        tuple[datetime, datetime]: Aligned (start_time, end_time).
    """

    start_time_ms = get_timestamp(remove_sec_precision(start_time))
    end_time_ms = get_timestamp(remove_sec_precision(end_time))

    print(f"Initial Start Time: {remove_sec_precision(start_time)}")
    print(f"Initial End Time: {remove_sec_precision(end_time)}")

    print(f"Initial Start Time Ms: {start_time_ms}")
    print(f"Initial End Time Ms: {end_time_ms}")
    print(f"Time Step: {time_step_ms}")

    aligned_start_time_ms = (start_time_ms // time_step_ms) * time_step_ms
    aligned_end_time_ms = ((end_time_ms + time_step_ms - 1) // time_step_ms) * time_step_ms

    print(f"Final Start Time Ms: {aligned_start_time_ms}")
    print(f"Final End Time Ms: {aligned_end_time_ms}")

    aligned_start_time = get_date_from_timestamp(aligned_start_time_ms)
    aligned_end_time = get_date_from_timestamp(aligned_end_time_ms)

    print(f"Final Start Time: {aligned_start_time}")
    print(f"Final End Time: {aligned_end_time}")

    return aligned_start_time, aligned_end_time
