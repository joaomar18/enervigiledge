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
    Aligns a start and end datetime to the nearest multiples of a given step.

    The start time is floored to the largest multiple of `time_step_ms`
    less than or equal to it, and the end time is ceiled to the smallest
    multiple of `time_step_ms` greater than or equal to it.

    Args:
        start_time (datetime): Original start datetime.
        end_time (datetime): Original end datetime.
        time_step_ms (int): Step size in milliseconds.

    Returns:
        tuple[datetime, datetime]: (aligned_start_time, aligned_end_time)
    """
    
    start_time_ms = int(start_time.timestamp() * 1000)
    end_time_ms = int(end_time.timestamp() * 1000)

    aligned_start_time_ms = (start_time_ms // time_step_ms) * time_step_ms
    aligned_end_time_ms = ((end_time_ms + time_step_ms - 1) // time_step_ms) * time_step_ms

    aligned_start_time = datetime.fromtimestamp(aligned_start_time_ms / 1000)
    aligned_end_time = datetime.fromtimestamp(aligned_end_time_ms / 1000)

    return aligned_start_time, aligned_end_time
