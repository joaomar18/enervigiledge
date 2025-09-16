###########EXTERNAL IMPORTS############

from datetime import datetime
import random
import time

#######################################

#############LOCAL IMPORTS#############

#######################################


def generate_random_number(min: int = 0, max: int = 100000) -> int:
    """
    Generates a random integer between the specified range.

    Args:
        min (int): Minimum value (inclusive). Defaults to 0.
        max (int): Maximum value (inclusive). Defaults to 100000.

    Returns:
        int: Randomly generated integer.
    """

    return random.randint(min, max)


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
