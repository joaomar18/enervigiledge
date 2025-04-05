###########EXTERNAL IMPORTS############

from cryptography.fernet import Fernet
from datetime import datetime
import random
import time

#######################################

#############LOCAL IMPORTS#############

#######################################


def decrypt_password(password_encrypted: str, key: str) -> str:
    """
    Decrypts an encrypted password using the given Fernet key.

    Args:
        password_encrypted (str): The encrypted password string.
        key (str): The encryption key used to decrypt.

    Returns:
        str: The decrypted password.
    """

    return Fernet(key).decrypt(password_encrypted.encode()).decode()


def remove_phase_string(name: str) -> str:
    """
    Removes the phase prefix (e.g., 'l1_', 'l2_', 'l3_') from a node name if present.

    Args:
        name (str): The name of the node.

    Returns:
        str: The node name without the phase prefix.
    """

    parts = name.split("_")
    if parts[0] in {"l1", "l2", "l3"}:
        return "_".join(parts[1:])
    return name


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
