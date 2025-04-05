from cryptography.fernet import Fernet
from datetime import datetime
import random
import time


def decrypt_password(password_encrypted: str, key: str) -> str:
    return Fernet(key).decrypt(password_encrypted).decode()


def remove_phase_string(name: str) -> str:
    parts = name.split("_")
    if parts[0] in {"l1", "l2", "l3"}:
        return "_".join(parts[1:])
    return name


def generate_random_number(min: int = 0, max: int = 100000):
    a = random.randint(min, max)
    return a


def get_current_date() -> datetime:
    return datetime.fromtimestamp(time.time())


def subtracte_datetime_mins(date_time_01: datetime, date_time_02: datetime) -> int:
    minutes_date_time_01 = date_time_01.minute + (date_time_01.hour * 60)
    minutes_date_time_02 = date_time_02.minute + (date_time_02.hour * 60)
    return minutes_date_time_01 - minutes_date_time_02
