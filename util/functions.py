from datetime import datetime
import random
import time

def generate_random_number(min: int = 0, max: int = 100000):
    a = random.randint(min, max)
    return a

def get_current_date() -> datetime:
    return datetime.fromtimestamp(time.time())

def subtracte_datetime_mins(date_time_01: datetime, date_time_02: datetime) -> int:
    minutes_date_time_01 = date_time_01.minute + (date_time_01.hour * 60)
    minutes_date_time_02 = date_time_02.minute + (date_time_02.hour * 60)
    return minutes_date_time_01 - minutes_date_time_02
    