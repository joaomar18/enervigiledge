###########EXTERNAL IMPORTS############

from typing import Tuple, Optional, List, Iterator
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import arrow

#######################################

#############LOCAL IMPORTS#############

from model.date import FormattedTimeStep

#######################################


def min_to_ms(mins: int) -> int:

    return mins * 60 * 1000


def get_current_utc_datetime() -> datetime:

    return datetime.now(tz=timezone.utc)


def get_timestamp(date: datetime) -> int:

    return int(date.timestamp() * 1000)


def get_date_from_timestamp(timestamp: int) -> datetime:

    return datetime.fromtimestamp(timestamp / 1000)


def convert_isostr_to_date(date_str: str) -> datetime:
    date = datetime.fromisoformat(date_str)
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    return date


def convert_isostr_to_utc_date(date_str: str) -> datetime:

    date = convert_isostr_to_date(date_str)
    return date.replace(tzinfo=timezone.utc)


def remove_sec_precision(date: datetime) -> datetime:

    return date.replace(second=0, microsecond=0)


def to_iso_minutes(date: datetime) -> str:

    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)

    date = date.replace(second=0, microsecond=0)
    return date.isoformat(timespec="minutes")


def subtract_datetime_mins(dt1: datetime, dt2: datetime) -> int:

    difference = int((dt2 - dt1).total_seconds()) * 60
    return difference


def get_time_zone_info(time_zone_str: Optional[str]) -> ZoneInfo:

    time_zone: Optional[ZoneInfo] = None
    try:
        time_zone_key = time_zone_str if time_zone_str is not None else "UTC"
        time_zone = ZoneInfo(time_zone_key)
        return time_zone
    except Exception as e:
        raise ValueError(f"Couldn't parse the time_zone {time_zone_str}: {e}")


def process_time_span(
    start_time: datetime, end_time: datetime, formatted_time_step: Optional[FormattedTimeStep], time_zone: ZoneInfo
) -> Tuple[datetime, datetime, FormattedTimeStep]:

    if formatted_time_step is None:
        formatted_time_step = get_formatted_time_step(start_time, end_time, time_zone)

    aligned_start_time = align_start_time(start_time, formatted_time_step).astimezone(time_zone)
    aligned_end_time = align_end_time(end_time, formatted_time_step, time_zone).astimezone(time_zone)

    return (aligned_start_time, aligned_end_time, formatted_time_step)


def get_formatted_time_step(start_time: datetime, end_time: datetime, time_zone: Optional[ZoneInfo] = None) -> FormattedTimeStep:

    if calculate_date_delta(start_time, FormattedTimeStep._1Y, time_zone) < end_time:
        return FormattedTimeStep._1Y

    elif calculate_date_delta(start_time, FormattedTimeStep._1M, time_zone) < end_time:
        return FormattedTimeStep._1M

    elif calculate_date_delta(start_time, FormattedTimeStep._1d, time_zone) < end_time:
        return FormattedTimeStep._1d

    elif calculate_date_delta(start_time, FormattedTimeStep._1h, time_zone) < end_time:
        return FormattedTimeStep._1h

    elif calculate_date_delta(start_time, FormattedTimeStep._15m, time_zone) < end_time:
        return FormattedTimeStep._15m

    else:
        return FormattedTimeStep._1m


def calculate_date_delta(start_time: datetime, formatted_time_step: FormattedTimeStep, time_zone: Optional[ZoneInfo] = None) -> datetime:

    if time_zone:
        arr_start = arrow.get(start_time).to(time_zone)
    else:
        arr_start = arrow.get(start_time).to("UTC")

    arr_end: Optional[arrow.Arrow] = None

    if formatted_time_step is FormattedTimeStep._1m:
        arr_end = arr_start.shift(minutes=1)

    elif formatted_time_step is FormattedTimeStep._15m:
        arr_end = arr_start.shift(minutes=15)

    elif formatted_time_step is FormattedTimeStep._1h:
        arr_end = arr_start.shift(hours=1)

    elif formatted_time_step is FormattedTimeStep._1d:
        arr_end = arr_start.shift(days=1)

    elif formatted_time_step is FormattedTimeStep._1M:
        arr_end = arr_start.shift(months=1)

    elif formatted_time_step is FormattedTimeStep._1Y:
        arr_end = arr_start.shift(years=1)

    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")

    end_time = arr_end.datetime
    return end_time


def time_step_grouping(reference_date: datetime, formatted_time_step: FormattedTimeStep, time_zone: Optional[ZoneInfo] = None) -> str:

    if formatted_time_step is FormattedTimeStep._1m:
        return "1m"

    elif formatted_time_step is FormattedTimeStep._15m:
        return "15m"

    elif formatted_time_step is FormattedTimeStep._1h:
        return "60m"

    elif formatted_time_step is FormattedTimeStep._1d:
        return "1d"

    elif formatted_time_step is FormattedTimeStep._1M:
        start_of_month = reference_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = calculate_date_delta(start_of_month, formatted_time_step, time_zone)
        days_in_month = (next_month - start_of_month).days
        return f"{days_in_month}d"

    elif formatted_time_step is FormattedTimeStep._1Y:
        start_of_year = reference_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        next_year = calculate_date_delta(start_of_year, formatted_time_step, time_zone)
        days_in_year = (next_year - start_of_year).days
        return f"{days_in_year}d"
    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")


def check_time_step(check: List[bool], time_step_01: FormattedTimeStep, time_step_02: FormattedTimeStep, current_time_step: FormattedTimeStep) -> bool:

    if time_step_01 == current_time_step:
        check[0] = True

    if time_step_02 == current_time_step:
        check[1] = True

    if check[0] and check[1]:
        return True
    
    return False


def bigger_time_step(time_step_01: FormattedTimeStep, time_step_02: FormattedTimeStep) -> FormattedTimeStep:
    if time_step_01 == time_step_02:
        return time_step_01
    
    time_step_check: List[bool] = [False, False]

    if check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._1m):
        return FormattedTimeStep._1m
    elif check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._15m):
        return FormattedTimeStep._15m
    elif check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._1h):
        return FormattedTimeStep._1h
    elif check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._1d):
        return FormattedTimeStep._1d
    elif check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._1M):
        return FormattedTimeStep._1M
    elif check_time_step(time_step_check, time_step_01, time_step_02, FormattedTimeStep._1Y):
        return FormattedTimeStep._1Y
    
    raise RuntimeError(f'One of the following time steps is invalid: {time_step_01}, {time_step_02}')


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


def align_end_time(end_time: datetime, formatted_time_step: FormattedTimeStep, time_zone: Optional[ZoneInfo] = None) -> datetime:

    if formatted_time_step is FormattedTimeStep._1m:
        if end_time.second == 0 and end_time.microsecond == 0:
            return end_time

    elif formatted_time_step is FormattedTimeStep._15m:
        if (end_time.minute % 15 == 0) and end_time.second == 0 and end_time.microsecond == 0:
            return end_time

    elif formatted_time_step is FormattedTimeStep._1h:
        if end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            return end_time

    elif formatted_time_step is FormattedTimeStep._1d:
        if end_time.hour == 0 and end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            return end_time

    elif formatted_time_step is FormattedTimeStep._1M:
        start_of_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if end_time == start_of_month:
            return end_time

    elif formatted_time_step is FormattedTimeStep._1Y:
        start_of_year = end_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        if end_time == start_of_year:
            return end_time

    else:
        raise ValueError(f"Unknown formatted time_step {formatted_time_step}.")

    return calculate_date_delta(align_start_time(end_time, formatted_time_step), formatted_time_step, time_zone)


def iterate_time_periods(
    start_time: datetime,
    end_time: datetime,
    formatted_time_step: FormattedTimeStep,
    time_zone: Optional[ZoneInfo] = None,
) -> Optional[Iterator[Tuple[datetime, str]]]:

    if formatted_time_step not in (FormattedTimeStep._1M, FormattedTimeStep._1Y):
        return None

    def _iterator() -> Iterator[Tuple[datetime, str]]:
        current_time = align_start_time(start_time, formatted_time_step)
        aligned_end_time = align_end_time(end_time, formatted_time_step)

        while current_time < aligned_end_time:
            group_by_time = time_step_grouping(current_time, formatted_time_step, time_zone)
            yield (current_time, group_by_time)
            current_time = calculate_date_delta(current_time, formatted_time_step, time_zone)

    return _iterator()


def find_bucket_for_time(time: datetime, aligned_buckets: List[Tuple[datetime, datetime]]) -> datetime:

    for bucket_start, bucket_end in aligned_buckets:

        if bucket_start <= time < bucket_end:
            return bucket_start

    raise ValueError(f"Didn't find an aligned bucket for time: {time}.")

def get_aligned_time_buckets(start_time: datetime, end_time: datetime, time_step: FormattedTimeStep, time_zone: Optional[ZoneInfo] = None) -> List[Tuple[datetime, datetime]]:
    
    time_buckets: List[Tuple[datetime, datetime]] = []

    current_st = start_time
    current_et = calculate_date_delta(current_st, time_step, time_zone)
    while current_st < end_time:
        time_buckets.append((current_st, current_et))
        current_st = calculate_date_delta(current_st, time_step, time_zone)
        current_et = calculate_date_delta(current_et, time_step, time_zone)
    
    return time_buckets