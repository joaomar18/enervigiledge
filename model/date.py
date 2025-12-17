###########EXTERNAL IMPORTS############

from enum import Enum
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo

#######################################

#############LOCAL IMPORTS#############

#######################################


class FormattedTimeStep(str, Enum):
    """Time step intervals for formatted log data."""

    _1m = "1m"  # 1 Minute
    _15m = "15m"  # 15 Minutes
    _1h = "1h"  # 1 Hour
    _1d = "1d"  # 1 Day
    _1M = "1M"  # 1 Month
    _1Y = "1Y"  # 1 Year


@dataclass
class TimeSpanParameters:
    """
    Defines a normalized time span configuration for time-series queries.

    Holds validated start and end timestamps, optional formatting and
    aggregation settings, and timezone information used when querying
    and processing time-series data.

    Attributes:
        start_time: Start of the time span (timezone-aware datetime), or None.
        end_time: End of the time span (timezone-aware datetime), or None.
        time_step: Optional step interval used for formatted queries.
        formatted: Indicates whether formatted time span behavior is enabled.
        time_zone: Time zone used for interpreting timestamps.
        force_aggregation: Optional flag to force aggregation of data.
    """

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    time_step: Optional[FormattedTimeStep] = None
    formatted: Optional[bool] = None
    time_zone: Optional[ZoneInfo] = None
    force_aggregation: Optional[bool] = None
