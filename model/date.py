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
    """Parameters for specifying a time span with optional formatting and timezone.
    
    Attributes:
        start_time: Start of the time span
        end_time: End of the time span
        time_step: Step interval for formatted time spans
        formatted: Whether to use formatted time span behavior
        time_zone: Timezone for interpreting the time span
        force_aggregation: Whether to force aggregation of data (default: None)
    """

    start_time: Optional[datetime]
    end_time: Optional[datetime]
    time_step: Optional[FormattedTimeStep]
    formatted: Optional[bool]
    time_zone: Optional[ZoneInfo]
    force_aggregation: Optional[bool] = None
