###########EXTERNAL IMPORTS############

from enum import Enum

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