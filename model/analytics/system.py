###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Dict, Union, Optional

#######################################

#############LOCAL IMPORTS#############

#######################################


@dataclass
class RealTimeSystemData:
    """
    Container for the latest real-time system metrics snapshot.

    This class represents a point-in-time view of system performance and
    resource usage. All fields are optional to allow partial updates and
    graceful handling of unavailable metrics.

    Instances of this class are typically updated by a background
    monitoring component and consumed by UI layers, APIs, or logging
    systems. The data is intended to be read-only by consumers.

    Attributes:
        cpu_use_perc: Current CPU usage percentage (0–100).
        ram_use_perc: Current RAM usage percentage (0–100).
        ram_usage: Amount of RAM currently in use, in bytes.
        total_ram: Total available RAM, in bytes.
        disk_usage: Amount of disk space currently in use, in bytes.
        disk_total: Total available disk space, in bytes.
        cpu_temp: Current CPU temperature in degrees Celsius.
        boot_date: System boot date/time in ISO 8601 string format.
    """

    cpu_use_perc: Optional[float] = None
    ram_use_perc: Optional[float] = None
    ram_usage: Optional[int] = None
    total_ram: Optional[int] = None
    disk_usage: Optional[int] = None
    disk_total: Optional[int] = None
    cpu_temp: Optional[float] = None
    boot_date: Optional[str] = None

    def get_data(self) -> Dict[str, Optional[Union[float, int, str]]]:
        """
        Returns the current system metrics as a dictionary.

        This method provides a serialization-friendly representation of
        the real-time system data, suitable for JSON encoding, API
        responses, or templating.

        Returns:
            A dictionary mapping metric names to their current values.
            Fields with unavailable data are represented as None.
        """

        return {
            "cpu_use_perc": self.cpu_use_perc,
            "ram_use_perc": self.ram_use_perc,
            "ram_usage": self.ram_usage,
            "total_ram": self.total_ram,
            "disk_usage": self.disk_usage,
            "disk_total": self.disk_total,
            "cpu_temperature": self.cpu_temp,
            "boot_date": self.boot_date,
        }
