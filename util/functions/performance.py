###########EXTERNAL IMPORTS############

from typing import Tuple
import psutil

#######################################

#############LOCAL IMPORTS#############

#######################################


def get_ram_performance() -> Tuple[int, int]:
    """
    Retrieves the current RAM usage of the system.
    Returns:
        A tuple containing used RAM and total RAM in bytes.
    """

    virtual_memory = psutil.virtual_memory()
    return (virtual_memory.used, virtual_memory.total)


def get_cpu_performance() -> float:
    """
    Retrieves the current CPU usage percentage of the system.
    Returns:
        CPU usage as a float percentage.
    """

    return psutil.cpu_percent()


def get_disk_performance() -> Tuple[int, int]:
    """
    Retrieves the current disk usage of the root partition.
    Returns:
        A tuple containing used disk space and total disk space in bytes.
    """

    disk_usage = psutil.disk_usage('/')
    return (disk_usage.used, disk_usage.total)