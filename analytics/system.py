###########EXTERNAL IMPORTS############

import ctypes
import threading
import multiprocessing
from multiprocessing import shared_memory
from multiprocessing.synchronize import Event
from typing import List, Tuple, Optional
import psutil
import math

#######################################

#############LOCAL IMPORTS#############

from model.struct.sliding_window import SlidingWindow
import util.functions.date as date
from model.analytics.system import RealTimeSystemData

#######################################


class SharedSystemData(ctypes.Structure):
    """
    ctypes-based shared memory structure holding raw system metrics.

    This structure defines the exact binary layout used to exchange
    system metrics between processes via shared memory. The layout is
    intentionally simple and compact to minimize IPC overhead and avoid
    serialization.

    The structure is written exclusively by a dedicated writer process
    and read by the main process. All consumers must treat this structure
    as read-only. Access should be performed through the provided
    properties, which expose Python-native types and handle NaN semantics
    where applicable.

    Numeric representation:
    - Percentage values are stored as float32.
    - Absolute resource values (RAM, disk) are stored as signed integers
      representing bytes.
    - Missing or unavailable numeric values are encoded as NaN where
      applicable.

    Fields:
        _cpu_usage (float): CPU usage percentage (0–100).
        _ram_usage (int): Used RAM in bytes.
        _total_ram (int): Total RAM in bytes.
        _disk_usage (int): Used disk space in bytes.
        _disk_total (int): Total disk space in bytes.
        _cpu_temperature (float): CPU temperature in degrees Celsius,
            or NaN if not available.
    """

    _fields_ = [
        ("_cpu_usage", ctypes.c_float),
        ("_ram_usage", ctypes.c_uint64),
        ("_total_ram", ctypes.c_uint64),
        ("_disk_usage", ctypes.c_uint64),
        ("_disk_total", ctypes.c_uint64),
        ("_cpu_temperature", ctypes.c_float),
    ]

    @property
    def cpu_usage(self) -> float:
        return float(self._cpu_usage)

    @property
    def ram_usage(self) -> int:
        return int(self._ram_usage)

    @property
    def total_ram(self) -> int:
        return int(self._total_ram)

    @property
    def disk_usage(self) -> int:
        return int(self._disk_usage)

    @property
    def disk_total(self) -> int:
        return int(self._disk_total)

    @property
    def cpu_temperature(self) -> float:
        return float(self._cpu_temperature)


class SystemMonitor:
    """
    High-level system monitoring controller.

    This class coordinates a background writer process and a listener
    thread to collect and consume real-time system metrics with minimal
    overhead.

    Responsibilities:
    - Owns and manages the shared memory region.
    - Spawns a writer process responsible for sampling system metrics.
    - Runs a listener thread that reacts to update events and updates
      in-memory data structures.
    - Provides accessors for historical and real-time monitoring data.

    The monitor is explicitly started and stopped using `start()` and
    `stop()`. No background activity occurs until `start()` is called.

    Design notes:
    - Processes do not share Python objects; all cross-process data
      exchange happens through shared memory.
    - Threads are used only within the main process and may safely
      access instance state.
    - Events are used for synchronization instead of polling or locks.
    """

    POLLING_INTERVAL_SECONDS = 1
    DATA_SIZE_SECONDS = 60

    def __init__(self):
        self.cpu_usage_perc: SlidingWindow[float] = SlidingWindow(max_size=self.DATA_SIZE_SECONDS)
        self.ram_usage_perc: SlidingWindow[float] = SlidingWindow(max_size=self.DATA_SIZE_SECONDS)
        self.realtime_data = RealTimeSystemData(
            boot_date=date.to_iso(date.get_date_from_timestamp(int(psutil.boot_time()) * 1000))
        )
        self.shared_memory: Optional[shared_memory.SharedMemory] = None
        self.update_event = multiprocessing.Event()
        self.stop_writer_event = multiprocessing.Event()
        self.stop_listener_event = threading.Event()
        self.writer_process: Optional[multiprocessing.Process] = None
        self.listener_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """
        Starts the system monitoring infrastructure.

        This method:
        - Clears all synchronization events.
        - Spawns the writer process responsible for collecting system metrics.
        - Starts the listener thread that waits for update events and
        consumes shared memory data.

        Raises:
            RuntimeError: If the monitor is already running.
        """

        if self.writer_process is not None or self.listener_thread is not None or self.shared_memory is not None:
            raise RuntimeError("SystemMonitor is already running.")

        self.update_event.clear()
        self.stop_writer_event.clear()
        self.stop_listener_event.clear()
        self.shared_memory = shared_memory.SharedMemory(create=True, size=ctypes.sizeof(SharedSystemData))
        self.writer_process = multiprocessing.Process(
            target=SystemMonitor._writer,
            args=(self.shared_memory.name, self.POLLING_INTERVAL_SECONDS, self.update_event, self.stop_writer_event),
        )
        self.listener_thread = threading.Thread(target=self._listener, daemon=True)
        self.writer_process.start()
        self.listener_thread.start()

    def stop(self) -> None:
        """
        Stops the system monitoring infrastructure.

        This method signals both the writer process and the listener thread
        to terminate, wakes any blocked wait operations, and waits for all
        background execution to complete before returning.

        Raises:
            RuntimeError: If the monitor is not currently running.
        """

        if self.writer_process is None or self.listener_thread is None or self.shared_memory is None:
            raise RuntimeError("SystemMonitor is not running.")

        self.stop_writer_event.set()
        self.stop_listener_event.set()
        self.update_event.set()
        self.writer_process.join()
        self.listener_thread.join()
        self.shared_memory.close()
        self.shared_memory.unlink()
        self.writer_process = None
        self.listener_thread = None
        self.shared_memory = None

    @staticmethod
    def _writer(shared_mem_name: str, polling_interval: int, update_event: Event, stop_event: Event) -> None:
        """
        Background writer process entry point.

        This function runs in a separate process and is responsible for
        periodically sampling system metrics and writing them into shared
        memory.

        After each update, it signals the main process via `update_event`.
        The process terminates cleanly when `stop_event` is set.

        Args:
            shared_mem_name: Name of the shared memory block to attach to.
            polling_interval: Update interval in seconds.
            update_event: Event used to notify the listener of new data.
            stop_event: Event used to request process termination.
        """

        shared_mem = shared_memory.SharedMemory(name=shared_mem_name)
        buffer = shared_mem.buf
        assert buffer is not None
        data = SharedSystemData.from_buffer(buffer)
        try:
            while not stop_event.is_set():
                data._cpu_usage = SystemMonitor.get_cpu_performance(interval=0.1)
                data._ram_usage, data._total_ram = SystemMonitor.get_ram_performance()
                data._disk_usage, data._disk_total = SystemMonitor.get_disk_performance()
                data._cpu_temperature = SystemMonitor.get_cpu_temperature()
                update_event.set()
                stop_event.wait(timeout=polling_interval)
        finally:
            del data
            shared_mem.close()

    def _listener(self) -> None:
        """
        Background listener thread entry point.

        This method runs in a dedicated thread within the main process.
        It blocks on `update_event` and, when signaled, reads the latest
        system metrics from shared memory and updates local data structures.

        The thread exits cleanly when `stop_listener_event` is set.
        """

        assert self.shared_memory is not None and self.shared_memory.buf is not None
        data = SharedSystemData.from_buffer(self.shared_memory.buf)
        try:
            while not self.stop_listener_event.is_set():
                self.update_event.wait()
                self.update_event.clear()
                if self.stop_listener_event.is_set():
                    break

                cpu_use_perc = round(data.cpu_usage, 2)
                ram_use_perc = round((data.ram_usage / data.total_ram) * 100, 2)
                ram_usage = data.ram_usage
                total_ram = data.total_ram
                disk_usage = data.disk_usage
                disk_total = data.disk_total
                cpu_temp = round(data.cpu_temperature, 2) if not math.isnan(data.cpu_temperature) else None

                self.cpu_usage_perc.add(cpu_use_perc)
                self.ram_usage_perc.add(ram_use_perc)
                self.realtime_data.ram_usage = ram_usage
                self.realtime_data.total_ram = total_ram
                self.realtime_data.disk_usage = disk_usage
                self.realtime_data.disk_total = disk_total
                self.realtime_data.cpu_temp = cpu_temp
        finally:
            del data

    def get_cpu_usage_history(self) -> List[float]:
        """
        Returns the historical CPU usage values.

        Returns:
            A list of CPU usage percentages collected over the configured
            time window.
        """

        return self.cpu_usage_perc.get_list()

    def get_ram_usage_history(self) -> List[float]:
        """
        Returns the historical RAM usage values.

        Returns:
            A list of RAM usage percentages collected over the configured
            time window.
        """

        return self.ram_usage_perc.get_list()

    def get_realtime_data(self) -> RealTimeSystemData:
        """
        Returns the latest real-time system metrics snapshot.

        Returns:
            A RealTimeSystemData instance containing the most recent values.
        """

        return self.realtime_data

    @staticmethod
    def get_cpu_temperature() -> float:
        """
        Retrieves the current CPU temperature.

        Returns:
            Maximum CPU core temperature in degrees Celsius.
            Returns NaN if the temperature cannot be determined.
        """

        # Standard psutil method
        temps = psutil.sensors_temperatures()
        if temps:
            for key in ("coretemp", "cpu_thermal", "soc_thermal"):
                entries = temps.get(key)
                if entries:
                    values = [t.current for t in entries if t.current is not None]
                    if values:
                        return max(values)

        # Raspberry Pi specific method
        try:
            with open("/sys/class/thermal/thermal_zone0/temp") as f:
                return int(f.read().strip()) / 1000.0
        except Exception:
            return math.nan

    @staticmethod
    def get_ram_performance() -> Tuple[int, int]:
        """
        Retrieves the current RAM usage of the system.
        Returns:
            A tuple containing used RAM and total RAM in bytes.
        """

        virtual_memory = psutil.virtual_memory()
        return (virtual_memory.used, virtual_memory.total)

    @staticmethod
    def get_cpu_performance(interval: Optional[float] = None) -> float:
        """
        Retrieves the current CPU usage percentage of the system.
        Returns:
            CPU usage as a float representing the overall CPU usage percentage.
        """

        return psutil.cpu_percent(interval)

    @staticmethod
    def get_disk_performance() -> Tuple[int, int]:
        """
        Retrieves the current disk usage of the root partition.
        Returns:
            A tuple containing used disk space and total disk space in bytes.
        """

        disk_usage = psutil.disk_usage("/")
        return (disk_usage.used, disk_usage.total)
