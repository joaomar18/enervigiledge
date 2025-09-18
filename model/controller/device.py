###########EXTERNAL IMPORTS############

from enum import Enum
import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Set

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol
from model.controller.node import NodeRecord

#######################################


class EnergyMeterType(str, Enum):
    """
    Enumeration of supported energy meter configurations.

    Attributes:
        SINGLE_PHASE (str): Single-phase meter.
        THREE_PHASE (str): Three-phase meter.
    """

    SINGLE_PHASE = "SINGLE_PHASE"
    THREE_PHASE = "THREE_PHASE"


class PowerFactorDirection(str, Enum):
    """
    Enumeration representing the direction of power factor.

    Attributes:
        UNKNOWN (str): Power factor direction is unknown.
        UNITARY (str): Power factor is unitary (1.0).
        LAGGING (str): Power factor is lagging (inductive load).
        LEADING (str): Power factor is leading (capacitive load).
    """

    UNKNOWN = "UNKNOWN"
    UNITARY = "UNITARY"
    LAGGING = "LAGGING"
    LEADING = "LEADING"


@dataclass
class EnergyMeterOptions:
    """
    Configuration options for how an energy meter should behave.

    Attributes:
        read_energy_from_meter (bool): Whether to read energy directly from the meter.
        read_separate_forward_reverse_energy (bool): Whether to track forward and reverse energy separately.
        negative_reactive_power (bool): Whether the meter reads negative (leading) reactive power.
        frequency_reading (bool): Whether the meter provides frequency readings.
    """

    read_energy_from_meter: bool
    read_separate_forward_reverse_energy: bool
    negative_reactive_power: bool
    frequency_reading: bool

    def get_meter_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current energy meter options.

        Returns:
            Dict[str, Any]: A dictionary with all configuration flags and their values.
        """
        return asdict(self)


@dataclass
class BaseCommunicationOptions:
    """
    Base configuration options for communication.

    Attributes:
        read_period (int): Interval in seconds between read operations. Defaults to 5.
        timeout (int): Timeout in seconds for communication attempts. Defaults to 5.
    """

    read_period: int = 5
    timeout: int = 5

    def get_communication_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the dataclass.

        Returns:
            Dict[str, Any]: A dictionary with all configuration flags and their values.
        """

        return asdict(self)


@dataclass
class EnergyMeterRecord:
    """
    Represents the full configuration of an energy meter for persistence in SQLite.

    Attributes:
        id (int): id of the device
        name (str): Human-readable name of the energy meter (e.g., "OR-WE-516 Energy Meter").
        protocol (Protocol): Communication protocol used by the meter (e.g., "modbus_rtu", "opcua").
        type (EnergyMeterType): Type of the meter, typically based on electrical configuration (e.g., "three_phase", "single_phase").
        options (Dict[str, Any]): Configuration options related to what the meter should read or expose (e.g., frequency, energy direction).
        communication_options (Dict[str, Any]): Protocol-specific connection settings (e.g., slave ID, port, URL, baudrate).
    """

    name: str
    protocol: Protocol
    type: EnergyMeterType
    options: Dict[str, Any]
    communication_options: Dict[str, Any]
    nodes: Set[NodeRecord]
    id: Optional[int] = None


@dataclass
class DeviceHistoryStatus:
    """
    Represents the operational status and connection history of a device.

    Tracks connection events and record lifecycle timestamps for monitoring
    device availability and operational status over time.

    Attributes:
        connection_on_datetime (Optional[datetime.datetime]): Timestamp when was the device last connection.
        connection_off_datetime (Optional[datetime.datetime]): Timestamp when was the device last disconnection.
        created_at (Optional[datetime.datetime]): Timestamp when the device was first created.
        updated_at (Optional[datetime.datetime]): Timestamp when the device was last modified.
    """

    connection_on_datetime: Optional[datetime.datetime]
    connection_off_datetime: Optional[datetime.datetime]
    created_at: Optional[datetime.datetime]
    updated_at: Optional[datetime.datetime]

    def get_status(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the device history status.

        Returns:
            Dict[str, Any]: All status attributes as key-value pairs.
        """
        return asdict(self)
