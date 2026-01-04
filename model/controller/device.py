###########EXTERNAL IMPORTS############

from enum import Enum
from datetime import datetime
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
    Placeholder for future meter-level configuration options.

    This class provides a structured container for energy meter settings and will
    be expanded in later versions to support protocol- or device-specific behavior.
    """

    def get_meter_options(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the current meter options.

        Returns:
            Dict[str, Any]: All configuration flags and values.
        """

        return asdict(self)

    @staticmethod
    def cast_from_dict(options_dict: Dict[str, Any]) -> "EnergyMeterOptions":
        """
        Construct EnergyMeterOptions from a persisted options dictionary.

        Currently returns an empty EnergyMeterOptions instance, as no concrete
        meter-level options are defined yet. This method exists to maintain a
        consistent casting interface and support future extensions.

        Raises:
            ValueError: If the options dictionary cannot be cast into valid
            energy meter options.
        """

        try:
            return EnergyMeterOptions()
        except Exception as e:
            raise ValueError(f"Couldn't cast dictionary into Energy Meter Options: {e}.")


@dataclass
class BaseCommunicationOptions:
    """
    Base type for protocol-specific communication option models.

    Acts as a common interface for all communication option subclasses.
    This class does not define any concrete attributes itself but provides
    shared behavior for serialization and type consistency across protocols.
    """

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
    Persistent representation of an energy meter configuration.

    Encapsulates the complete meter definition required for database storage
    and for reconstructing runtime EnergyMeter instances, including protocol,
    meter type, configuration options, communication settings, and node
    definitions.

    Attributes:
        name: Human-readable name of the energy meter.
        protocol: Communication protocol used by the meter.
        type: Electrical configuration of the meter (e.g. single-phase,
            three-phase).
        options: Meter-level configuration options controlling exposed
            measurements and behavior.
        communication_options: Protocol-specific connection settings.
        nodes: Set of node records associated with the meter.
        id: Identifier of the meter in the database, if assigned.
    """

    name: str
    protocol: Protocol
    type: EnergyMeterType
    options: EnergyMeterOptions
    communication_options: BaseCommunicationOptions
    nodes: Set[NodeRecord]
    id: Optional[int] = None


@dataclass
class DeviceHistoryStatus:
    """
    Represents the operational status and connection history of a device.

    Tracks connection events and record lifecycle timestamps for monitoring
    device availability and operational status over time.

    Attributes:
        last_seen (Optional[datetime]): Timestamp when was the device last seen.
        created_at (Optional[datetime]): Timestamp when the device was first created.
        updated_at (Optional[datetime]): Timestamp when the device was last modified.
    """

    last_seen: Optional[datetime]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    def get_status(self) -> Dict[str, Any]:
        """
        Returns a dictionary representation of the device history status.

        Returns:
            Dict[str, Any]: All status attributes as key-value pairs.
        """
        return asdict(self)
