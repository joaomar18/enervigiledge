###########EXTERNAL IMPORTS############

from enum import Enum

#######################################

#############LOCAL IMPORTS#############

#######################################


##########     A B S T R A C T     E N U M S     ##########
class Protocol(str, Enum):
    """
    Enumeration of supported device communication protocols.
    """

    NONE = "NONE"
    OPC_UA = "OPC_UA"
    MQTT = "MQTT"
    MODBUS_TCP = "MODBUS_TCP"
    MODBUS_RTU = "MODBUS_RTU"

    @classmethod
    def valid_protocols(cls) -> set[str]:
        """Returns a set of all valid protocol string values."""
        return {p.value for p in cls}


##########     D E V I C E     A N D     E N E R G Y     M E T E R     E N U M S     ##########
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


##########     N O D E     E N U M S     ##########
class NodeType(str, Enum):
    """
    Enumeration of supported node data types.

    Attributes:
        INT (str): Integer data type.
        FLOAT (str): Floating point number.
        BOOL (str): Boolean value.
        STRING (str): String/text value.
    """

    INT = "INT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"
    STRING = "STRING"
