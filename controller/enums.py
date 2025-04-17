###########EXTERNAL IMPORTS############

from enum import Enum

#######################################

#############LOCAL IMPORTS#############

#######################################


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