###########EXTERNAL IMPORTS############

from enum import Enum

#######################################

#############LOCAL IMPORTS#############

#######################################


class Protocol(str, Enum):
    """
    Enumeration of supported communication protocols.

    Attributes:
        NONE (str): No protocol specified.
        OPC_UA (str): OPC Unified Architecture protocol.
        MQTT (str): Message Queuing Telemetry Transport protocol.
        MODBUS_TCP (str): Modbus TCP/IP protocol.
        MODBUS_RTU (str): Modbus RTU serial protocol.
    """

    NONE = "NONE"
    OPC_UA = "OPC_UA"
    MQTT = "MQTT"
    MODBUS_TCP = "MODBUS_TCP"
    MODBUS_RTU = "MODBUS_RTU"

    @classmethod
    def valid_protocols(cls) -> set[str]:
        """
        Returns a set of all valid protocol string values.

        Returns:
            set[str]: Set containing all protocol enum values as strings.
        """
        return {p.value for p in cls}
