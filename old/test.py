from pymodbus.client import ModbusSerialClient as ModbusRTUClient
import struct

from pymodbus.client import ModbusSerialClient

client = ModbusSerialClient(
    port="/dev/ttyAMA0",
    baudrate=9600,
    stopbits=1,
    parity="E",
    bytesize=8,
    timeout=1.0
)

client.connect()

try:
    print("Requesting", flush=True)
    response = client.read_holding_registers(address=0x000E, count=2, slave=1)
    print("Response:", response, flush=True)
except Exception as e:
    print("Exception happened:", e)