###########EXERTNAL IMPORTS############

import asyncio

#######################################

#############LOCAL IMPORTS#############

import data.nodes as nodes
from db.timedb import TimeDBClient
from mqtt.client import MQTTClient
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter, ModbusRTUOptions
from controller.device import DeviceManager
from controller.meter import EnergyMeterType, EnergyMeterOptions
from web.server import HTTPServer
from util.debug import LoggerManager
import util.functions as functions

#######################################


async def async_main():  # Main coroutine
    LoggerManager.init()
    timedb_client = TimeDBClient()
    mqtt_client = MQTTClient(config_file="mqtt/client_options.env")
    device_manager = DeviceManager(publish_queue=mqtt_client.publish_queue)
    http_server = HTTPServer(host="0.0.0.0", port=8000, device_manager=device_manager, timedb=timedb_client)
    try:
        meter_orno_we_516_options = ModbusRTUOptions(
            slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1, retries=0
        )
        meter_orno_we_516 = ModbusRTUEnergyMeter(
            id=1,
            name="OR-WE-516 Energy Meter",
            publish_queue=mqtt_client.publish_queue,
            measurements_queue=timedb_client.write_queue,
            meter_type=EnergyMeterType.THREE_PHASE,
            meter_options=EnergyMeterOptions(
                read_energy_from_meter=True, read_separate_forward_reverse_energy=True, negative_reactive_power=False, frequency_reading=True
            ),
            connection_options=meter_orno_we_516_options,
            nodes=nodes.get_orno_we_516_nodes(),
        )

        device_manager.add_device(meter_orno_we_516)

    except Exception as e:
        LoggerManager.get_logger(__name__).error(e)

    while True:
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(async_main())
