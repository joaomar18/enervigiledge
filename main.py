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
import util.debug as debug
import util.functions as functions

#######################################


async def async_main():  # Main coroutine
    
    timedb_client = TimeDBClient()
    
    print(timedb_client.check_db_exists("OR-WE-516 Energy Meter_1"))
    print(timedb_client.get_measurements_list("OR-WE-516 Energy Meter_1"))
    print(timedb_client.client._database)
    
    mqtt_client = MQTTClient(
        config_file = "mqtt/client_options.env"
    )

    device_manager = DeviceManager(publish_queue=mqtt_client.publish_queue)

    try:

        meter_orno_we_516_options = ModbusRTUOptions(
            slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1.0
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
        debug.logger.error(e)

    while True:
        await asyncio.sleep(2)  # Sleeps for 2 seconds without stoping the event loop


if __name__ == "__main__":
    asyncio.run(async_main())