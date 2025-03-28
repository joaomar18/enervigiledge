###########EXERTNAL IMPORTS############

import asyncio

#######################################

#############LOCAL IMPORTS#############

import util.functions as utilf
import data.nodes as nodes
from mqtt.client import MQTTClient
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter, ModbusRTUOptions
from controller.meter import EnergyMeterType, EnergyMeterOptions
import util.debug as debug

#######################################D


async def async_main():  # Main coroutine

    mqtt_client = MQTTClient(
        id="enervigiledge1",
        address="127.0.0.1",
        port=1883,
        username="admin",
        password="Estudante18.",
    )

    try:

        meter_orno_we_516_options = ModbusRTUOptions(
            slave_id=1,
            port="/dev/ttyAMA0",
            baudrate=9600,
            stopbits=1,
            parity="E",
            bytesize=8,
            read_period=5,
            timeout=1.0,
        )

        meter_orno_we_516 = ModbusRTUEnergyMeter(
            id=1,
            name="OR-WE-516 Energy Meter",
            publish_queue=mqtt_client.publish_queue,
            meter_type=EnergyMeterType.THREE_PHASE,
            meter_options=EnergyMeterOptions(
                read_energy_from_meter=True,
                read_separate_forward_reverse_energy=True,
                negative_reactive_power=False,
                frequency_reading=True,
            ),
            connection_options=meter_orno_we_516_options,
            nodes=nodes.get_orno_we_516_nodes(),
        )

    except Exception as e:
        debug.logger.error(e)

    while True:
        await asyncio.sleep(2)  # Sleeps for 2 seconds without stoping the event loop


if __name__ == "__main__":
    asyncio.run(async_main())