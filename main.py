###########EXERTNAL IMPORTS############

import asyncio

#######################################

#############LOCAL IMPORTS#############

import data.nodes as nodes
from db.timedb import TimeDBClient
from db.db import SQLiteDBClient
from mqtt.client import MQTTClient
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter, ModbusRTUOptions
from protocol.opcua.opcua_device import OPCUAEnergyMeter, OPCUAOptions
from controller.device import DeviceManager
from controller.meter import EnergyMeterType, EnergyMeterOptions
from web.server import HTTPServer
from util.debug import LoggerManager

#######################################


async def async_main():
    """
    Main asynchronous entry point for the application.

    Responsibilities:
        - Initializes logging, database, MQTT, and HTTP server components.
        - Creates and registers energy meter devices.
        - Keeps the event loop alive to support background tasks (e.g., MQTT, HTTP, write queues).
    """

    # Initialize global logger
    LoggerManager.init()

    # Create core infrastructure
    timedb_client = TimeDBClient()
    sqlitedb_client = SQLiteDBClient()
    mqtt_client = MQTTClient(config_file="mqtt/client_options.env")
    device_manager = DeviceManager(publish_queue=mqtt_client.publish_queue)
    http_server = HTTPServer(host="0.0.0.0", port=8000, device_manager=device_manager, timedb=timedb_client)

    try:
        modbus_meter = ModbusRTUEnergyMeter(
            id=1,
            name="OR-WE-516 Energy Meter",
            publish_queue=mqtt_client.publish_queue,
            measurements_queue=timedb_client.write_queue,
            meter_type=EnergyMeterType.THREE_PHASE,
            meter_options=EnergyMeterOptions(
                read_energy_from_meter=True, read_separate_forward_reverse_energy=True, negative_reactive_power=False, frequency_reading=True
            ),
            connection_options=ModbusRTUOptions(
                slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1, retries=0
            ),
            nodes=nodes.get_orno_we_516_nodes(),
        )

        opcua_meter = OPCUAEnergyMeter(
            id=2,
            name="SM1238 S7-1200 Meter",
            publish_queue=mqtt_client.publish_queue,
            measurements_queue=timedb_client.write_queue,
            meter_type=EnergyMeterType.THREE_PHASE,
            meter_options=EnergyMeterOptions(
                read_energy_from_meter=False, read_separate_forward_reverse_energy=False, negative_reactive_power=True, frequency_reading=True
            ),
            connection_options=OPCUAOptions(url="opc.tcp://192.168.10.10:4840"),
            nodes=nodes.get_sm1238_nodes(),
        )

        device_manager.add_device(modbus_meter)
        device_manager.add_device(opcua_meter)

    except Exception as e:
        LoggerManager.get_logger(__name__).error(f"Failed to initialize devices: {e}")

    # Keep main loop alive to support background tasks
    while True:
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(async_main())
