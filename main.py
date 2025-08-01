###########EXERTNAL IMPORTS############

import asyncio

#######################################

#############LOCAL IMPORTS#############

from db.timedb import TimeDBClient
from db.db import SQLiteDBClient
from mqtt.client import MQTTClient
from controller.manager import DeviceManager
from web.server import HTTPServer
from util.debug import LoggerManager
#from data.nodes import get_orno_we_516_db, get_sm1238_db

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
    #sqlitedb_client.insert_energy_meter(get_orno_we_516_db())
    #sqlitedb_client.insert_energy_meter(get_sm1238_db())
    mqtt_client = MQTTClient(config_file="mqtt/client_options.env")
    device_manager = DeviceManager(publish_queue=mqtt_client.publish_queue, measurements_queue=timedb_client.write_queue, devices_db=sqlitedb_client)
    http_server = HTTPServer(host="0.0.0.0", port=8000, device_manager=device_manager, timedb=timedb_client)

    # Keep main loop alive to support background tasks
    while True:
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(async_main())
