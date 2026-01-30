###########EXTERNAL IMPORTS############

import asyncio
import logging

#######################################

#############LOCAL IMPORTS#############

from db.timedb import TimeDBClient
from db.db import SQLiteDBClient
from mqtt.client import MQTTClient
from controller.manager import DeviceManager
from web.server import HTTPServer
from util.debug import LoggerManager

# from meter_models.meters import get_orno_we_516_db, get_sm1238_db

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
    logger = LoggerManager.get_logger(__name__, level=logging.DEBUG)
    timedb_client = TimeDBClient(host="127.0.0.1", port=8010)
    sqlitedb_client = SQLiteDBClient()
    mqtt_client = MQTTClient(config_file="mqtt/client_options.env")
    device_manager = DeviceManager(
        publish_queue=mqtt_client.publish_queue, measurements_queue=timedb_client.write_queue, devices_db=sqlitedb_client
    )
    http_server = HTTPServer(
        host="0.0.0.0", port=8000, device_manager=device_manager, db=sqlitedb_client, timedb=timedb_client
    )

    try:
        # Create core infrastructure
        await timedb_client.init_connection()
        await sqlitedb_client.init_connection()
        # sqlitedb_client.insert_energy_meter(get_orno_we_516_db())
        # sqlitedb_client.insert_energy_meter(get_sm1238_db())
        await device_manager.start()
        await mqtt_client.start()
        await http_server.start()
        # Keep main loop alive to support background tasks
        while True:
            await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("Application shutdown by the user.")
    except Exception as e:
        logger.exception(f"Application failed to start: {e}")
    finally:
        await timedb_client.close_connection()
        await sqlitedb_client.close_connection()
        await mqtt_client.stop()
        await http_server.stop()
        await device_manager.stop()


if __name__ == "__main__":
    asyncio.run(async_main())
