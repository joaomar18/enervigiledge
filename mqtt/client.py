###########EXTERNAL IMPORTS############

import asyncio
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Optional, Dict
import os
import aiomqtt.client as mqtt
import json

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
import util.functions.auth as auth_util
import util.functions.objects as objects

#######################################


@dataclass
class MQTTMessage:
    """
    Simple container for MQTT message data.

    Attributes:
        qos (int): Quality of Service level for the message.
        topic (str): Topic to which the message will be published.
        payload (Dict): Message content.
    """

    qos: int
    topic: str
    payload: Dict


class MQTTClient:
    """
    Asynchronous MQTT client that handles connection and publishing messages through an internal queue.
    """

    @staticmethod
    def check_config_valid(config_file: str) -> None:
        """
        Loads the environment and validates required MQTT settings.

        Args:
            config_file (str): Path to the .env config file.

        Raises:
            ValueError: If any required setting is missing.
        """

        load_dotenv(config_file)
        required = [
            "ENABLED",
            "USE_AUTHENTICATION",
            "MQTT_CLIENT_ID",
            "MQTT_ADDRESS",
            "MQTT_PORT",
            "MQTT_USERNAME",
            "MQTT_PASSWORD_ENCRYPTED",
            "MQTT_PASSWORD_KEY",
        ]
        missing = [var for var in required if os.getenv(var) is None]
        if missing:
            raise ValueError(f"Missing required MQTT config(s): {', '.join(missing)}")

    def __init__(self, config_file: str):
        """
        Initializes the MQTT client using a .env configuration file.

        Args:
            config_file (str): Path to the environment file with MQTT credentials.
        """

        MQTTClient.check_config_valid(config_file)

        self.enabled = objects.require_env_variable("ENABLED").upper() == "TRUE"
        self.use_authentication = objects.require_env_variable("USE_AUTHENTICATION").upper() == "TRUE"
        self.id = objects.require_env_variable("MQTT_CLIENT_ID")
        self.address = objects.require_env_variable("MQTT_ADDRESS")
        self.port = int(objects.require_env_variable("MQTT_PORT"))
        self.username = objects.require_env_variable("MQTT_USERNAME")
        self.password = auth_util.decrypt_password(
            password_encrypted=objects.require_env_variable("MQTT_PASSWORD_ENCRYPTED"),
            key=objects.require_env_variable("MQTT_PASSWORD_KEY"),
        )
        self.publish_queue: asyncio.Queue[MQTTMessage] = asyncio.Queue(maxsize=1000)
        self.publish_task: Optional[asyncio.Task] = None
        self.client: Optional[mqtt.Client] = None

    async def start(self) -> None:
        """
        Starts background tasks for MQTT handling and publishing.
        """

        if not self.enabled:
            raise NotImplementedError("MQTT client is not enabled in the configuration")
        if self.client is not None or self.publish_task is not None:
            raise RuntimeError("Client or publish task are already instantiated")
        loop = asyncio.get_event_loop()
        if self.use_authentication:
            self.client = mqtt.Client(hostname=self.address, port=self.port, username=self.username, password=self.password)
        else:
            self.client = mqtt.Client(hostname=self.address, port=self.port)
        self.publish_task = loop.create_task(self.publisher())

    async def stop(self) -> None:
        """
        Stops the MQTT client by cancelling the publish task.
        """

        try:
            if self.publish_task:
                self.publish_task.cancel()
                await self.publish_task
                self.publish_task = None

            if self.client:
                self.client = None
        except asyncio.CancelledError:
            pass

    def __require_client(self) -> mqtt.Client:
        """
        Return the active mqtt client connection.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.client is None:
            raise RuntimeError(f"MQTT client is not instantiated properly. ")
        return self.client

    async def publisher(self) -> None:
        """
        Publishes messages from the internal queue to the MQTT broker.

        Automatically connects and reconnects to the broker.
        Upon the first successful connection, clears any queued messages
        to avoid publishing stale device state.

        This method runs indefinitely in the background.
        """

        logger = LoggerManager.get_logger(__name__)
        mqtt_client = self.__require_client()

        while True:
            try:
                async with mqtt_client as client:
                    logger.info("Connected to the MQTT broker.")
                    self.clear_queue()
                    while True:
                        message: MQTTMessage = await self.publish_queue.get()
                        await client.publish(topic=message.topic, payload=json.dumps(message.payload), qos=message.qos)
                        logger.debug(f"Published to topic {message.topic}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"MQTT publish task error: {e}")
                await asyncio.sleep(2)

    def clear_queue(self) -> None:
        """
        Clears all pending messages from the publish queue.

        Intended to be called right after a (re)connection to the MQTT broker,
        to prevent publishing outdated or irrelevant messages.
        """

        while not self.publish_queue.empty():
            try:
                self.publish_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
