###########EXERTNAL IMPORTS############

import asyncio
from dotenv import load_dotenv
import os
import aiomqtt.client as mqtt
import json

#######################################

#############LOCAL IMPORTS#############

from util.debug import LoggerManager
import util.functions as functions

#######################################


class MQTTMessage:
    def __init__(self, qos: int, topic: str, payload: dict):
        self.qos = qos
        self.topic = topic
        self.payload = payload


class MQTTClient:

    @staticmethod
    def check_config_valid(config_file: str):
        load_dotenv(config_file)
        required = ["MQTT_CLIENT_ID", "MQTT_ADDRESS", "MQTT_PORT", "MQTT_USERNAME", "MQTT_PASSWORD_ENCRYPTED", "MQTT_PASSWORD_KEY"]
        missing = [var for var in required if os.getenv(var) is None]
        if missing:
            raise ValueError(f"Missing required MQTT config(s): {', '.join(missing)}")

    def __init__(self, config_file: str, topics_sub: set[str] = set()):

        MQTTClient.check_config_valid(config_file)
        self.id = os.getenv("MQTT_CLIENT_ID")
        self.address = os.getenv("MQTT_ADDRESS")
        self.port = int(os.getenv("MQTT_PORT"))
        self.username = os.getenv("MQTT_USERNAME")
        self.password = functions.decrypt_password(password_encrypted=os.getenv("MQTT_PASSWORD_ENCRYPTED"), key=os.getenv("MQTT_PASSWORD_KEY"))
        self.topics_sub = topics_sub

        self.publish_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.client = None

        asyncio.get_event_loop().create_task(self.handler_task())
        asyncio.get_event_loop().create_task(self.publish_task())

    async def handler_task(self):
        while True:
            try:
                self.client = mqtt.Client(hostname=self.address, port=self.port, username=self.username, password=self.password)
                async with self.client as client:
                    await self.subscribe_topics(client, self.topics_sub)
                    async for message in client.messages:
                        await self.receive_handler(str(message.topic), message.payload)
            except Exception as e:
                LoggerManager.get_logger(__name__).error(f"{e}")
            await asyncio.sleep(2)

    async def subscribe_topics(self, client: mqtt.Client, topics: set[str]):
        for topic in topics:
            await client.subscribe(topic)

    async def receive_handler(self, topic, payload):
        data = json.loads(payload)

    async def publish_task(self):
        await asyncio.sleep(1)
        while True:
            if self.client != None:
                try:
                    message: MQTTMessage = await self.publish_queue.get()
                    await self.client.publish(topic=message.topic, payload=json.dumps(message.payload), qos=message.qos)
                except Exception as e:
                    LoggerManager.get_logger(__name__).error(f"{e}")
            await asyncio.sleep(0)
