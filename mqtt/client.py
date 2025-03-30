###########EXERTNAL IMPORTS############

import asyncio
import aiomqtt.client as mqtt
import json

#######################################

#############LOCAL IMPORTS#############

import util.debug as debug

#######################################


class MQTTMessage:
    def __init__(self, qos: int, topic: str, payload: dict):
        self.qos = qos
        self.topic = topic
        self.payload = payload


class MQTTClient:
    def __init__(
        self,
        id: str,
        address: str,
        port: int,
        username: str,
        password: str,
        topics_sub: set[str] = set(),
    ):

        self.id = id
        self.address = address
        self.port = port
        self.username = username
        self.password = password
        self.topics_sub = topics_sub

        self.publish_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.client = None

        asyncio.get_event_loop().create_task(self.handler_task())
        asyncio.get_event_loop().create_task(self.publish_task())

    async def handler_task(self):
        while True:
            try:
                self.client = mqtt.Client(
                    hostname=self.address,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )
                async with self.client as client:
                    await self.subscribe_topics(client, self.topics_sub)
                    async for message in client.messages:
                        await self.receive_handler(str(message.topic), message.payload)
            except Exception as e:
                debug.logger.error(f"MQTT Client - Handler Task: {e}")
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
                    await self.client.publish(
                        topic=message.topic, payload=json.dumps(message.payload), qos=message.qos
                    )
                except Exception as e:
                    debug.logger.error(f"MQTT Client - Publish Task: {e}")
            await asyncio.sleep(0)
