############### EXTERNAL IMPORTS ###############

import asyncio
from asyncua import Client, ua
from dataclasses import dataclass
from typing import Optional, Set
import logging

############### LOCAL IMPORTS ###############

from util.debug import LoggerManager
from controller.device import Protocol
from controller.node import Node, NodeType
from controller.meter import EnergyMeter, EnergyMeterType, EnergyMeterOptions

LoggerManager.get_logger(__name__).setLevel(logging.INFO)


@dataclass
class OPCUAOptions:
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    read_period: int = 5
    timeout: int = 5


class OPCUANode(Node):
    def __init__(
        self,
        name: str,
        type: NodeType,
        node_id: str,
        unit: str,
        publish: bool = True,
        calculated: bool = False,
        logging: bool = False,
        logging_period: int = 15,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
    ):
        super().__init__(
            name=name,
            type=type,
            unit=unit,
            publish=publish,
            calculated=calculated,
            logging=logging,
            logging_period=logging_period,
            min_alarm=min_alarm,
            max_alarm=max_alarm,
            min_alarm_value=min_alarm_value,
            max_alarm_value=max_alarm_value,
        )
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state


class OPCUAEnergyMeter(EnergyMeter):
    def __init__(
        self,
        id: int,
        name: str,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        connection_options: OPCUAOptions,
        nodes: Optional[Set[Node]] = None,
    ):
        super().__init__(
            id=id,
            name=name,
            protocol=Protocol.OPC_UA,
            publish_queue=publish_queue,
            measurements_queue=measurements_queue,
            meter_type=meter_type,
            meter_options=meter_options,
            meter_nodes=nodes if nodes else set(),
        )

        self.connection_options = connection_options
        self.client = Client(url=self.connection_options.url, timeout=self.connection_options.timeout)

        if self.connection_options.username:
            self.client.set_user(self.connection_options.username)
            self.client.set_password(self.connection_options.password)

        self.nodes = nodes if nodes else set()
        self.opcua_nodes: Set[OPCUANode] = {node for node in self.nodes if isinstance(node, OPCUANode)}
        self.connection_open = False
        self.start()

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        self.connection_task = loop.create_task(self.manage_connection())
        self.receiver_task = loop.create_task(self.receiver())

    async def manage_connection(self):
        logger = LoggerManager.get_logger(__name__)

        while True:
            try:
                logger.info(f"Trying to connect OPC UA client {self.name} with id {self.id}...")
                await self.client.connect()
                self.connection_open = True
                logger.info(f"Client {self.name} with id {self.id} connected")

                while self.connection_open:
                    await asyncio.sleep(3)

                logger.warning(f"Client {self.name} with id {self.id} disconnected")

            except Exception as e:
                logger.error(f"Connection error: {e}")
                await self.close_connection()
                await asyncio.sleep(3)

    async def receiver(self):
        logger = LoggerManager.get_logger(__name__)

        while True:
            try:
                if self.connection_open:
                    tasks = [asyncio.create_task(self.read_node(node)) for node in self.opcua_nodes]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    failed_nodes = []

                    for node, result in zip(self.opcua_nodes, results):
                        if isinstance(result, Exception):
                            failed_nodes.append(node.name)
                            node.set_value(None)
                            continue
                        node.set_value(result)

                    if failed_nodes:
                        logger.warning(f"Failed to read {len(failed_nodes)} nodes from {self.name}: {', '.join(failed_nodes)}")

                    if any(node.connected for node in self.opcua_nodes):
                        self.set_connection_state(True)
                    else:
                        self.set_connection_state(False)

                    await self.process_nodes()

            except Exception as e:
                logger.exception(f"Receiver error: {e}")
                await self.close_connection()

            await asyncio.sleep(self.connection_options.read_period)

    async def read_float(self, node: OPCUANode):
        try:
            opc_node = self.client.get_node(node.node_id)
            value = await opc_node.read_value()
            node.set_connection_state(True)
            return float(value)
        except Exception as e:
            node.set_connection_state(False)
            raise Exception(f"Failed to read {node.name} from {self.name}") from e

    async def close_connection(self):
        self.set_connection_state(False)
        try:
            await self.client.disconnect()
        except Exception:
            pass
        self.connection_open = False
