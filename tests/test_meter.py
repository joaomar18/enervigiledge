###########EXTERNAL IMPORTS############

import asyncio
import types
import pytest
from typing import Set

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from controller.meter.meter import EnergyMeter
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType
from model.controller.node import NodeType, NodeConfig
from db.db import EnergyMeterRecord
from mqtt.client import MQTTMessage

#######################################


class DummyMeter(EnergyMeter):
    def __init__(self, nodes: Set[Node]):
        self.name = "meter"
        self.id = 1
        self.publish_queue = asyncio.Queue()
        self.measurements_queue = asyncio.Queue()
        self.meter_nodes = types.SimpleNamespace(nodes={n.config.name: n for n in nodes})

    async def start(self):
        pass

    async def stop(self):
        pass

    def get_meter_record(self) -> EnergyMeterRecord:
        return EnergyMeterRecord(
            id=self.id,
            name=self.name,
            protocol=Protocol.MODBUS_RTU,  # Dummy protocol
            type=EnergyMeterType.THREE_PHASE,  # Dummy type
            options={},  # Dummy options
            communication_options={},  # Dummy connection options
            nodes=set(),  # Dummy nodes
        )


@pytest.mark.asyncio
async def test_publish_nodes_filters_and_formats():
    node1 = Node(NodeConfig("voltage", NodeType.FLOAT, "V", publish=True))
    node1.processor.set_value(10)
    node2 = Node(NodeConfig("current", NodeType.FLOAT, "A", publish=True))
    node3 = Node(NodeConfig("frequency", NodeType.FLOAT, "Hz", publish=False))
    node3.processor.set_value(50)

    meter = DummyMeter({node1, node2, node3})
    await meter.publish_nodes()
    msg: MQTTMessage = await meter.publish_queue.get()
    assert msg.topic == "meter_1_nodes"
    assert msg.payload == {"voltage": node1.get_publish_format()}
    assert meter.publish_queue.empty()
