###########EXTERNAL IMPORTS############

import pytest

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from controller.meter.device import EnergyMeter
from model.controller.general import Protocol
from controller.node.node import Node
from model.controller.node import NodeConfig, NodeType, NodeRecord
from controller.exceptions import NotImplemeted

#######################################


@pytest.fixture(autouse=True)
def reset_registry():
    original = ProtocolRegistry._registry.copy()
    ProtocolRegistry._registry.clear()
    yield
    ProtocolRegistry._registry = original


def test_missing_protocol():
    with pytest.raises(NotImplemeted):
        ProtocolRegistry.get_protocol_plugin(Protocol.MQTT)


def test_register_new_protocol_and_retrieve():
    class DummyOptions:
        pass

    class DummyMeter(EnergyMeter):
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            pass

        async def stop(self):
            pass

    def dummy_node_factory(record: NodeRecord) -> Node:
        return Node(NodeConfig(name="dummy", type=NodeType.FLOAT, unit=""))

    ProtocolRegistry.register_protocol(Protocol.MQTT, DummyMeter, DummyOptions, dummy_node_factory)
    plugin = ProtocolRegistry.get_protocol_plugin(Protocol.MQTT)
    assert plugin is not None
    assert plugin.meter_class is DummyMeter
    assert plugin.options_class is DummyOptions
    assert plugin.node_factory is dummy_node_factory
