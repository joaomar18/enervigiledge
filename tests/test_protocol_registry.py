###########EXTERNAL IMPORTS############

import pytest
from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from controller.meter.device import EnergyMeter
from model.controller.general import Protocol
from controller.node.node import Node
from model.controller.device import BaseCommunicationOptions
from model.controller.node import NodeConfig, NodeType, NodeRecord, BaseNodeRecordConfig, BaseNodeProtocolOptions, NodeAttributes

#######################################


@pytest.fixture(autouse=True)
def reset_registry():
    original = ProtocolRegistry._registry.copy()
    ProtocolRegistry._registry.clear()
    yield
    ProtocolRegistry._registry = original


def test_missing_protocol():
    with pytest.raises(NotImplementedError):
        ProtocolRegistry.get_protocol_plugin(Protocol.MQTT)


def test_register_new_protocol_and_retrieve():
    class DummyOptions(BaseCommunicationOptions):
        pass

    class DummyNodeProtocolOptions(BaseNodeProtocolOptions):
        pass

    class DummyMeter(EnergyMeter):
        def __init__(self, *args, **kwargs):
            pass

        async def start(self):
            pass

        async def stop(self):
            pass

    def dummy_node_factory(record: NodeRecord) -> Node:
        return Node(NodeConfig(name="dummy", type=NodeType.FLOAT, unit=""), DummyNodeProtocolOptions())

    def dummy_node_record_factory(
        name: str,
        protocol: str,
        config_dict: dict,
        protocol_options_dict: dict,
        attributes_dict: dict,
    ) -> NodeRecord:

        return NodeRecord(
            name=str(name),
            protocol=Protocol(protocol),
            config=BaseNodeRecordConfig.cast_from_dict(config_dict),
            protocol_options=BaseNodeProtocolOptions(),
            attributes=NodeAttributes.cast_from_dict(attributes_dict),
        )

    def dummy_meter_protocol_options_parser(options_dict: Dict[str, Any]) -> DummyOptions:
        return DummyOptions()

    def dummy_node_protocol_options_parser(options_dict: Dict[str, Any]) -> DummyNodeProtocolOptions:
        return DummyNodeProtocolOptions()

    ProtocolRegistry.register_protocol(Protocol.MQTT, DummyMeter, DummyOptions, DummyNodeProtocolOptions, None, dummy_node_record_factory, dummy_node_factory, dummy_meter_protocol_options_parser, dummy_node_protocol_options_parser)
    plugin = ProtocolRegistry.get_protocol_plugin(Protocol.MQTT)
    assert plugin is not None
    assert plugin.meter_class is DummyMeter
    assert plugin.options_class is DummyOptions
    assert plugin.node_options_class is DummyNodeProtocolOptions
    assert plugin.meter_record_factory is None
    assert plugin.node_factory is dummy_node_factory
    assert plugin.node_record_factory is dummy_node_record_factory
    assert plugin.meter_comm_options_parser_method is dummy_meter_protocol_options_parser
    assert plugin.node_protocol_options_parser_method is dummy_node_protocol_options_parser
