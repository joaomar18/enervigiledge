###########EXTERNAL IMPORTS############

import pytest

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from model.controller.node import NodeType, NodeConfig, CounterMode, BaseNodeProtocolOptions
from controller.node.processor.float_processor import FloatNodeProcessor

#######################################


def test_node_updates_direction_and_stats():
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V"), BaseNodeProtocolOptions())
    node.processor.set_value(10)
    node.processor.set_value(15)
    assert isinstance(node.processor, FloatNodeProcessor)
    assert node.processor.value == 15
    assert node.processor.positive_direction is True
    assert node.processor.negative_direction is False
    assert node.processor.min_value == 10
    assert node.processor.max_value == 15


def test_incremental_node_logic():
    node = Node(NodeConfig("energy", NodeType.FLOAT, "kWh", is_counter=True, counter_mode=CounterMode.CUMULATIVE), BaseNodeProtocolOptions())
    assert isinstance(node.processor, FloatNodeProcessor)
    node.processor.set_value(100)
    assert node.processor.value == 0
    node.processor.set_value(150)
    assert node.processor.value == 50
    assert node.processor.positive_direction is True
    node.processor.set_value(140)
    assert node.processor.value == 40
    assert node.processor.negative_direction is True


def test_publish_format_with_alarms_and_rounding():
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V", min_alarm=True, max_alarm=True, min_alarm_value=5.0, max_alarm_value=15.0, decimal_places=2), BaseNodeProtocolOptions())
    node.processor.set_value(3.14159)
    publish = node.get_publish_format()
    assert publish["value"] == 3.14
    assert publish["type"] == NodeType.FLOAT
    assert publish["unit"] == "V"
    assert publish["min_alarm_state"] is True
    assert publish["max_alarm_state"] is False
