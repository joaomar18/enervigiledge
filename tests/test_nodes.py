###########EXTERNAL IMPORTS############

import pytest

#######################################

#############LOCAL IMPORTS#############

from controller.node import Node
from controller.types import NodeType, NodeConfig

#######################################


def test_node_updates_direction_and_stats():
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V"))
    node.set_value(10)
    node.set_value(15)
    assert node.value == 15
    assert node.positive_direction is True
    assert node.negative_direction is False
    assert node.min_value == 10
    assert node.max_value == 15
    assert node.mean_value == pytest.approx(12.5)


def test_incremental_node_logic():
    node = Node(NodeConfig("energy", NodeType.FLOAT, "kWh", incremental_node=True))
    node.set_value(100)
    assert node.value == 0
    node.set_value(150)
    assert node.value == 50
    assert node.positive_direction is True
    node.set_value(140)
    assert node.value == 40
    assert node.negative_direction is True


def test_publish_format_with_alarms_and_rounding():
    node = Node(NodeConfig("voltage", NodeType.FLOAT, "V", min_alarm=True, max_alarm=True, min_alarm_value=5.0, max_alarm_value=15.0, decimal_places=2))
    node.set_value(3.14159)
    publish = node.get_publish_format()
    assert publish["value"] == 3.14
    assert publish["type"] == NodeType.FLOAT
    assert publish["unit"] == "V"
    assert publish["min_alarm_state"] is True
    assert publish["max_alarm_state"] is False
