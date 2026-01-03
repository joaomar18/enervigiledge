########### EXTERNAL IMPORTS ############

import pytest

#########################################

############# LOCAL IMPORTS #############

from model.controller.node import NodeType, NodeConfig
from controller.node.processor.float_processor import FloatNodeProcessor
from controller.node.processor.int_processor import IntNodeProcessor
from controller.node.processor.bool_processor import BoolNodeProcessor
from controller.node.processor.string_processor import StringNodeProcessor
from controller.registry.node_type import TypeRegistry

#########################################


@pytest.fixture(autouse=True)
def reset_types_registry():

    original = TypeRegistry._registry.copy()
    TypeRegistry._registry.clear()
    yield
    TypeRegistry._registry = original


def get_unregistered_behavior():
    with pytest.raises(NotImplementedError):
        TypeRegistry.get_type_plugin(NodeType.INT)


def test_register_new_type_and_retrieve():

    def dummy_float_processor_factory(config: NodeConfig) -> FloatNodeProcessor:
        return FloatNodeProcessor(NodeConfig(name="dummy", type=NodeType.FLOAT, unit=""))

    def dummy_int_processor_factory(config: NodeConfig) -> IntNodeProcessor:
        return IntNodeProcessor(NodeConfig(name="dummy", type=NodeType.INT, unit=""))

    def dummy_bool_processor_factory(config: NodeConfig) -> BoolNodeProcessor:
        return BoolNodeProcessor(NodeConfig(name="dummy", type=NodeType.BOOL, unit=""))

    def dummy_str_processor_factory(config: NodeConfig) -> StringNodeProcessor:
        return StringNodeProcessor(NodeConfig(name="dummy", type=NodeType.STRING, unit=""))

    TypeRegistry.register_type(NodeType.FLOAT, dummy_float_processor_factory)
    TypeRegistry.register_type(NodeType.INT, dummy_int_processor_factory)
    TypeRegistry.register_type(NodeType.BOOL, dummy_bool_processor_factory)
    TypeRegistry.register_type(NodeType.STRING, dummy_str_processor_factory)

    assert TypeRegistry.get_type_plugin(NodeType.FLOAT).node_processor_factory is dummy_float_processor_factory
    assert TypeRegistry.get_type_plugin(NodeType.INT).node_processor_factory is dummy_int_processor_factory
    assert TypeRegistry.get_type_plugin(NodeType.BOOL).node_processor_factory is dummy_bool_processor_factory
    assert TypeRegistry.get_type_plugin(NodeType.STRING).node_processor_factory is dummy_str_processor_factory
