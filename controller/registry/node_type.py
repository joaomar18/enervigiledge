###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Callable, Dict, Any

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.processor import NodeProcessor
from controller.node.processor.bool_processor import BoolNodeProcessor
from controller.node.processor.float_processor import FloatNodeProcessor
from controller.node.processor.int_processor import IntNodeProcessor
from controller.node.processor.string_processor import StringNodeProcessor
from controller.types.node import NodeType, NodeConfig
from controller.exceptions import NotImplemeted

#######################################

NodeProcessorFactory = Callable[[NodeConfig], NodeProcessor[Any]]


@dataclass
class TypePlugin:
    node_processor_factory: NodeProcessorFactory


class TypeRegistry:

    _registry: Dict[NodeType, TypePlugin] = {}

    def __init__(self):
        raise TypeError("TypeRegistry is a static class and cannot be instantiated")

    @staticmethod
    def register_type(node_type: NodeType, node_processor_factory: NodeProcessorFactory) -> None:

        TypeRegistry._registry[node_type] = TypePlugin(node_processor_factory=node_processor_factory)

    @staticmethod
    def get_type_plugin(node_type: NodeType) -> TypePlugin:
        plugin = TypeRegistry._registry.get(node_type)
        if plugin is None:
            raise NotImplemeted(f"Type {node_type} doesn't have a plugin implemented.")

        return plugin


###############     T Y P E S     R E G I S T R A T I O N     ###############


# Bool Type Registration
def _bool_type_registration(config: NodeConfig) -> NodeProcessor[bool]:
    return BoolNodeProcessor(configuration=config)


TypeRegistry.register_type(NodeType.BOOL, _bool_type_registration)


# String Type Registration
def _str_type_registration(config: NodeConfig) -> NodeProcessor[str]:
    return StringNodeProcessor(configuration=config)


TypeRegistry.register_type(NodeType.STRING, _str_type_registration)


# Int Type Registration
def _int_type_registration(config: NodeConfig) -> NodeProcessor[int]:
    return IntNodeProcessor(configuration=config)


TypeRegistry.register_type(NodeType.INT, _int_type_registration)


# Float Type Registration
def _float_type_registration(config: NodeConfig) -> NodeProcessor[float]:
    return FloatNodeProcessor(configuration=config)


TypeRegistry.register_type(NodeType.FLOAT, _float_type_registration)

#############################################################################
