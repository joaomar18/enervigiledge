###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Type

#######################################

#############LOCAL IMPORTS#############

from controller.meter.meter import EnergyMeter
from controller.node.node import Node, ModbusRTUNode, OPCUANode
from model.controller.general import Protocol
from model.controller.device import BaseCommunicationOptions
from model.controller.node import NodeRecord, NodeConfig, BaseNodeProtocolOptions
from model.controller.protocol.no_protocol import NoProtocolNodeOptions, NONE_TO_INTERNAL_TYPE_MAP
from model.controller.protocol.modbus_rtu import ModbusRTUOptions, ModbusRTUNodeOptions, MODBUS_RTU_TO_INTERNAL_TYPE_MAP
from model.controller.protocol.opcua import OPCUAOptions, OPCUANodeOptions, OPCUA_TO_INTERNAL_TYPE_MAP
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from controller.exceptions import NotImplemeted

#######################################

NodeFactory = Callable[[NodeRecord], Node]


@dataclass
class ProtocolPlugin:
    """Plugin containing protocol-specific classes and factories."""

    meter_class: Type[EnergyMeter]
    options_class: Type[BaseCommunicationOptions]
    node_options_class: Type[BaseNodeProtocolOptions]
    node_factory: NodeFactory


class ProtocolRegistry:
    """
    Static registry managing protocol-specific implementations.
    """

    _registry: Dict[Protocol, ProtocolPlugin] = {}
    base_node_factory: Optional[NodeFactory] = None
    no_protocol_options: Type[BaseNodeProtocolOptions]  = NoProtocolNodeOptions

    def __init__(self):
        raise TypeError("ProtocolRegistry is a static class and cannot be instantiated")

    @staticmethod
    def get_base_node_factory() -> NodeFactory:
        """
        Retrieves the base node factory for protocol-agnostic nodes.

        Returns:
            NodeFactory: Factory function for creating base nodes.

        Raises:
            NotImplemeted: If base node factory is not implemented.
        """
        if _base_node_factory is None:
            raise NotImplemeted(f"Base node factory is not implemented.")

        return _base_node_factory

    @staticmethod
    def register_protocol(protocol: Protocol, meter_class: Type[EnergyMeter], options_class: Type[BaseCommunicationOptions], node_options_class: Type[BaseNodeProtocolOptions], node_factory: NodeFactory) -> None:
        """
        Registers a protocol with its device class, option classes, and node factory.

        Args:
            protocol: Protocol identifier.
            meter_class: Energy meter class for the protocol.
            options_class: Device-level communication options class.
            node_options_class: Node-level protocol options class.
            node_factory: Factory for creating protocol-specific nodes.
        """

        ProtocolRegistry._registry[protocol] = ProtocolPlugin(meter_class=meter_class, options_class=options_class, node_options_class=node_options_class, node_factory=node_factory)

    @staticmethod
    def get_protocol_plugin(protocol: Protocol) -> ProtocolPlugin:
        """
        Retrieves the plugin for a specific protocol.

        Args:
            protocol: The protocol type to retrieve plugin for.

        Returns:
            ProtocolPlugin: Plugin containing protocol-specific implementations.

        Raises:
            NotImplemeted: If protocol plugin is not implemented.
        """

        plugin = ProtocolRegistry._registry.get(protocol)
        if plugin is None:
            raise NotImplemeted(f"Protocol {protocol} doesn't have a plugin implemented.")

        return plugin


###########     P R O T O C O L S     R E G I S T R A T I O N     ###########


# Base Node Factory
def _base_node_factory(record: NodeRecord) -> Node:
    """Creates a basic Node instance from a NodeRecord."""
    
    protocol_options = NoProtocolNodeOptions(**record.protocol_options.get_options())
    internal_type = NONE_TO_INTERNAL_TYPE_MAP[protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return Node(configuration=config, protocol_options=protocol_options)


ProtocolRegistry.base_node_factory = _base_node_factory


# Modbus RTU Node Factory
def _modbus_rtu_node_factory(record: NodeRecord) -> Node:
    """Creates a ModbusRTUNode instance with register configuration."""
    
    protocol_options = ModbusRTUNodeOptions(**record.protocol_options.get_options())
    internal_type = MODBUS_RTU_TO_INTERNAL_TYPE_MAP[protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return ModbusRTUNode(configuration=config, protocol_options=protocol_options)


ProtocolRegistry.register_protocol(Protocol.MODBUS_RTU, ModbusRTUEnergyMeter, ModbusRTUOptions, ModbusRTUNodeOptions, _modbus_rtu_node_factory)


# OPC UA Node Factory
def _opcua_node_factory(record: NodeRecord) -> Node:
    """Creates an OPCUANode instance with node_id configuration."""
    
    protocol_options = OPCUANodeOptions(**record.protocol_options.get_options())
    internal_type = OPCUA_TO_INTERNAL_TYPE_MAP[protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return OPCUANode(configuration=config, protocol_options=protocol_options)


ProtocolRegistry.register_protocol(Protocol.OPC_UA, OPCUAEnergyMeter, OPCUAOptions, OPCUANodeOptions, _opcua_node_factory)

#############################################################################
