###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Type

#######################################

#############LOCAL IMPORTS#############

from controller.meter.meter import EnergyMeter
from controller.node.node import Node, ModbusRTUNode, OPCUANode
from model.controller.general import Protocol
from model.controller.device import BaseCommunicationOptions
from model.controller.node import NodeRecord, NodeConfig
from protocol.modbus_rtu.rtu_device import ModbusRTUOptions, ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAOptions, OPCUAEnergyMeter
from controller.exceptions import NotImplemeted

#######################################

NodeFactory = Callable[[NodeRecord], Node]


@dataclass
class ProtocolPlugin:
    """Plugin containing protocol-specific classes and factories."""

    meter_class: Type[EnergyMeter]
    options_class: Type[BaseCommunicationOptions]
    node_factory: NodeFactory


class ProtocolRegistry:
    """
    Static registry managing protocol-specific implementations.
    """

    _registry: Dict[Protocol, ProtocolPlugin] = {}
    base_node_factory: Optional[NodeFactory] = None

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
    def register_protocol(protocol: Protocol, meter_class: Type[EnergyMeter], options_class: Type, node_factory: NodeFactory) -> None:
        """
        Registers a protocol plugin with associated classes and factories.

        Args:
            protocol: The protocol type to register.
            meter_class: Energy meter class for this protocol.
            options_class: Configuration options class for this protocol.
            node_factory: Factory function for creating protocol-specific nodes.
        """

        ProtocolRegistry._registry[protocol] = ProtocolPlugin(meter_class=meter_class, options_class=options_class, node_factory=node_factory)

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
    config = NodeConfig.create_from_node_record(record)
    return Node(configuration=config)


ProtocolRegistry.base_node_factory = _base_node_factory


# Modbus RTU Node Factory
def _modbus_rtu_node_factory(record: NodeRecord) -> Node:
    """Creates a ModbusRTUNode instance with register configuration."""
    config = NodeConfig.create_from_node_record(record)
    return ModbusRTUNode(configuration=config, register=record.config["register"])


ProtocolRegistry.register_protocol(Protocol.MODBUS_RTU, ModbusRTUEnergyMeter, ModbusRTUOptions, _modbus_rtu_node_factory)


# OPC UA Node Factory
def _opcua_node_factory(record: NodeRecord) -> Node:
    """Creates an OPCUANode instance with node_id configuration."""
    config = NodeConfig.create_from_node_record(record)
    return OPCUANode(configuration=config, node_id=record.config["node_id"])


ProtocolRegistry.register_protocol(Protocol.OPC_UA, OPCUAEnergyMeter, OPCUAOptions, _opcua_node_factory)

#############################################################################
