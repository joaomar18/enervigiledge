###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Type

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node import NodeType, Node, ModbusRTUNode, OPCUANode
from controller.types import Protocol
from db.db import NodeRecord
from protocol.modbus_rtu.rtu_device import ModbusRTUOptions, ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAOptions, OPCUAEnergyMeter

#######################################

NodeFactory = Callable[[NodeRecord], Node]  # A callable that creates a :class:`Node` from a database record


@dataclass
class ProtocolPlugin:
    """Container for protocol specific classes and factories.

    Attributes
    ----------
    meter_class:
        Concrete :class:`~controller.device.Device` subclass implementing the
        protocol.
    options_class:
        Dataclass representing protocol specific connection options.
    node_factory:
        Callable used to create protocol specific :class:`~controller.node.Node`
        instances from :class:`db.db.NodeRecord` objects.
    """

    meter_class: Type[Device]
    options_class: Type
    node_factory: NodeFactory


class ProtocolRegistry:
    """
    Static registry for managing protocol-specific device implementations and factories.

    This class provides a centralized static registry for different communication protocols
    (Modbus RTU, OPC-UA, etc.) and their associated device classes, option classes,
    and node factory functions.

    The registry enables dynamic protocol selection and device instantiation based
    on the protocol type, supporting extensible plugin architecture for new protocols.

    All methods are static, making this a singleton-like registry accessible from anywhere
    without needing to instantiate the class.

    Attributes:
        _registry: Static dictionary mapping Protocol enums to ProtocolPlugin instances
    """

    _registry: Dict[Protocol, ProtocolPlugin] = {}

    def __init__(self):
        raise TypeError("ProtocolRegistry is a static class and cannot be instantiated")

    @staticmethod
    def register_protocol(protocol: Protocol, meter_class: Type[Device], options_class: Type, node_factory: NodeFactory) -> None:
        """
        Register protocol specific handlers in the static registry.

        Parameters
        ----------
        protocol:
            The :class:`~controller.types.Protocol` implemented by the plugin.
        meter_class:
            Device class implementing the protocol.
        options_class:
            Dataclass for connection options.
        node_factory:
            Callable returning protocol specific nodes.
        """
        ProtocolRegistry._registry[protocol] = ProtocolPlugin(meter_class=meter_class, options_class=options_class, node_factory=node_factory)

    @staticmethod
    def get_protocol_plugin(protocol: Protocol) -> Optional[ProtocolPlugin]:
        """
        Retrieve registered plugin for the specified protocol.

        Parameters
        ----------
        protocol:
            The protocol type to look up in the registry.

        Returns
        -------
        Optional[ProtocolPlugin]:
            The registered plugin for the protocol, or None if not found.
        """
        return ProtocolRegistry._registry.get(protocol)


##########     M O D B U S     R T U     R E G I S T R A T I O N     ##########


def _modbus_rtu_node_factory(record: NodeRecord) -> Node:
    """Create a :class:`ModbusRTUNode` from a :class:`NodeRecord`."""
    cfg = record.config
    return ModbusRTUNode(
        name=record.name,
        type=NodeType(cfg["type"]),
        register=cfg["register"],
        **{k: v for k, v in cfg.items() if k not in ["type", "register"]}
    )


ProtocolRegistry.register_protocol(Protocol.MODBUS_RTU, ModbusRTUEnergyMeter, ModbusRTUOptions, _modbus_rtu_node_factory)

##########     O P C     U A     R E G I S T R A T I O N     ##########


def _opcua_node_factory(record: NodeRecord) -> Node:
    """Create an :class:`OPCUANode` from a :class:`NodeRecord`."""
    cfg = record.config
    return OPCUANode(
        name=record.name,
        type=NodeType(cfg["type"]),
        node_id=cfg["node_id"],
        **{k: v for k, v in cfg.items() if k not in ["type", "node_id"]}
    )


ProtocolRegistry.register_protocol(Protocol.OPC_UA, OPCUAEnergyMeter, OPCUAOptions, _opcua_node_factory)
