###########EXTERNAL IMPORTS############

from dataclasses import dataclass
from typing import Callable, Dict, Set, Optional, Any, Type

#######################################

#############LOCAL IMPORTS#############

from controller.meter.meter import EnergyMeter
from controller.node.node import Node, ModbusRTUNode, OPCUANode
from model.controller.general import Protocol
from model.controller.device import BaseCommunicationOptions, EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions
from model.controller.node import NodeRecord, NodeConfig, BaseNodeRecordConfig, BaseNodeProtocolOptions, NodeAttributes
from model.controller.protocol.no_protocol import NoProtocolNodeOptions, NONE_TO_INTERNAL_TYPE_MAP
from model.controller.protocol.modbus_rtu import ModbusRTUOptions, ModbusRTUNodeOptions, MODBUS_RTU_TO_INTERNAL_TYPE_MAP
from model.controller.protocol.opcua import OPCUAOptions, OPCUANodeOptions, OPCUA_TO_INTERNAL_TYPE_MAP
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter

#######################################

NodeFactory = Callable[[NodeRecord], Node]
NodeRecordFactory = Callable[[str, str, Dict[str, Any], Dict[str, Any], Dict[str, Any]], NodeRecord]
MeterRecordFactory = Callable[[int, str, str, str, Dict[str, Any], Dict[str, Any], Set[NodeRecord]], EnergyMeterRecord]


@dataclass
class ProtocolPlugin:
    """Plugin containing protocol-specific classes and factories."""

    meter_class: Optional[Type[EnergyMeter]]
    options_class: Optional[Type[BaseCommunicationOptions]]
    node_options_class: Type[BaseNodeProtocolOptions]
    meter_record_factory: Optional[MeterRecordFactory]
    node_record_factory: NodeRecordFactory
    node_factory: NodeFactory


class ProtocolRegistry:
    """
    Static registry managing protocol-specific implementations.
    """

    _registry: Dict[Protocol, ProtocolPlugin] = {}

    def __init__(self):
        raise TypeError("ProtocolRegistry is a static class and cannot be instantiated")

    @staticmethod
    def register_protocol(
        protocol: Protocol,
        meter_class: Optional[Type[EnergyMeter]],
        options_class: Optional[Type[BaseCommunicationOptions]],
        node_options_class: Type[BaseNodeProtocolOptions],
        meter_record_factory: Optional[MeterRecordFactory],
        node_record_factory: NodeRecordFactory,
        node_factory: NodeFactory,
    ) -> None:
        """
        Register a protocol and its associated domain factories in the registry.

        Binds a protocol identifier to the concrete energy meter class, communication
        options, node protocol options, and factory functions required to construct
        meter and node domain objects for that protocol.
        """

        ProtocolRegistry._registry[protocol] = ProtocolPlugin(
            meter_class=meter_class,
            options_class=options_class,
            node_options_class=node_options_class,
            meter_record_factory=meter_record_factory,
            node_record_factory=node_record_factory,
            node_factory=node_factory,
        )

    @staticmethod
    def get_protocol_plugin(protocol: Protocol | str) -> ProtocolPlugin:
        """
        Retrieve the registered protocol plugin for a given protocol.

        Accepts either a Protocol enum or its string representation and returns
        the corresponding ProtocolPlugin instance from the registry.

        Raises:
            ValueError: If the protocol string cannot be converted to a Protocol enum.
            NotImplementedError: If no plugin is registered for the specified protocol.
        """

        if isinstance(protocol, str):
            try:
                protocol = Protocol(protocol)
            except Exception as e:
                raise ValueError(f"Invalid protocol {protocol} trying to be converted.")

        plugin = ProtocolRegistry._registry.get(protocol)
        if plugin is None:
            raise NotImplementedError(f"Protocol {protocol} doesn't have a plugin implemented.")

        return plugin


###########     P R O T O C O L S     R E G I S T R A T I O N     ###########


# NONE Protocol
def _no_protocol_node_record_factory(
    name: str,
    protocol: str,
    config_dict: Dict[str, Any],
    protocol_options_dict: Dict[str, Any],
    attributes_dict: Dict[str, Any],
) -> NodeRecord:
    """Create a NodeRecord instance for nodes without a communication protocol."""

    return NodeRecord(
        name=str(name),
        protocol=Protocol(protocol),
        config=BaseNodeRecordConfig.cast_from_dict(config_dict),
        protocol_options=NoProtocolNodeOptions.cast_from_dict(protocol_options_dict),
        attributes=NodeAttributes.cast_from_dict(attributes_dict),
    )


def _no_protocol_node_factory(record: NodeRecord) -> Node:
    """Creates a basic Node instance from a NodeRecord."""

    if not isinstance(record.protocol_options, NoProtocolNodeOptions):
        raise TypeError(f"Expected NoProtocolNodeOptions for protocol NONE, got {type(record.protocol_options).__name__}")

    internal_type = NONE_TO_INTERNAL_TYPE_MAP[record.protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return Node(configuration=config, protocol_options=record.protocol_options)


ProtocolRegistry.register_protocol(
    Protocol.NONE, None, None, NoProtocolNodeOptions, None, _no_protocol_node_record_factory, _no_protocol_node_factory
)


# Modbus RTU Protocol
def _modbus_rtu_meter_record_factory(
    id: int,
    name: str,
    protocol: str,
    type: str,
    options_dict: Dict[str, Any],
    communication_options_dict: Dict[str, Any],
    nodes: Set[NodeRecord],
) -> EnergyMeterRecord:
    """Create an EnergyMeterRecord instance for a Modbus RTU device."""

    return EnergyMeterRecord(
        name=str(name),
        protocol=Protocol(protocol),
        type=EnergyMeterType(type),
        options=EnergyMeterOptions.cast_from_dict(options_dict),
        communication_options=ModbusRTUOptions.cast_from_dict(communication_options_dict),
        nodes=nodes,
        id=int(id),
    )


def _modbus_rtu_node_record_factory(
    name: str,
    protocol: str,
    config_dict: Dict[str, Any],
    protocol_options_dict: Dict[str, Any],
    attributes_dict: Dict[str, Any],
) -> NodeRecord:
    """Create a NodeRecord instance for a Modbus RTU node."""

    return NodeRecord(
        name=str(name),
        protocol=Protocol(protocol),
        config=BaseNodeRecordConfig.cast_from_dict(config_dict),
        protocol_options=ModbusRTUNodeOptions.cast_from_dict(protocol_options_dict),
        attributes=NodeAttributes.cast_from_dict(attributes_dict),
    )


def _modbus_rtu_node_factory(record: NodeRecord) -> Node:
    """Creates a ModbusRTUNode instance with register configuration."""

    if not isinstance(record.protocol_options, ModbusRTUNodeOptions):
        raise TypeError(f"Expected ModbusRTUNodeOptions for protocol Modbus RTU, got {type(record.protocol_options).__name__}")

    internal_type = MODBUS_RTU_TO_INTERNAL_TYPE_MAP[record.protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return ModbusRTUNode(configuration=config, protocol_options=record.protocol_options)


ProtocolRegistry.register_protocol(
    Protocol.MODBUS_RTU,
    ModbusRTUEnergyMeter,
    ModbusRTUOptions,
    ModbusRTUNodeOptions,
    _modbus_rtu_meter_record_factory,
    _modbus_rtu_node_record_factory,
    _modbus_rtu_node_factory,
)


# OPC UA Protocol
def _opc_ua_meter_record_factory(
    id: int,
    name: str,
    protocol: str,
    type: str,
    options_dict: Dict[str, Any],
    communication_options_dict: Dict[str, Any],
    nodes: Set[NodeRecord],
) -> EnergyMeterRecord:
    """Create an EnergyMeterRecord instance for a OPC UA device."""

    return EnergyMeterRecord(
        name=str(name),
        protocol=Protocol(protocol),
        type=EnergyMeterType(type),
        options=EnergyMeterOptions.cast_from_dict(options_dict),
        communication_options=OPCUAOptions.cast_from_dict(communication_options_dict),
        nodes=nodes,
        id=int(id),
    )


def _opc_ua_node_record_factory(
    name: str,
    protocol: str,
    config_dict: Dict[str, Any],
    protocol_options_dict: Dict[str, Any],
    attributes_dict: Dict[str, Any],
) -> NodeRecord:
    """Create a NodeRecord instance for a OPC UA node."""

    return NodeRecord(
        name=str(name),
        protocol=Protocol(protocol),
        config=BaseNodeRecordConfig.cast_from_dict(config_dict),
        protocol_options=OPCUANodeOptions.cast_from_dict(protocol_options_dict),
        attributes=NodeAttributes.cast_from_dict(attributes_dict),
    )


def _opcua_node_factory(record: NodeRecord) -> Node:
    """Creates an OPCUANode instance with node_id configuration."""

    if not isinstance(record.protocol_options, OPCUANodeOptions):
        raise TypeError(f"Expected OPCUANodeOptions for protocol OPC UA, got {type(record.protocol_options).__name__}")
    internal_type = OPCUA_TO_INTERNAL_TYPE_MAP[record.protocol_options.type]
    config = NodeConfig.create_config_from_record(record, internal_type)
    return OPCUANode(configuration=config, protocol_options=record.protocol_options)


ProtocolRegistry.register_protocol(
    Protocol.OPC_UA,
    OPCUAEnergyMeter,
    OPCUAOptions,
    OPCUANodeOptions,
    _opc_ua_meter_record_factory,
    _opc_ua_node_record_factory,
    _opcua_node_factory,
)

#############################################################################
