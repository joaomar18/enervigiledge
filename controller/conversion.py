###########EXTERNAL IMPORTS############

from typing import Dict, List, Set, Any, Type
import asyncio

#######################################

#############LOCAL IMPORTS#############

from controller.meter import EnergyMeterOptions, EnergyMeterType
from controller.types import Protocol, BaseNodeRecordConfig, NodeRecord, EnergyMeterRecord, EnergyMeterType
from controller.registry import ProtocolRegistry
from controller.node import Node
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
import util.functions.objects as objects

#######################################


def convert_dict_to_meter_options(dict_meter_options: Dict[str, any]) -> EnergyMeterOptions:
    """
    Converts a dictionary to EnergyMeterOptions with field validation.

    Args:
        dict_meter_options (Dict[str, any]): Dictionary with meter configuration fields.

    Returns:
        EnergyMeterOptions: Validated dataclass instance.

    Raises:
        ValueError: If required fields are missing or input is invalid.
        TypeError: If field types are incorrect.
    """

    result = objects.check_required_keys(dict_meter_options, EnergyMeterOptions)

    if not result:
        raise ValueError(f"Missing required fields for energy meter options")

    dataclass_fields, required_fields, optional_fields = result

    try:
        arguments = objects.create_dict_from_fields(dict_meter_options, dataclass_fields, required_fields, optional_fields)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid field type in meter options: {e}")

    return EnergyMeterOptions(**arguments)


def convert_dict_to_comm_options(dict_communication_options: Dict[str, Any], protocol: Protocol) -> Type:
    """
    Converts a dictionary to protocol-specific communication options using registry introspection.

    Args:
        dict_communication_options (Dict[str, Any]): Dictionary containing communication fields.
        protocol (Protocol): The communication protocol to use.

    Returns:
        object: Protocol-specific options dataclass instance.

    Raises:
        ValueError: If protocol is unsupported or required fields are missing.
        TypeError: If field types are invalid.
    """

    # Get protocol plugin from registry
    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    if not plugin:
        raise ValueError(f"Protocol {protocol} is not supported")

    result = objects.check_required_keys(dict_communication_options, plugin.options_class)
    if not result:
        raise ValueError(f"Missing required fields for {protocol}")

    dataclass_fields, required_fields, optional_fields = result

    try:
        arguments = objects.create_dict_from_fields(dict_communication_options, dataclass_fields, required_fields, optional_fields)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid field type in {protocol.value} options: {e}")

    return plugin.options_class(**arguments)


def convert_dict_to_node(dict_node: Dict[str, Any]) -> Node:
    """
    Converts a node configuration dictionary into the appropriate protocol-specific Node object.

    Uses the ProtocolRegistry to create the correct Node subclass (ModbusRTUNode, OPCUANode,
    or base Node) based on the protocol. Validates required fields and enum conversions.

    Args:
        dict_node (Dict[str, Any]): Configuration with 'name', 'protocol', and 'config' keys.

    Returns:
        Node: Protocol-specific Node instance with validated configuration.

    Raises:
        ValueError: Missing required fields, invalid protocol, or enum conversion errors.
        TypeError: Field type conversion failures.
    """

    result = objects.check_required_keys(dict_node, NodeRecord)
    if not result:
        raise ValueError(f"Missing required fields for node record")

    node_dict_config = dict_node.get('config')

    result = objects.check_required_keys(node_dict_config, BaseNodeRecordConfig)
    if not result:
        raise ValueError(f"Missing required fields for node record configuration")

    try:
        name = str(dict_node['name'])
        protocol_str = str(dict_node["protocol"])

        try:
            protocol = Protocol(protocol_str)
        except ValueError:
            raise ValueError(f"Invalid protocol: {protocol_str}. Must be one of: {[p.value for p in Protocol]}")

    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid field type in nodes options: {e}")

    if protocol is Protocol.NONE:

        node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config)
        return ProtocolRegistry.base_node_factory(node_record)

    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    if not plugin:
        raise ValueError(f"Protocol {protocol} is not supported for node creation")

    node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config)
    return plugin.node_factory(node_record)


def convert_dict_to_energy_nodes(list_nodes: List[Dict[str, any]]) -> Set[Node]:
    """
    Converts a list representation of multiple nodes into a set of Node objects.

    This function iterates through a list where each item represents a node configuration.
    It uses convert_dict_to_node to create individual Node instances and returns them as a set.

    Args:
        list_nodes (List[Dict[str, any]]): List containing node configurations.

    Returns:
        Set[Node]: A set of Node instances created from the list configurations.

    Raises:
        ValueError: If the input list is None or empty.
        KeyError: If any required field is missing from any node configuration.
        TypeError: If any field has an incorrect type.
    """

    if not list_nodes:
        raise ValueError("Input list cannot be None or empty")

    nodes = set()

    for node_config in list_nodes:
        try:

            # Ensure the node config is a dictionary
            if not isinstance(node_config, dict):
                raise TypeError(f"Node configuration must be a dictionary")

            # Convert the node configuration to a Node object
            node = convert_dict_to_node(node_config)
            nodes.add(node)

        except (KeyError, TypeError, ValueError) as e:
            # Re-raise the exception with context about which node failed
            raise type(e)(f"Error processing node: {str(e)}")

    return nodes


def convert_dict_to_energy_meter(
    dict_energy_meter: Dict[str, any], dict_nodes: Dict[str, any], publish_queue: asyncio.Queue, measurements_queue: asyncio.Queue
) -> ModbusRTUEnergyMeter | OPCUAEnergyMeter:
    """
    Converts dictionary configurations into a fully initialized protocol-specific EnergyMeter instance.

    Uses ProtocolRegistry to create the appropriate meter class (ModbusRTUEnergyMeter, OPCUAEnergyMeter)
    with validated configuration, converted options, and processed nodes.

    Args:
        dict_energy_meter (Dict[str, any]): Meter configuration with name, protocol, type,
            options, and communication_options.
        dict_nodes (Dict[str, any]): Node configurations for the meter.
        publish_queue (asyncio.Queue): Queue for publishing meter data and events.
        measurements_queue (asyncio.Queue): Queue for storing measurement data.

    Returns:
        ModbusRTUEnergyMeter | OPCUAEnergyMeter: Fully configured protocol-specific energy meter.

    Raises:
        ValueError: Missing required fields, invalid protocol/meter type, or unsupported protocol.
        TypeError: Field type conversion failures.
    """

    result = objects.check_required_keys(dict_energy_meter, EnergyMeterRecord, ("nodes"))
    if not result:
        raise ValueError(f"Missing required fields for energy meter configuration")

    # Extract and validate common fields
    try:
        meter_id = int(dict_energy_meter['id']) if dict_energy_meter.get('id') else None
        protocol_str = str(dict_energy_meter['protocol'])
        meter_type_str = str(dict_energy_meter['type'])
        name = str(dict_energy_meter['name'])

        try:
            protocol = Protocol(protocol_str)
        except ValueError:
            raise ValueError(f"Invalid protocol: {protocol_str}. Must be one of: {[p.value for p in Protocol]}")

        # Convert meter type string to EnergyMeterType enum
        try:
            meter_type = EnergyMeterType(meter_type_str)
        except ValueError:
            raise ValueError(f"Invalid meter type: {meter_type_str}. Must be one of: {[t.value for t in EnergyMeterType]}")

    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid field type in energy meter options: {e}")

    meter_options = convert_dict_to_meter_options(dict_energy_meter['options'])
    communication_options = convert_dict_to_comm_options(dict_energy_meter['communication_options'], protocol)
    nodes = convert_dict_to_energy_nodes(dict_nodes)

    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    if not plugin:
        raise ValueError(f"Protocol {protocol} is not supported for energy meter creation")

    return plugin.meter_class(
        id=meter_id,
        name=name,
        publish_queue=publish_queue,
        measurements_queue=measurements_queue,
        meter_type=meter_type,
        meter_options=meter_options,
        communication_options=communication_options,
        nodes=nodes,
    )
