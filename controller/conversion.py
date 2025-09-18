###########EXTERNAL IMPORTS############

from typing import Dict, List, Set, Any, Optional
import asyncio

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol
from model.controller.device import EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions, BaseCommunicationOptions
from model.controller.node import BaseNodeRecordConfig, NodeRecord
from controller.meter.meter import EnergyMeter
from controller.registry.protocol import ProtocolRegistry
from controller.node.node import Node
import util.functions.objects as objects
import util.functions.meter as meter_util

#######################################


def convert_dict_to_meter_options(dict_meter_options: Dict[str, Any]) -> EnergyMeterOptions:
    """
    Converts a dictionary of meter options into a structured format.

    Args:
        dict_meter_options (Dict[str, Any]): The input dictionary containing meter options.

    Returns:
        Dict[str, Any]: A structured dictionary with processed meter options.

    Raises:
        ValueError: If required fields are missing or invalid.
    """

    dataclass_fields, optional_fields = objects.check_required_keys(dict_meter_options, EnergyMeterOptions)
    try:
        arguments = objects.create_dict_from_fields(dict_meter_options, dataclass_fields, optional_fields)
    except Exception as e:
        raise TypeError(f"Invalid field type in meter options: {e}")

    return EnergyMeterOptions(**arguments)


def convert_dict_to_comm_options(dict_communication_options: Dict[str, Any], protocol: Protocol) -> BaseCommunicationOptions:
    """
    Converts a dictionary of communication options based on the specified protocol.

    Args:
        dict_communication_options (Dict[str, Any]): The input dictionary containing communication options.
        protocol (Any): The protocol to use for processing the options.

    Returns:
        Dict[str, Any]: A structured dictionary with processed communication options.

    Raises:
        ValueError: If required fields are missing or invalid.
    """

    # Get protocol plugin from registry
    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    if not plugin:
        raise ValueError(f"Protocol {protocol} is not supported")

    dataclass_fields, optional_fields = objects.check_required_keys(dict_communication_options, plugin.options_class)
    try:
        arguments = objects.create_dict_from_fields(dict_communication_options, dataclass_fields, optional_fields)
    except Exception as e:
        raise TypeError(f"Invalid field type in {protocol.value} options: {e}")

    return plugin.options_class(**arguments)


def convert_dict_to_node(dict_node: Dict[str, Any], meter_type: EnergyMeterType) -> Node:
    """
    Converts a dictionary to a node object.

    Args:
        dict_node (Dict[str, Any]): The input dictionary representing a node.
        meter_type (EnergyMeterType): The type of the energy meter.

    Returns:
        Node: A node object created from the dictionary.

    Raises:
        ValueError: If the dictionary contains invalid or missing fields.
    """

    objects.check_required_keys(dict_node, NodeRecord)
    node_dict_config: Dict[str, Any] = objects.require_field(dict_node, "config", Dict[str, Any])
    node_dict_attributes: Optional[Dict[str, Any]] = dict_node.get("attributes")
    objects.check_required_keys(node_dict_config, BaseNodeRecordConfig)
    if node_dict_attributes is None:
        node_dict_attributes = meter_util.create_default_node_attributes(meter_type).get_attributes()

    name = objects.require_field(dict_node, "name", str)
    protocol = objects.convert_str_to_enum(objects.require_field(dict_node, "protocol", str), Protocol)

    if protocol is Protocol.NONE:
        node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config, attributes=node_dict_attributes)
        node_factory = ProtocolRegistry.get_base_node_factory()
        return node_factory(node_record)

    plugin = ProtocolRegistry.get_protocol_plugin(protocol)

    node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config, attributes=node_dict_attributes)
    return plugin.node_factory(node_record)


def convert_dict_to_energy_nodes(list_nodes: List[Dict[str, Any]], meter_type: EnergyMeterType) -> Set[Node]:
    """
    Converts a list of node dictionaries to a set of node objects.

    Args:
        list_nodes (List[Dict[str, Any]]): The input list of dictionaries representing nodes.
        meter_type (EnergyMeterType): The type of the energy meter.

    Returns:
        Set[Node]: A set of node objects created from the dictionaries.

    Raises:
        ValueError: If the input list is None or empty.
        TypeError: If a node configuration is not a dictionary.
    """

    if not list_nodes:
        raise ValueError("Input list cannot be None or empty")

    nodes: Set[Node] = set()

    for node_config in list_nodes:
        # Ensure the node config is a dictionary
        if not isinstance(node_config, dict):
            raise TypeError(f"Node configuration must be a dictionary")

        # Convert the node configuration to a Node object
        node = convert_dict_to_node(node_config, meter_type)
        nodes.add(node)

    return nodes


def convert_dict_to_energy_meter(
    dict_energy_meter: Dict[str, Any], dict_nodes: List[Dict[str, Any]], publish_queue: asyncio.Queue, measurements_queue: asyncio.Queue
) -> EnergyMeter:
    """
    Converts a dictionary to an energy meter object.

    Args:
        dict_energy_meter (Dict[str, Any]): The input dictionary representing an energy meter.
        dict_nodes (List[Dict[str, Any]]): The list of node dictionaries.
        publish_queue (asyncio.Queue): The queue for publishing messages.
        measurements_queue (asyncio.Queue): The queue for measurements.

    Returns:
        Any: An energy meter object created from the dictionary.

    Raises:
        ValueError: If the dictionary contains invalid or missing fields.
    """

    result = objects.check_required_keys(dict_energy_meter, EnergyMeterRecord, ("nodes",))

    meter_id = objects.require_field(dict_energy_meter, "id", int)
    name = objects.require_field(dict_energy_meter, "name", str)
    protocol = objects.convert_str_to_enum(objects.require_field(dict_energy_meter, "protocol", str), Protocol)
    meter_type = objects.convert_str_to_enum(objects.require_field(dict_energy_meter, "type", str), EnergyMeterType)

    meter_options_dict = objects.require_field(dict_energy_meter, "options", Dict[str, Any])
    meter_options = convert_dict_to_meter_options(meter_options_dict)

    communication_options_dict = objects.require_field(dict_energy_meter, "communication_options", Dict[str, Any])
    communication_options = convert_dict_to_comm_options(communication_options_dict, protocol)

    nodes = convert_dict_to_energy_nodes(dict_nodes, meter_type)

    plugin = ProtocolRegistry.get_protocol_plugin(protocol)

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
