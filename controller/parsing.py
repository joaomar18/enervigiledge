###########EXTERNAL IMPORTS############

from typing import Dict, List, Set, Any, Optional
import asyncio

#######################################

#############LOCAL IMPORTS#############

from model.controller.general import Protocol
from model.controller.device import EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions, BaseCommunicationOptions
from model.controller.node import BaseNodeRecordConfig, NodeRecord, NodeAttributes
from controller.meter.meter import EnergyMeter
from controller.registry.protocol import ProtocolRegistry
from controller.node.node import Node
import util.functions.objects as objects
import util.functions.meter as meter_util
import web.exceptions as api_exception

#######################################








def parse_node_record(dict_node: Dict[str, Any], meter_type: EnergyMeterType) -> NodeRecord:
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

    try:
        objects.check_required_keys(dict_node, NodeRecord)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_NODE_FIELDS, details={"fields": missing_fields})

    
    try:
        objects.check_required_keys(dict_node["config"], BaseNodeRecordConfig)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_NODE_FIELDS, details={"fields": missing_fields})
    
    
    
    node_dict_config = objects.require_field(dict_node, "config", Dict[str, Any])
    
    
    
    
    
    
    
    
    
    node_dict_protocol_options = objects.require_field(dict_node, "protocol_options", Dict[str, Any])
    node_dict_attributes: Optional[Dict[str, Any]] = dict_node.get("attributes") #Optional
    
    if node_dict_attributes is None:
        node_dict_attributes = meter_util.create_default_node_attributes(meter_type).get_attributes()
    else:
        try:
            objects.check_required_keys(node_dict_attributes, NodeAttributes)
        except KeyError as e:
            missing_fields = list(e.args[0]) if e.args else []
            raise api_exception.InvalidRequestPayload(error=api_exception.Errors.DEVICE.MISSING_NODE_ATTRIBUTES_FIELDS, details={"fields": missing_fields})
    
    objects.check_required_keys(node_dict_config, BaseNodeRecordConfig)
    name = objects.require_field(dict_node, "name", str)
    protocol = objects.convert_str_to_enum(objects.require_field(dict_node, "protocol", str), Protocol)

    """
    if protocol is Protocol.NONE:
        objects.check_required_keys(node_dict_protocol_options, ProtocolRegistry.no_protocol_options)
        node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config, protocol_options=node_dict_protocol_options, attributes=node_dict_attributes)
        node_factory = ProtocolRegistry.get_base_node_factory()
        return node_factory(node_record)

    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    objects.check_required_keys(node_dict_protocol_options, plugin.node_protocol_options)
    node_record = NodeRecord(name=name, protocol=protocol, config=node_dict_config, protocol_options=node_dict_protocol_options, attributes=node_dict_attributes)
    return plugin.node_factory(node_record)
    """


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
  
  
  
  
    device_id = dict_energy_meter.get("id")
    
    if meter_id is not None:
                   
        try: 
            device_id = int(device_id)
        except Exception:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.DEVICE.INVALID_DEVICE_ID)
            
    
    
    
    
    
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
