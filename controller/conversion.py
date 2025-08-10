###########EXTERNAL IMPORTS############

from typing import Dict, List, Set, Any, Type
import asyncio

#######################################

#############LOCAL IMPORTS#############

from controller.meter import EnergyMeterOptions, EnergyMeterType
from controller.types import Protocol
from controller.registry import ProtocolRegistry
from controller.node import NodeType, Node
from protocol.modbus_rtu.rtu_device import ModbusRTUEnergyMeter
from protocol.opcua.opcua_device import OPCUAEnergyMeter
from db.db import NodeRecord
import util.functions_objects as objects

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

    Extracts node configuration from a structured dictionary and creates the correct Node
    subclass (ModbusRTUNode, OPCUANode, or base Node) based on the specified protocol.
    Validates all configuration fields and converts string enums to proper types.

    Dictionary Structure:
    - Top level: Contains 'name', 'protocol', and 'config' keys
    - Config section: Contains all node-specific configuration parameters

    Args:
        dict_node (Dict[str, any]): Node configuration dictionary.
            Top-level fields:
                - name (str): Unique identifier for the node
                - protocol (str): Communication protocol ('MODBUS_RTU', 'OPC_UA', 'NONE')
            Config section fields:
                - type (str): Data type ('INT', 'FLOAT', 'BOOL', 'STRING')
                - unit (str): Unit of measurement
                - enabled (bool): Whether the node is active
                - publish (bool): Whether to publish via MQTT
                - calculated (bool): Whether the value is calculated
                - custom (bool): Whether it's a custom node
                - logging (bool): Whether to log historical values
                - logging_period (int): Logging interval in minutes
                - min_alarm/max_alarm (bool): Alarm configuration
                - min_alarm_value/max_alarm_value (float): Alarm thresholds
                - incremental_node (bool): For energy accumulation
                - Protocol-specific: 'register' (Modbus) or 'node_id' (OPC UA)

    Returns:
        Node: Protocol-specific Node instance (ModbusRTUNode, OPCUANode, or base Node)
        with validated configuration and proper type conversions.

    Raises:
        KeyError: If required fields are missing from the configuration.
        TypeError: If field types cannot be converted to expected types.
        ValueError: If input is invalid, protocol unsupported, or enum conversion fails.
    """

    if not dict_node:
        raise ValueError("Input dictionary cannot be None or empty")

    node_dict_config = dict_node.get('config')

    if not node_dict_config:
        raise ValueError("Node configuration dictionary cannot be None or empty")

    # Extract common required fields
    required_common_fields = ['name', 'protocol']

    required_config_fields = [
        'type',
        'unit',
        'enabled',
        'publish',
        'calculated',
        'custom',
        'logging',
        'logging_period',
        'min_alarm',
        'max_alarm',
        'min_alarm_value',
        'max_alarm_value',
        'incremental_node',
        'positive_incremental',
        'calculate_increment',
        'decimal_places',
    ]

    missing_common_fields = [field for field in required_common_fields if field not in dict_node]
    if missing_common_fields:
        raise KeyError(f"Missing required common fields: {', '.join(missing_common_fields)}")

    missing_config_fields = [field for field in required_config_fields if field not in node_dict_config]
    if missing_config_fields:
        raise KeyError(f"Missing required config fields: {', '.join(missing_config_fields)}")

    # Extract and validate common fields
    try:
        name = str(dict_node['name'])
        type_str = str(node_dict_config['type'])
        unit = str(node_dict_config['unit'])
        protocol_str = str(dict_node['protocol'])

        # Convert type string to NodeType enum
        try:
            node_type = NodeType(type_str)
        except ValueError:
            raise ValueError(f"Invalid node type: {type_str}. Must be one of: {[t.value for t in NodeType]}")

        # Convert protocol string to Protocol enum
        try:
            protocol = Protocol(protocol_str)
        except ValueError:
            raise ValueError(f"Invalid protocol: {protocol_str}. Must be one of: {[p.value for p in Protocol]}")

    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid field type in node options: {e}")

    # Extract required fields with validation
    enabled = bool(node_dict_config['enabled'])
    publish = bool(node_dict_config['publish'])
    calculated = bool(node_dict_config['calculated'])
    custom = bool(node_dict_config['custom'])
    logging = bool(node_dict_config['logging'])
    logging_period = int(node_dict_config['logging_period'])
    min_alarm = bool(node_dict_config['min_alarm'])
    max_alarm = bool(node_dict_config['max_alarm'])
    min_alarm_value = float(node_dict_config['min_alarm_value']) if node_dict_config['min_alarm_value'] is not None else None
    max_alarm_value = float(node_dict_config['max_alarm_value']) if node_dict_config['max_alarm_value'] is not None else None
    incremental_node = bool(node_dict_config['incremental_node']) if node_dict_config['incremental_node'] is not None else None
    positive_incremental = bool(node_dict_config['positive_incremental']) if node_dict_config['positive_incremental'] is not None else None
    calculate_increment = bool(node_dict_config['calculate_increment']) if node_dict_config['calculate_increment'] is not None else None
    decimal_places = int(node_dict_config['decimal_places']) if node_dict_config['decimal_places'] is not None else None

    # Create node based on protocol using registry
    if protocol is Protocol.NONE:
        # For calculated nodes or virtual nodes, create a base Node instance
        return Node(
            name=name,
            type=node_type,
            unit=unit,
            protocol=Protocol.NONE,
            enabled=enabled,
            incremental_node=incremental_node,
            positive_incremental=positive_incremental,
            calculate_increment=calculate_increment,
            publish=publish,
            calculated=calculated,
            custom=custom,
            logging=logging,
            logging_period=logging_period,
            min_alarm=min_alarm,
            max_alarm=max_alarm,
            min_alarm_value=min_alarm_value,
            max_alarm_value=max_alarm_value,
            decimal_places=decimal_places,
        )
    else:
        # Use registry to get the node factory
        plugin = ProtocolRegistry.get_protocol_plugin(protocol)
        if not plugin:
            raise ValueError(f"Protocol {protocol} is not supported for node creation")

        # Create a NodeRecord to pass to the factory
        node_record = NodeRecord(device_id=None, name=name, protocol=protocol.value, config=node_dict_config)  # Will be set later when adding to device

        # Use the factory from the registry
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

    Creates a complete energy meter object by parsing configuration dictionaries, validating
    data types, converting to appropriate enums, and instantiating the correct protocol-specific
    class using the ProtocolRegistry with all dependencies properly configured.

    Process Flow:
    1. Validates required fields and extracts basic configuration
    2. Converts string values to proper enum types (Protocol, EnergyMeterType)
    3. Uses helper functions to convert meter options, communication options, and nodes
    4. Uses ProtocolRegistry to get the appropriate meter class and instantiate it

    Args:
        dict_energy_meter (Dict[str, any]): Energy meter configuration dictionary.
            Required fields:
                - name (str): Display name of the meter
                - protocol (str): Communication protocol ('MODBUS_RTU' or 'OPC_UA')
                - type (str): Meter type ('SINGLE_PHASE' or 'THREE_PHASE')
                - options (Dict): General meter configuration options
                - communication_options (Dict): Protocol-specific communication parameters
            Optional fields:
                - id (int): Unique identifier (None for new meters)
        dict_nodes (List[Dict[str, any]]): List of node configuration dictionaries.
        publish_queue (asyncio.Queue): Queue for publishing meter data and events.
        measurements_queue (asyncio.Queue): Queue for storing measurement data.

    Returns:
        ModbusRTUEnergyMeter | OPCUAEnergyMeter: Fully configured protocol-specific energy meter
        instance with validated configuration, provided queues, and converted node set.

    Raises:
        KeyError: If required configuration fields are missing.
        TypeError: If field types cannot be converted to expected types.
        ValueError: If input is invalid, protocol unsupported, or enum conversion fails.
    """

    if not dict_energy_meter:
        raise ValueError("Input dictionary cannot be None or empty")

    # Extract common required fields
    required_fields = ['name', 'protocol', 'type', 'options', 'communication_options']
    missing_fields = [field for field in required_fields if field not in dict_energy_meter]
    if missing_fields:
        raise KeyError(f"Missing required fields: {', '.join(missing_fields)}")

    # Extract and validate common fields
    try:
        meter_id = int(dict_energy_meter['id']) if dict_energy_meter.get('id') else None
        name = str(dict_energy_meter['name'])
        protocol_str = str(dict_energy_meter['protocol'])
        meter_type_str = str(dict_energy_meter['type'])

        # Convert protocol string to Protocol enum
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

    # Convert meter options
    meter_options = convert_dict_to_meter_options(dict_energy_meter['options'])

    # Convert communication options
    communication_options = convert_dict_to_comm_options(dict_energy_meter['communication_options'], protocol)

    # Convert nodes
    nodes = convert_dict_to_energy_nodes(dict_nodes)

    # Create energy meter based on protocol using registry
    plugin = ProtocolRegistry.get_protocol_plugin(protocol)
    if not plugin:
        raise ValueError(f"Protocol {protocol} is not supported for energy meter creation")

    # Use the meter class from the registry
    meter_class = plugin.meter_class

    return meter_class(
        id=meter_id,
        name=name,
        publish_queue=publish_queue,
        measurements_queue=measurements_queue,
        meter_type=meter_type,
        meter_options=meter_options,
        connection_options=communication_options,
        nodes=nodes,
    )
