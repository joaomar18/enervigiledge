###########EXTERNAL IMPORTS############

from typing import Optional, Dict, Set, List, Any
from fastapi import Request
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from model.controller.device import EnergyMeterType
from model.controller.node import NodeRecord, BaseNodeRecordConfig, BaseNodeProtocolOptions, NodeAttributes
from model.controller.general import Protocol
from model.date import TimeSpanParameters, FormattedTimeStep
import util.functions.objects as objects
import web.exceptions as api_exception
import util.functions.date as date
import util.functions.meter as meter_util

#######################################


async def parse_formatted_time_span(request: Request, formatted: bool, force_aggregation: Optional[bool] = None) -> TimeSpanParameters:
    """
    Parses and validates time span query parameters.

    Extracts start_time, end_time, time_step, time_zone, and formatting options
    from request query parameters and converts them into normalized datetime
    values with second precision removed.

    When formatted is True, start_time is required and must be a valid ISO 8601
    datetime string. end_time is optional and defaults to the current time.
    When formatted is False, all time parameters are optional.

    Args:
        request: HTTP request containing query parameters.
        formatted: Enables formatted time span behavior with required start_time.
        force_aggregation: Optional flag to force data aggregation.

    Returns:
        TimeSpanParameters: Parsed and validated time span configuration.

    Raises:
        InvalidRequestPayload:
            - If start_time is missing when formatted is enabled.
            - If start_time or end_time is not a valid ISO 8601 datetime.
            - If time_zone is invalid.
    """

    try:
        time_zone = date.get_time_zone_info(request.query_params.get("time_zone"))
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_TIME_ZONE)

    if formatted:
        time_step = request.query_params.get("time_step")
        time_step = FormattedTimeStep(time_step) if time_step is not None else None
        if not objects.validate_field_type(request.query_params, "start_time", str):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_START_TIME)
        start_time = request.query_params["start_time"]
        end_time = request.query_params.get("end_time")
        end_time = end_time if end_time is not None else datetime.isoformat(datetime.now())  # If None accounts end time is now
    else:
        start_time = request.query_params.get("start_time")  # Optional
        end_time = request.query_params.get("end_time")  # Optional
        time_step = None

    try:
        start_time = date.remove_sec_precision(date.convert_isostr_to_date(start_time)) if start_time else None
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_START_TIME)

    try:
        end_time = date.remove_sec_precision(date.convert_isostr_to_date(end_time)) if end_time else None
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_END_TIME)

    return TimeSpanParameters(start_time, end_time, time_step, formatted, time_zone, force_aggregation)


def parse_node_config(dict_node_config: Dict[str, Any]) -> BaseNodeRecordConfig:
    """
    Parse and validate a node configuration payload.

    Validates required and optional node configuration fields against the
    BaseNodeRecordConfig schema and returns a constructed configuration
    instance. Missing or invalid fields are translated into API-level
    validation errors.

    Args:
        dict_node_config: Raw node configuration data from an external source.

    Returns:
        BaseNodeRecordConfig: Parsed and validated node configuration object.

    Raises:
        InvalidRequestPayload:
            - If required configuration fields are missing.
            - If a configuration field has an invalid value or type.
    """

    try:
        dataclass_fields, optional_fields = objects.check_required_keys(dict_node_config, BaseNodeRecordConfig)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.MISSING_NODE_CONFIG_FIELDS, details={"fields": missing_fields}
        )

    try:
        arguments = objects.create_dict_from_fields(dict_node_config, dataclass_fields, optional_fields)
    except ValueError as e:
        invalid_field = e.args[0] if e.args else None
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.INVALID_NODE_CONFIG_FIELDS, details={"field": invalid_field}
        )

    return BaseNodeRecordConfig(**arguments)


def parse_node_protocol_options(dict_node_protocol_options: Dict[str, Any], protocol: Protocol) -> BaseNodeProtocolOptions:
    """
    Parse and validate protocol-specific node configuration options.

    Resolves the protocol plugin, validates the provided options against the
    protocol-specific node options schema, and returns a constructed options
    instance. Validation failures are translated into API-level errors.

    Args:
        dict_node_protocol_options: Raw protocol-specific node options data
            from an external source.
        protocol: Communication protocol used to resolve the options schema.

    Returns:
        BaseNodeProtocolOptions: Parsed and validated protocol-specific node
        options object.

    Raises:
        InvalidRequestPayload:
            - If the protocol is invalid or unsupported.
            - If required protocol options fields are missing.
            - If a protocol option field has an invalid value or type.
    """

    if protocol is Protocol.NONE:
        options_class = ProtocolRegistry.no_protocol_options
    else:
        try:
            plugin = ProtocolRegistry.get_protocol_plugin(protocol)
            options_class = plugin.node_options_class
        except NotImplementedError as e:
            raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_PROTOCOL)

    try:
        dataclass_fields, optional_fields = objects.check_required_keys(dict_node_protocol_options, options_class)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS_FIELDS, details={"fields": missing_fields}
        )

    try:
        arguments = objects.create_dict_from_fields(dict_node_protocol_options, dataclass_fields, optional_fields)
    except ValueError as e:
        invalid_field = e.args[0] if e.args else None
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.INVALID_NODE_PROTOCOL_OPTIONS_FIELDS, details={"field": invalid_field}
        )

    return options_class(**arguments)


def parse_node_attributes(dict_node_attributes: Dict[str, Any]) -> NodeAttributes:
    """
    Parse and validate node attribute metadata.

    Validates required and optional node attribute fields against the
    NodeAttributes schema and returns a constructed attributes instance.
    Validation failures are translated into API-level errors.

    Args:
        dict_node_attributes: Raw node attribute data from an external source.

    Returns:
        NodeAttributes: Parsed and validated node attributes object.

    Raises:
        InvalidRequestPayload:
            - If required node attribute fields are missing.
            - If a node attribute field has an invalid value or type.
    """

    try:
        dataclass_fields, optional_fields = objects.check_required_keys(dict_node_attributes, NodeAttributes)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.MISSING_NODE_ATTRIBUTES_FIELDS, details={"fields": missing_fields}
        )

    try:
        arguments = objects.create_dict_from_fields(dict_node_attributes, dataclass_fields, optional_fields)
    except ValueError as e:
        invalid_field = e.args[0] if e.args else None
        raise api_exception.InvalidRequestPayload(
            error=api_exception.Errors.NODES.INVALID_NODE_ATTRIBUTES_FIELDS, details={"field": invalid_field}
        )

    return NodeAttributes(**arguments)


def parse_node(dict_node: Dict[str, Any], meter_type: EnergyMeterType) -> NodeRecord:
    """
    Parse and validate a node definition.

    Validates the top-level node fields, resolves and validates protocol,
    parses node configuration, protocol-specific options, and attributes,
    and returns a constructed NodeRecord instance. Validation failures are
    translated into API-level errors.

    Args:
        dict_node: Raw node definition data from an external source.
        meter_type: Energy meter type used to determine default node
            attributes when none are provided.

    Returns:
        NodeRecord: Parsed and validated node record.

    Raises:
        InvalidRequestPayload:
            - If required node fields are missing.
            - If the protocol is missing or invalid.
            - If node configuration, protocol options, or attributes are
              missing or invalid.
    """

    # Check for Configuration fields
    try:
        objects.check_required_keys(dict_node, NodeRecord)
    except KeyError as e:
        missing_fields = list(e.args[0]) if e.args else []
        raise api_exception.InvalidRequestPayload(error=api_exception.Errors.NODES.MISSING_NODE_FIELDS, details={"fields": missing_fields})

    # Name Parsing
    if not objects.validate_field_type(dict_node, "name", str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_NAME)
    device_name: str = dict_node["name"]

    # Protocol Parsing
    if not objects.validate_field_type(dict_node, "protocol", str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_PROTOCOL)
    protocol: str = dict_node["protocol"]
    try:
        protocol = Protocol(protocol)
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_PROTOCOL)

    # Node Configuration
    if not objects.validate_field_type(dict_node, "config", Dict[str, Any]):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_CONFIG)
    node_config_dict: Dict[str, Any] = dict_node["config"]
    node_config = parse_node_config(node_config_dict)

    # Node Protocol Options
    if not objects.validate_field_type(dict_node, "protocol_options", Dict[str, Any]):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS)
    node_protocol_options_dict: Dict[str, Any] = dict_node["protocol_options"]
    node_protocol_options = parse_node_protocol_options(node_protocol_options_dict, protocol)

    # Node Attributes
    node_dict_attributes: Optional[Dict[str, Any]] = dict_node.get("attributes")  # Optional
    if node_dict_attributes is None:
        node_attributes = meter_util.create_default_node_attributes(meter_type)
    else:
        node_attributes = parse_node_attributes(node_dict_attributes)

    return NodeRecord(
        name=device_name, protocol=protocol, config=node_config, protocol_options=node_protocol_options, attributes=node_attributes
    )


def parse_nodes(nodes_list_dict: List[Dict[str, Any]], meter_type: EnergyMeterType) -> Set[NodeRecord]:
    """
    Parse and validate a list of node definitions.

    Iterates over a list of raw node definitions, validates each entry,
    and returns a set of constructed NodeRecord instances. Validation
    failures are translated into API-level errors.

    Args:
        nodes_list_dict: List of raw node definition dictionaries.
        meter_type: Energy meter type used during node parsing.

    Returns:
        Set[NodeRecord]: Set of parsed and validated node records.

    Raises:
        InvalidRequestPayload:
            - If a node entry is not a dictionary.
            - If a node definition is invalid or fails validation.
    """

    node_records: Set[NodeRecord] = set()

    for record in nodes_list_dict:

        # Ensure the node record is a dictionary
        if not isinstance(record, dict):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_NODE)

        # Convert the node configuration to a Node object
        node_record = parse_node(record, meter_type)
        node_records.add(node_record)

    return node_records
