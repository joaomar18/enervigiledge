###########EXTERNAL IMPORTS############

from typing import Optional, Dict, Set, List, Any
from types import NoneType
from fastapi import Request
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.registry.protocol import ProtocolRegistry
from model.controller.device import EnergyMeterType
from model.controller.node import NodePhase, CounterMode, NodeRecord, BaseNodeRecordConfig, NodeAttributes
from model.controller.general import Protocol
from model.date import TimeSpanParameters, FormattedTimeStep
import web.exceptions as api_exception
import util.functions.date as date
import util.functions.meter as meter_util
import web.parsers.helpers as parse_helper

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
        start_time = request.query_params.get("start_time")
        if start_time is None or not isinstance(start_time, str):
            raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_START_TIME)
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
    Parse and validate base node configuration from an API payload.

    Extracts and type-checks all protocol-independent node configuration
    fields, applies enum normalization where required, and returns a
    fully constructed ``BaseNodeRecordConfig`` instance.

    Args:
        dict_node_config: Raw node configuration dictionary from the API request.

    Returns:
        BaseNodeRecordConfig: Parsed and validated node configuration.

    Raises:
        InvalidRequestPayload: If required fields are missing or invalid.
        ValueError: If parsed values violate expected internal types
            (indicating an unexpected internal state).
    """

    missing: List[str] = []

    # Parse Enabled
    enabled = parse_helper.parse_bool_field_from_dict(dict_node_config, "enabled", missing)

    # Parse Unit
    unit = parse_helper.parse_str_field_from_dict(dict_node_config, "unit", missing, True)

    # Parse Publish
    publish = parse_helper.parse_bool_field_from_dict(dict_node_config, "publish", missing)

    # Parse Calculated
    calculated = parse_helper.parse_bool_field_from_dict(dict_node_config, "calculated", missing)

    # Parse Custom
    custom = parse_helper.parse_bool_field_from_dict(dict_node_config, "custom", missing)

    # Parse Decimal Places
    decimal_places = parse_helper.parse_int_field_from_dict(dict_node_config, "decimal_places", missing, True)

    # Parse Logging
    logging = parse_helper.parse_bool_field_from_dict(dict_node_config, "logging", missing)

    # Parse Logging Period
    logging_period = parse_helper.parse_int_field_from_dict(dict_node_config, "logging_period", missing)

    # Parse Min Alarm
    min_alarm = parse_helper.parse_bool_field_from_dict(dict_node_config, "min_alarm", missing)

    # Parse Max Alarm
    max_alarm = parse_helper.parse_bool_field_from_dict(dict_node_config, "max_alarm", missing)

    # Parse Min Alarm Value
    min_alarm_value = parse_helper.parse_float_field_from_dict(dict_node_config, "min_alarm_value", missing, True)

    # Parse Max Alarm Value
    max_alarm_value = parse_helper.parse_float_field_from_dict(dict_node_config, "max_alarm_value", missing, True)

    # Parse Min Warning
    min_warning = parse_helper.parse_bool_field_from_dict(dict_node_config, "min_warning", missing)

    # Parse Max Warning
    max_warning = parse_helper.parse_bool_field_from_dict(dict_node_config, "max_warning", missing)

    # Parse Min Warning Value
    min_warning_value = parse_helper.parse_float_field_from_dict(dict_node_config, "min_warning_value", missing, True)

    # Parse Max Warning Value
    max_warning_value = parse_helper.parse_float_field_from_dict(dict_node_config, "max_warning_value", missing, True)

    # Parse Is Counter
    is_counter = parse_helper.parse_bool_field_from_dict(dict_node_config, "is_counter", missing)

    # Parse Counter Mode
    counter_mode = parse_helper.parse_str_field_from_dict(dict_node_config, "counter_mode", missing, True)
    if counter_mode is not None:
        try:
            counter_mode = CounterMode(counter_mode)
        except Exception as e:
            counter_mode = None
            missing.append("counter_mode")

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.NODES.MISSING_NODE_CONFIG_FIELDS, None, details={"missing_fields": missing}
        )

    if (
        not isinstance(enabled, bool)
        or not isinstance(unit, (str, NoneType))
        or not isinstance(publish, bool)
        or not isinstance(calculated, bool)
        or not isinstance(custom, bool)
        or not isinstance(decimal_places, (int, NoneType))
        or not isinstance(logging, bool)
        or not isinstance(logging_period, int)
        or not isinstance(min_alarm, bool)
        or not isinstance(max_alarm, bool)
        or not isinstance(min_alarm_value, (float, NoneType))
        or not isinstance(max_alarm_value, (float, NoneType))
        or not isinstance(min_warning, bool)
        or not isinstance(max_warning, bool)
        or not isinstance(min_warning_value, (float, NoneType))
        or not isinstance(max_warning_value, (float, NoneType))
        or not isinstance(is_counter, bool)
        or not isinstance(counter_mode, (CounterMode, NoneType))
    ):
        raise ValueError(f"Invalid types in Node Base Configuration.")

    return BaseNodeRecordConfig(
        enabled=enabled,
        unit=unit,
        publish=publish,
        calculated=calculated,
        custom=custom,
        decimal_places=decimal_places,
        logging=logging,
        logging_period=logging_period,
        min_alarm=min_alarm,
        max_alarm=max_alarm,
        min_alarm_value=min_alarm_value,
        max_alarm_value=max_alarm_value,
        min_warning=min_warning,
        max_warning=max_warning,
        min_warning_value=min_warning_value,
        max_warning_value=max_warning_value,
        is_counter=is_counter,
        counter_mode=counter_mode,
    )


def parse_node_attributes(dict_node_attributes: Dict[str, Any]) -> NodeAttributes:
    """
    Parse and validate node attributes from an API payload.

    Converts raw attribute values into their corresponding domain types and
    returns a validated NodeAttributes instance.

    Args:
        dict_node_attributes: Raw node attributes dictionary from the request.

    Returns:
        NodeAttributes: Parsed node attributes.

    Raises:
        InvalidRequestPayload: If required attributes are missing or invalid.
        ValueError: If parsed values violate expected internal types.
    """

    missing: List[str] = []

    # Parse Type
    phase = parse_helper.parse_str_field_from_dict(dict_node_attributes, "phase", missing)
    if phase is not None:
        try:
            phase = NodePhase(phase)
        except Exception as e:
            phase = None
            missing.append("phase")

    if len(missing) > 0:
        raise api_exception.InvalidRequestPayload(
            api_exception.Errors.NODES.MISSING_NODE_ATTRIBUTES_FIELDS, None, details={"missing_fields": missing}
        )

    if not isinstance(phase, NodePhase):
        raise ValueError(f"Invalid types in Node Attributes.")

    return NodeAttributes(phase=phase)


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

    # Name Parsing
    device_name = dict_node.get("name")
    if device_name is None or not isinstance(device_name, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_NAME)

    # Protocol Parsing
    protocol = dict_node.get("protocol")
    if protocol is None or not isinstance(protocol, str):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_PROTOCOL)
    try:
        protocol = Protocol(protocol)
        parser_comm_method = ProtocolRegistry.get_protocol_plugin(protocol).node_protocol_options_parser_method
    except Exception as e:
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.INVALID_PROTOCOL)

    # Node Configuration
    node_config_dict_req = dict_node.get("config")
    if node_config_dict_req is None or not isinstance(node_config_dict_req, dict):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_CONFIG)
    node_config_dict: Dict[str, Any] = node_config_dict_req
    node_config = parse_node_config(node_config_dict)

    # Node Protocol Options
    node_protocol_options_dict_req = dict_node.get("protocol_options")
    if node_protocol_options_dict_req is None or not isinstance(node_protocol_options_dict_req, dict):
        raise api_exception.InvalidRequestPayload(api_exception.Errors.NODES.MISSING_NODE_PROTOCOL_OPTIONS)
    node_protocol_options_dict: Dict[str, Any] = node_protocol_options_dict_req
    node_protocol_options = parser_comm_method(node_protocol_options_dict)

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
