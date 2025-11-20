###########EXERTNAL IMPORTS############

from typing import Dict, Any, Optional

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from model.controller.node import NodePhase, NodeType, NodeDirection, NodeLogs
from model.date import TimeSpanParameters
from db.timedb import TimeDBClient
import controller.meter.calculation as meter_calc
import util.functions.meter as meter_util

#######################################


def get_meter_energy_consumption(device: Device, phase: NodePhase, direction: NodeDirection, timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    """
    Retrieve energy consumption metrics and calculated power factor information for a meter.

    This function gathers the active and reactive energy logs for a specified device/phase/direction combination
    over a given time span. It also calculates the power factor and power factor direction, both per-point
    (when a formatted query is requested) and as a global aggregate value. If nodes for any variable are missing,
    empty log structures are used. Data from active and reactive energy logs are expected to be aligned. Raises
    a ValueError if their time-steps differ.

    Args:
        device (Device): The meter device containing the nodes for measurement.
        phase (NodePhase): The phase for which to retrieve energy data.
        direction (NodeDirection): The current direction (e.g., import/export).
        timedb (TimeDBClient): Database client for querying logs.
        time_span (TimeSpanParameters): Time parameters (start/end, step, format) for retrieval.

    Returns:
        Dict[str, Any]: A dictionary with energy and power factor logs, each structured as:
            - 'active_energy': NodeLogs dict with energy log details
            - 'reactive_energy': NodeLogs dict with reactive energy log details
            - 'power_factor': NodeLogs dict with calculated per-point/global power factor(s)
            - 'power_factor_direction': NodeLogs dict with calculated per-point/global power factor direction(s)

    Raises:
        ValueError: If the time_step of active and reactive energy logs do not match.
    """

    active_energy_node_name = meter_util.create_node_name("active_energy", phase, direction)
    reactive_energy_node_name = meter_util.create_node_name("reactive_energy", phase, direction)
    pf_node_name = meter_util.create_node_name("power_factor", phase, None)
    active_energy_node = next((n for n in device.nodes if n.config.name == active_energy_node_name), None)
    reactive_energy_node = next((n for n in device.nodes if n.config.name == reactive_energy_node_name), None)
    pf_node = next((n for n in device.nodes if n.config.name == pf_node_name), None)
    
    if active_energy_node:
        active_energy_logs = timedb.get_variable_logs(device.name, device.id, active_energy_node, time_span)
    else:
        
        active_energy_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            incremental=None,
            points=meter_util.get_empty_logs(numeric=True, incremental=True, time_span=time_span),
            time_step=time_span.time_step,
            global_metrics={"value": None}
        )

    if reactive_energy_node:
        reactive_energy_logs = timedb.get_variable_logs(device.name, device.id, reactive_energy_node, time_span) 
    else:
        reactive_energy_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            incremental=None,
            points=meter_util.get_empty_logs(numeric=True, incremental=True, time_span=time_span),
            time_step=time_span.time_step,
            global_metrics={"value": None}
        )

    if active_energy_logs.time_step != reactive_energy_logs.time_step:
        raise ValueError(f"Active Energy and Reactive Energy time steps can't be different")
    
    if pf_node:
        pf_dp = pf_node.config.decimal_places
    else:
        pf_dp = 2 # Default to two decimal places if there is no known configuration for the power factor

    pf_logs = NodeLogs(
        unit=None,
        decimal_places=pf_dp,
        type=NodeType.FLOAT,
        incremental=None,
        points=meter_util.get_empty_logs(numeric=True, incremental=False, time_span=time_span),
        time_step=time_span.time_step,
        global_metrics={"value": None}
    )

    pf_direction_logs = NodeLogs(
        unit=None,
        decimal_places=None,
        type=NodeType.STRING,
        incremental=None,
        points=meter_util.get_empty_logs(numeric=False, incremental=False, time_span=time_span),
        time_step=time_span.time_step,
        global_metrics={"value": None}
    )

    if time_span.formatted:
        for active_point, reactive_point in zip(active_energy_logs.points, reactive_energy_logs.points):
            active_value: Optional[int | float] = active_point.get("value")
            reactive_value: Optional[int | float] = reactive_point.get("value")
            (pf, pf_direction) = meter_calc.calculate_pf_and_dir_with_energy(active_value, reactive_value)
            pf_logs.points.append({"value": pf})
            pf_direction_logs.points.append({"value": pf_direction})

    global_active_value: Optional[int | float] = active_energy_logs.global_metrics.get("value") if active_energy_logs.global_metrics else None
    global_reactive_value: Optional[int | float] = reactive_energy_logs.global_metrics.get("value") if reactive_energy_logs.global_metrics else None
    (global_pf, global_pf_direction) = meter_calc.calculate_pf_and_dir_with_energy(global_active_value, global_reactive_value)
    if pf_logs.global_metrics:
        pf_logs.global_metrics["value"] = global_pf
    if pf_direction_logs.global_metrics:
        pf_direction_logs.global_metrics["value"] = global_pf_direction
        
    output: Dict[str, Any] = {}
    output["active_energy"] = active_energy_logs.get_logs()
    output["reactive_energy"] = reactive_energy_logs.get_logs()
    output["power_factor"] = pf_logs.get_logs()
    output["power_factor_direction"] = pf_direction_logs.get_logs()
    
    return output


def get_meter_peak_power(device: Device, phase: NodePhase, timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    """
    Get peak power metrics (min, max, avg) for active and apparent power.
    
    Returns global metrics for both all-time and the specified time span. If a power node
    doesn't exist on the device, returns None for that power type.
    
    Args:
        device: Meter device to query
        phase: Phase to query (PHASE_1, PHASE_2, PHASE_3, or TOTAL)
        timedb: TimeDB client for querying data
        time_span: Time span parameters (start, end, timezone)
        
    Returns:
        Dictionary with keys: active_power_all_time, active_power, 
        apparent_power_all_time, apparent_power. Values are metric dicts or None.
    """

    output: Dict[str, Any] = {}
    active_power_node_name = meter_util.create_node_name("active_energy", phase, None)
    apparent_power_node_name = meter_util.create_node_name("reactive_energy", phase, None)

    active_power_node = next((n for n in device.nodes if n.config.name == active_power_node_name), None)
    apparent_power_node = next((n for n in device.nodes if n.config.name == apparent_power_node_name), None)

    if active_power_node:
        output["active_power_all_time"] = timedb.get_variable_logs(device.name, device.id, active_power_node, TimeSpanParameters(None, None, None, False, time_span.time_zone, time_span.force_aggregation)).global_metrics
        output["active_power"] = timedb.get_variable_logs(device.name, device.id, active_power_node, time_span).global_metrics
    else:
        output["active_power_all_time"] = None
        output["active_power"] = None

    if apparent_power_node:
        output["apparent_power_all_time"] = timedb.get_variable_logs(device.name, device.id, apparent_power_node, TimeSpanParameters(None, None, None, False, time_span.time_zone, time_span.force_aggregation)).global_metrics
        output["apparent_power"] = timedb.get_variable_logs(device.name, device.id, apparent_power_node, time_span).global_metrics
    else:
        output["apparent_power_all_time"] = None
        output["apparent_power"] = None

    return output


def get_meter_phase_balance(device: Device, timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    """
    Get phase balance metrics for voltage and current across all three phases.
    
    Retrieves global metrics (min, max, avg) for each phase's voltage and current,
    then calculates voltage and current imbalance percentages.
    
    Args:
        device: Meter device to query
        timedb: TimeDB client for querying data
        time_span: Time span parameters (start, end, timezone)
        
    Returns:
        Dictionary with l1/l2/l3 voltage/current metrics, voltage_imbalance,
        and current_imbalance percentages. Missing nodes return None.
    """

    output: Dict[str, Any] = {}
    l1_voltage_node_name = meter_util.create_node_name("voltage", NodePhase.L1, None)
    l2_voltage_node_name = meter_util.create_node_name("voltage", NodePhase.L2, None)
    l3_voltage_node_name = meter_util.create_node_name("voltage", NodePhase.L3, None)

    l1_current_node_name = meter_util.create_node_name("current", NodePhase.L1, None)
    l2_current_node_name = meter_util.create_node_name("current", NodePhase.L2, None)
    l3_current_node_name = meter_util.create_node_name("current", NodePhase.L3, None)

    l1_voltage_node = next((n for n in device.nodes if n.config.name == l1_voltage_node_name), None)
    l2_voltage_node = next((n for n in device.nodes if n.config.name == l2_voltage_node_name), None)
    l3_voltage_node = next((n for n in device.nodes if n.config.name == l3_voltage_node_name), None)

    l1_current_node = next((n for n in device.nodes if n.config.name == l1_current_node_name), None)
    l2_current_node = next((n for n in device.nodes if n.config.name == l2_current_node_name), None)
    l3_current_node = next((n for n in device.nodes if n.config.name == l3_current_node_name), None)

    output["l1_voltage"] = timedb.get_variable_logs(device.name, device.id, l1_voltage_node, time_span).global_metrics if l1_voltage_node else None
    output["l2_voltage"] = timedb.get_variable_logs(device.name, device.id, l2_voltage_node, time_span).global_metrics if l2_voltage_node else None
    output["l3_voltage"] = timedb.get_variable_logs(device.name, device.id, l3_voltage_node, time_span).global_metrics if l3_voltage_node else None

    output["l1_current"] = timedb.get_variable_logs(device.name, device.id, l1_current_node, time_span).global_metrics if l1_current_node else None
    output["l2_current"] = timedb.get_variable_logs(device.name, device.id, l2_current_node, time_span).global_metrics if l2_current_node else None
    output["l3_current"] = timedb.get_variable_logs(device.name, device.id, l3_current_node, time_span).global_metrics if l3_current_node else None
    
    output["voltage_imbalance"] = meter_calc.calculate_phase_imbalance(output, "voltage")
    output["current_imbalance"] = meter_calc.calculate_phase_imbalance(output, "current")

    return output
