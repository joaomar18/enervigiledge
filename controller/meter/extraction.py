###########EXERTNAL IMPORTS############

from typing import List, Dict, Any, Optional
import math

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from model.controller.device import PowerFactorDirection
from model.controller.node import NodePhase, NodeType, NodeDirection, NodeLogs
from model.date import TimeSpanParameters
from db.timedb import TimeDBClient
import util.functions.meter as meter_util

#######################################


def get_meter_energy_consumption(device: Device, phase: NodePhase, direction: NodeDirection, timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    """
    Fetches active and reactive energy logs for a device, computes their average power
    factor, and determines its direction.

    The function retrieves both active and reactive energy logs for the given phase and
    direction from the time-series database. If either node is missing, it returns an
    empty log for that variable. It then ensures both logs share the same time step and,
    if valid metrics exist, calculates the mean power factor:

        PF = E_active / sqrt(E_active² + E_reactive²)

    The sign of the reactive energy determines whether the power factor is lagging,
    leading, unitary, or unknown.

    Args:
        device: The target device containing energy measurement nodes.
        phase: The phase to retrieve data for (e.g., L1, L2, L3, or TOTAL).
        direction: The measurement direction (import or export).
        timedb: Time-series database client used to retrieve variable logs.
        time_span: Time range and resolution of the requested data.

    Returns:
        dict: {
            "active_energy": NodeLogs,
            "reactive_energy": NodeLogs,
            "power_factor": float | None,
            "power_factor_direction": PowerFactorDirection | None
        }

    Raises:
        ValueError: If active and reactive energy logs have different time steps.
    """
        
    output: Dict[str, Any] = {}
    active_energy_node_name = meter_util.create_node_name("active_energy", phase, direction)
    reactive_energy_node_name = meter_util.create_node_name("reactive_energy", phase, direction)

    active_energy_node = next((n for n in device.nodes if n.config.name == active_energy_node_name), None)
    reactive_energy_node = next((n for n in device.nodes if n.config.name == reactive_energy_node_name), None)

    if active_energy_node:
        output["active_energy"] = timedb.get_variable_logs(device.name, device.id, active_energy_node, time_span)
    else:
        output["active_energy"] = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            incremental=None,
            points=[],
            time_step=time_span.time_step,
            global_metrics=None
        ).get_logs()

    if reactive_energy_node:
        output["reactive_energy"] = timedb.get_variable_logs(device.name, device.id, reactive_energy_node, time_span) 
    else:
        output["reactive_energy"] = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            incremental=None,
            points=[],
            time_step=time_span.time_step,
            global_metrics=None
        ).get_logs()

    active_energy_time_step = output["active_energy"].get("time_step")
    reactive_energy_time_step = output["reactive_energy"].get("time_step")

    if active_energy_time_step != reactive_energy_time_step:
        raise ValueError(f"Active Energy and Reactive Energy time steps can't be different")

    active_energy_metrics: Optional[Dict[str, Any]] = output["active_energy"].get("global_metrics") 
    reactive_energy_metrics: Optional[Dict[str, Any]] = output["reactive_energy"].get("global_metrics")
    active_energy_value: Optional[int | float] = active_energy_metrics.get("value") if active_energy_metrics is not None else None
    reactive_energy_value: Optional[int | float] = reactive_energy_metrics.get("value") if reactive_energy_metrics is not None else None

    if active_energy_value is not None and reactive_energy_value is not None:
        if active_energy_value != 0 or reactive_energy_value != 0:
            output["power_factor"] = active_energy_value / math.sqrt(math.pow(active_energy_value, 2) + math.pow(reactive_energy_value, 2))
        else:
            output["power_factor"] = None

        if active_energy_value == 0 and reactive_energy_value == 0:
            output["power_factor_direction"] = PowerFactorDirection.UNKNOWN
        elif active_energy_value != 0 and reactive_energy_value == 0:
            output["power_factor_direction"] = PowerFactorDirection.UNITARY
        elif reactive_energy_value > 0:
            output["power_factor_direction"] = PowerFactorDirection.LAGGING
        elif reactive_energy_value < 0: 
            output["power_factor_direction"] = PowerFactorDirection.LEADING
        else:
            output["power_factor_direction"] = PowerFactorDirection.UNKNOWN

    else:
        output["power_factor"] = None
        output["power_factor_direction"] = None

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
        output["active_power_all_time"] = timedb.get_variable_logs(device.name, device.id, active_power_node, TimeSpanParameters(None, None, None, False, time_span.time_zone, time_span.force_aggregation)).get("global_metrics")
        output["active_power"] = timedb.get_variable_logs(device.name, device.id, active_power_node, time_span).get("global_metrics")
    else:
        output["active_power_all_time"] = None
        output["active_power"] = None

    if apparent_power_node:
        output["apparent_power_all_time"] = timedb.get_variable_logs(device.name, device.id, apparent_power_node, TimeSpanParameters(None, None, None, False, time_span.time_zone, time_span.force_aggregation)).get("global_metrics")
        output["apparent_power"] = timedb.get_variable_logs(device.name, device.id, apparent_power_node, time_span).get("global_metrics")
    else:
        output["apparent_power_all_time"] = None
        output["apparent_power"] = None

    return output


def _calculate_phase_imbalance(phase_balance_dict: Dict[str, Optional[Dict[str, Any]]], node_base_name: str) -> Optional[float]:
    """
    Calculate phase imbalance percentage using maximum deviation from average.
    
    Requires at least 2 phases with data. Returns None if insufficient data.
    
    Args:
        phase_balance_dict: Node names mapped to their metrics (with average_value)
        node_base_name: Node type to filter (e.g., "voltage", "current")
        
    Returns:
        Imbalance percentage or None
    """

    average_sum = 0.0
    average_count = 0
    average_values: List[int | float] = []

    for node_name, metrics in phase_balance_dict.items():
        if metrics is not None and node_base_name in node_name:
            average_value = metrics.get("average_value")
            if average_value is not None:
                average_values.append(float(average_value))
                average_sum += average_value
                average_count += 1

    global_average = average_sum / average_count if average_count > 1 else None
    

    max_deviation = max(abs(value - average_sum) for value in average_values) if global_average is not None else None 
    
    if max_deviation is None or global_average is None:
        return None

    return (max_deviation / global_average) * 100


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

    output["l1_voltage"] = timedb.get_variable_logs(device.name, device.id, l1_voltage_node, time_span).get("global_metrics") if l1_voltage_node else None
    output["l2_voltage"] = timedb.get_variable_logs(device.name, device.id, l2_voltage_node, time_span).get("global_metrics") if l2_voltage_node else None
    output["l3_voltage"] = timedb.get_variable_logs(device.name, device.id, l3_voltage_node, time_span).get("global_metrics") if l3_voltage_node else None

    output["l1_current"] = timedb.get_variable_logs(device.name, device.id, l1_current_node, time_span).get("global_metrics") if l1_current_node else None
    output["l2_current"] = timedb.get_variable_logs(device.name, device.id, l2_current_node, time_span).get("global_metrics") if l2_current_node else None
    output["l3_current"] = timedb.get_variable_logs(device.name, device.id, l3_current_node, time_span).get("global_metrics") if l3_current_node else None
    
    output["voltage_imbalance"] = _calculate_phase_imbalance(output, "voltage")
    output["current_imbalance"] = _calculate_phase_imbalance(output, "current")

    return output
