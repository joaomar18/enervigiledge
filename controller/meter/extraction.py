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
            is_counter=True,
            points=meter_util.get_empty_log_points(numeric=True, incremental=True, time_span=time_span),
            time_step=time_span.time_step,
            global_metrics=meter_util.get_empty_log_global_metrics(numeric=True, incremental=True)
        )

    if reactive_energy_node:
        reactive_energy_logs = timedb.get_variable_logs(device.name, device.id, reactive_energy_node, time_span) 
    else:
        reactive_energy_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            is_counter=True,
            points=meter_util.get_empty_log_points(numeric=True, incremental=True, time_span=time_span),
            time_step=time_span.time_step,
            global_metrics=meter_util.get_empty_log_global_metrics(numeric=True, incremental=True)
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
        is_counter=False,
        points=meter_util.get_empty_log_points(numeric=True, incremental=False, time_span=time_span),
        time_step=time_span.time_step,
        global_metrics=meter_util.get_empty_log_global_metrics(numeric=False, incremental=False)
    )

    pf_direction_logs = NodeLogs(
        unit=None,
        decimal_places=None,
        type=NodeType.STRING,
        is_counter=None,
        points=meter_util.get_empty_log_points(numeric=False, incremental=False, time_span=time_span),
        time_step=time_span.time_step,
        global_metrics=meter_util.get_empty_log_global_metrics(numeric=False, incremental=False)
    )

    if time_span.formatted:
        
        pf_logs.points.clear()
        pf_direction_logs.points.clear()
        
        for active_point, reactive_point in zip(active_energy_logs.points, reactive_energy_logs.points):
            active_value: Optional[int | float] = active_point.get("value")
            reactive_value: Optional[int | float] = reactive_point.get("value")
            (pf, pf_direction) = meter_calc.calculate_pf_and_dir_with_energy(active_value, reactive_value)
            pf_logs.points.append({"start_time": active_point.get("start_time"), "end_time": active_point.get("end_time"), "value": pf})
            pf_direction_logs.points.append({"start_time": active_point.get("start_time"), "end_time": active_point.get("end_time"), "value": pf_direction})

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
    Retrieve global peak power metrics (active, reactive, and apparent) for a specific device phase.

    This function attempts to fetch the corresponding nodes for each power type
    (active, reactive, apparent) in the given device and phase. For each node:
        - If the node exists, its logs are queried from the time-series database for the
          specified time span, and global metrics (e.g., peak, average) are included.
        - If the node does not exist, empty logs and placeholder global metrics are returned.

    The returned dictionary maps each power type to its logs and global metrics, ensuring
    a consistent structure regardless of node availability.

    Args:
        device (Device): The device containing the meter nodes to query.
        phase (NodePhase): The specific phase of the device for which to retrieve power metrics.
        timedb (TimeDBClient): Time-series database client used to fetch variable logs.
        time_span (TimeSpanParameters): Time interval parameters over which metrics are computed.

    Returns:
        Dict[str, Any]: A dictionary with keys:
            - 'active_power': Logs and metrics for active power.
            - 'reactive_power': Logs and metrics for reactive power.
            - 'apparent_power': Logs and metrics for apparent power.

        Each value is a dictionary returned by `NodeLogs.get_logs()`, containing:
            - Unit, decimal places, and type information
            - Global metrics (peak, average, etc.)
        If the corresponding node is missing, values are empty placeholders. The points are removed to reduce unecessary network lag.
    """

    active_power_node_name = meter_util.create_node_name("active_power", phase, None)
    reactive_power_node_name = meter_util.create_node_name("reactive_power", phase, None)
    apparent_power_node_name = meter_util.create_node_name("apparent_power", phase, None)

    active_power_node = next((n for n in device.nodes if n.config.name == active_power_node_name), None)
    reactive_power_node = next((n for n in device.nodes if n.config.name == reactive_power_node_name), None)
    apparent_power_node = next((n for n in device.nodes if n.config.name == apparent_power_node_name), None)

    if active_power_node:
        active_power_logs = timedb.get_variable_logs(device.name, device.id, active_power_node, time_span, True)
    else:
        active_power_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            is_counter=False,
            points=[],
            time_step=time_span.time_step,
            global_metrics=meter_util.get_empty_log_global_metrics(numeric=True, incremental=False)
        )

    if reactive_power_node:
        reactive_power_logs = timedb.get_variable_logs(device.name, device.id, reactive_power_node, time_span, True)
    else:
        reactive_power_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            is_counter=False,
            points=[],
            time_step=time_span.time_step,
            global_metrics=meter_util.get_empty_log_global_metrics(numeric=True, incremental=False)
        )

    if apparent_power_node:
        apparent_power_logs = timedb.get_variable_logs(device.name, device.id, apparent_power_node, time_span, True)
    else:
        apparent_power_logs = NodeLogs(
            unit=None,
            decimal_places=None,
            type=NodeType.FLOAT,
            is_counter=False,
            points=[],
            time_step=time_span.time_step,
            global_metrics=meter_util.get_empty_log_global_metrics(numeric=True, incremental=False)
        )

    output: Dict[str, Any] = {}
    output["active_power"] = active_power_logs.get_logs()
    output["reactive_power"] = reactive_power_logs.get_logs()
    output["apparent_power"] = apparent_power_logs.get_logs()

    return output
