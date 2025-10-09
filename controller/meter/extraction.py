###########EXERTNAL IMPORTS############

from typing import Dict, Any, Optional, Set, Callable
from datetime import datetime
import math

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node.node import Node
from model.controller.general import Protocol
from model.controller.device import PowerFactorDirection
from model.controller.node import NodePhase, NodeType, NodeDirection, NodeLogs
from model.date import TimeSpanParameters
from controller.meter.nodes import EnergyMeterNodes
import controller.meter.calculation as calculation
from mqtt.client import MQTTMessage
from db.timedb import TimeDBClient
import util.functions.date as date
import util.functions.meter as meter_util
from util.debug import LoggerManager

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
        output["power_factor"] = active_energy_value / math.sqrt(math.pow(active_energy_value, 2) + math.pow(reactive_energy_value, 2))

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

##########     T O     D O     ##########
def get_meter_energy_efficiency(phase: NodePhase, meter_nodes: Dict[str, Node], timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output

##########     T O     D O     ##########
def get_meter_peak_power(phase: NodePhase, meter_nodes: Dict[str, Node], timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output

##########     T O     D O     ##########
def get_meter_phase_balance(meter_nodes: Dict[str, Node], timedb: TimeDBClient, time_span: TimeSpanParameters) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output
