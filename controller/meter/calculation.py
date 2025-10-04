###########EXTERNAL IMPORTS############

from typing import Dict, Optional
import math

#######################################

#############LOCAL IMPORTS#############

from model.controller.device import EnergyMeterOptions, PowerFactorDirection
from controller.node.node import Node
from controller.node.processor.numeric_processor import NumericNodeProcessor
import util.functions.meter as meter_util
import util.functions.calculation as calculation

#######################################


def calculate_energy(prefix: str, energy_type: str, node: Node, meter_nodes: Dict[str, Node], meter_options: EnergyMeterOptions) -> None:
    """
    Calculates energy values for a node based on meter configuration and energy type.

    Supports multiple calculation methods:
    - Total energy calculation by summing individual phase values
    - Forward/reverse energy difference calculation when configured
    - Power-based energy integration when meter doesn't provide direct energy readings

    Args:
        prefix (str): Phase prefix ("l1_", "l2_", "l3_", "total_") or line-to-line prefix
        energy_type (str): Type of energy to calculate ("active" or "reactive")
        node (Node): Target node to store the calculated energy value
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes
        meter_options (EnergyMeterOptions): Configuration options for energy calculation
    """

    if prefix == "total_":
        total = 0.0
        for p in ("l1_", "l2_", "l3_"):
            (phase_node, phase_value) = meter_util.get_numeric_node_with_value(f"{p}{energy_type}_energy", meter_nodes)

            if phase_value is None:
                node.processor.set_value(None)
                return

            total += calculation.get_scaled_value(phase_value, phase_node.config.unit)

        scaled_total = calculation.apply_output_scaling(total, node.config.unit)
        node.processor.set_value(scaled_total)
        return

    if meter_options.read_separate_forward_reverse_energy:

        (forward_node, forward_value) = meter_util.get_numeric_node_with_value(f"{prefix}forward_{energy_type}_energy", meter_nodes)
        (reverse_node, reverse_value) = meter_util.get_numeric_node_with_value(f"{prefix}reverse_{energy_type}_energy", meter_nodes)

        if forward_value is None or reverse_value is None:
            return

        scaled_forward = calculation.get_scaled_value(forward_value, forward_node.config.unit)
        scaled_reverse = calculation.get_scaled_value(reverse_value, reverse_node.config.unit)
        scaled_value = calculation.apply_output_scaling(scaled_forward - scaled_reverse, node.config.unit)
        node.processor.set_value(scaled_value)

    elif not meter_options.read_energy_from_meter:

        (power_node, power_value) = meter_util.get_numeric_node_with_value(f"{prefix}{energy_type}_power", meter_nodes)

        if power_value is None:
            return

        elapsed_hours = power_node.processor.elapsed_time / 3600.0 if power_node.processor.elapsed_time else 0.0
        scaled_power = calculation.get_scaled_value(power_value, power_node.config.unit)
        scaled_value = calculation.apply_output_scaling(scaled_power * elapsed_hours, node.config.unit)
        node.processor.set_value(scaled_value)


def calculate_power(prefix: str, power_type: str, node: Node, meter_nodes: Dict[str, Node]) -> None:
    """
    Calculates power values for a node based on phase configuration and power type.

    Supports calculation for different power types:
    - Apparent power: calculated from active/reactive power or voltage/current
    - Reactive power: calculated from apparent/active power or voltage/current/power factor
    - Active power: calculated from apparent/reactive power or voltage/current/power factor

    For total power, sums individual phase values. For individual phases, uses available
    measurements (voltage, current, power factor) to calculate missing power values.

    Args:
        prefix (str): Phase prefix ("l1_", "l2_", "l3_", "total_") or line-to-line prefix
        power_type (str): Type of power to calculate ("active", "reactive", or "apparent")
        node (Node): Target node to store the calculated power value
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes
    """

    if prefix == "total_":
        total = 0.0
        for p in ("l1_", "l2_", "l3_"):
            (phase_node, phase_value) = meter_util.get_numeric_node_with_value(f"{p}{power_type}_power", meter_nodes)

            if phase_value is None:
                node.processor.set_value(None)
                return

            total += calculation.get_scaled_value(phase_value, phase_node.config.unit)

        scaled_total = calculation.apply_output_scaling(total, node.config.unit)
        node.processor.set_value(scaled_total)
        return

    if power_type == "apparent":
        _calculate_apparent_power(prefix=prefix, node=node, meter_nodes=meter_nodes)

    elif power_type == "active":
        _calculate_active_power(prefix=prefix, node=node, meter_nodes=meter_nodes)

    elif power_type == "reactive":
        _calculate_reactive_power(prefix=prefix, node=node, meter_nodes=meter_nodes)


def _calculate_apparent_power(prefix: str, node: Node, meter_nodes: Dict[str, Node]) -> None:
    """
    Calculates apparent power using available measurements.

    Uses active/reactive power (S = √(P² + Q²)) or voltage/current (S = V × I).
    Priority is given to power-based calculation when both methods are available.

    Args:
        prefix (str): Phase prefix for node identification.
        node (Node): Target node to store the calculated apparent power value.
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes.
    """

    v_node = meter_util.find_node(f"{prefix}voltage", meter_nodes)
    i_node = meter_util.find_node(f"{prefix}current", meter_nodes)
    p_node = meter_util.find_node(f"{prefix}active_power", meter_nodes)
    q_node = meter_util.find_node(f"{prefix}reactive_power", meter_nodes)

    value: Optional[int | float] = None

    if p_node is not None and q_node is not None:

        p_value = meter_util.get_numeric_value(p_node)
        q_value = meter_util.get_numeric_value(p_node)

        if p_value is not None and q_value is not None:

            scaled_p = calculation.get_scaled_value(p_value, p_node.config.unit)
            scaled_q = calculation.get_scaled_value(q_value, q_node.config.unit)
            value = math.sqrt(scaled_p**2 + scaled_q**2)

    elif value is None and v_node is not None and i_node is not None:

        v_value = meter_util.get_numeric_value(v_node)
        i_value = meter_util.get_numeric_value(i_node)

        if v_value is not None and i_value is not None:

            scaled_v = calculation.get_scaled_value(v_value, v_node.config.unit)
            scaled_i = calculation.get_scaled_value(i_value, i_node.config.unit)
            value = scaled_v * scaled_i

    scaled_value = calculation.apply_output_scaling(value, node.config.unit) if value is not None else None
    node.processor.set_value(scaled_value)


def _calculate_active_power(prefix: str, node: Node, meter_nodes: Dict[str, Node]) -> None:
    """
    Calculates active power using available measurements.

    Uses apparent/reactive power (P = √(S² - Q²)) or voltage/current/power factor (P = V × I × PF).
    Priority is given to power-based calculation when both methods are available.

    Args:
        prefix (str): Phase prefix for node identification.
        node (Node): Target node to store the calculated active power value.
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes.
    """

    v_node = meter_util.find_node(f"{prefix}voltage", meter_nodes)
    i_node = meter_util.find_node(f"{prefix}current", meter_nodes)
    pf_node = meter_util.find_node(f"{prefix}power_factor", meter_nodes)
    s_node = meter_util.find_node(f"{prefix}apparent_power", meter_nodes)
    q_node = meter_util.find_node(f"{prefix}reactive_power", meter_nodes)

    value: Optional[int | float] = None

    if s_node is not None and q_node is not None:

        s_value = meter_util.get_numeric_value(s_node)
        q_value = meter_util.get_numeric_value(q_node)

        if s_value is not None and q_value is not None:

            scaled_s = calculation.get_scaled_value(s_value, s_node.config.unit)
            scaled_q = calculation.get_scaled_value(q_value, q_node.config.unit)
            value = math.sqrt(scaled_s**2 - scaled_q**2)

    elif value is None and v_node is not None and i_node is not None and pf_node is not None:

        v_value = meter_util.get_numeric_value(v_node)
        i_value = meter_util.get_numeric_value(i_node)
        pf_value = meter_util.get_numeric_value(pf_node)

        if v_value is not None and i_value is not None and pf_value is not None:

            scaled_v = calculation.get_scaled_value(v_value, v_node.config.unit)
            scaled_i = calculation.get_scaled_value(i_value, i_node.config.unit)
            scaled_pf = calculation.get_scaled_value(pf_value, pf_node.config.unit)
            value = scaled_v * scaled_i * scaled_pf

    scaled_value = calculation.apply_output_scaling(value, node.config.unit) if value is not None else None
    node.processor.set_value(scaled_value)


def _calculate_reactive_power(prefix: str, node: Node, meter_nodes: Dict[str, Node]) -> None:
    """
    Calculates reactive power using available measurements.

    Uses apparent/active power (Q = √(S² - P²)) or voltage/current/power factor (Q = V × I × sin(acos(PF))).
    Priority is given to power-based calculation when both methods are available.

    Args:
        prefix (str): Phase prefix for node identification.
        node (Node): Target node to store the calculated reactive power value.
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes.
    """

    v_node = meter_util.find_node(f"{prefix}voltage", meter_nodes)
    i_node = meter_util.find_node(f"{prefix}current", meter_nodes)
    pf_node = meter_util.find_node(f"{prefix}power_factor", meter_nodes)
    s_node = meter_util.find_node(f"{prefix}apparent_power", meter_nodes)
    p_node = meter_util.find_node(f"{prefix}active_power", meter_nodes)

    value: Optional[int | float] = None

    if s_node is not None and p_node is not None:

        s_value = meter_util.get_numeric_value(s_node)
        p_value = meter_util.get_numeric_value(p_node)

        if s_value is not None and p_value is not None:

            scaled_s = calculation.get_scaled_value(s_value, s_node.config.unit)
            scaled_p = calculation.get_scaled_value(p_value, p_node.config.unit)
            value = math.sqrt(scaled_s**2 - scaled_p**2)

    elif value is None and v_node is not None and i_node is not None and pf_node is not None:

        v_value = meter_util.get_numeric_value(v_node)
        i_value = meter_util.get_numeric_value(i_node)
        pf_value = meter_util.get_numeric_value(pf_node)

        if v_value is not None and i_value is not None and pf_value is not None:

            scaled_v = calculation.get_scaled_value(v_value, v_node.config.unit)
            scaled_i = calculation.get_scaled_value(i_value, i_node.config.unit)
            scaled_pf = calculation.get_scaled_value(pf_value, pf_node.config.unit)
            value = scaled_v * scaled_i * math.sin(math.acos(scaled_pf))

    scaled_value = calculation.apply_output_scaling(value, node.config.unit) if value is not None else None
    node.processor.set_value(scaled_value)


def calculate_pf(prefix: str, node: Node, meter_nodes: Dict[str, Node]) -> None:
    """
    Calculates power factor values for a node using active and reactive power measurements.

    For total power factor, calculates the overall power factor by summing active and reactive
    power from all three phases and computing the cosine of the arctangent ratio.

    For individual phases, calculates power factor using the phase-specific active and
    reactive power values. Returns 0.0 when active power is zero, and None when required
    measurements are unavailable.

    Args:
        prefix (str): Phase prefix ("l1_", "l2_", "l3_", "total_") or line-to-line prefix
        node (Node): Target node to store the calculated power factor value
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes
    """

    if prefix == "total_":
        total_p = 0.0
        total_q = 0.0

        for p in ("l1_", "l2_", "l3_"):
            (p_node, p_val) = meter_util.get_numeric_node_with_value(f"{p}active_power", meter_nodes)
            (q_node, q_val) = meter_util.get_numeric_node_with_value(f"{p}reactive_power", meter_nodes)

            if p_val is None or q_val is None:
                node.processor.set_value(None)
                return

            scaled_p = calculation.get_scaled_value(p_val, p_node.config.unit)
            scaled_q = calculation.get_scaled_value(q_val, q_node.config.unit)

            total_p += scaled_p
            total_q += scaled_q

        if total_p == 0:
            node.processor.set_value(0.0)
        else:
            node.processor.set_value(math.cos(math.atan(total_q / total_p)))

        return

    # Per-phase PF
    (p_node, p_val) = meter_util.get_numeric_node_with_value(f"{prefix}active_power", meter_nodes)
    (q_node, q_val) = meter_util.get_numeric_node_with_value(f"{prefix}reactive_power", meter_nodes)

    if p_val is None or q_val is None:
        node.processor.set_value(None)
        return

    p_val = calculation.get_scaled_value(p_val, p_node.config.unit)
    q_val = calculation.get_scaled_value(q_val, q_node.config.unit)

    if p_val == 0:
        node.processor.set_value(0.0)
    else:
        node.processor.set_value(math.cos(math.atan(q_val / p_val)))


def calculate_pf_direction(prefix: str, node: Node, meter_nodes: Dict[str, Node], meter_options: EnergyMeterOptions) -> None:
    """
    Calculates power factor direction for a node based on meter configuration and power measurements.

    Determines whether the power factor is leading, lagging, unitary, or unknown based on:
    - Unity power factor (1.00) results in UNITARY direction
    - Negative reactive power configuration uses reactive power sign to determine direction
    - Forward/reverse energy configuration uses reactive energy direction
    - Falls back to UNKNOWN when insufficient data is available

    The direction affects energy accounting and power quality analysis.

    Args:
        prefix (str): Phase prefix ("l1_", "l2_", "l3_", "total_") or line-to-line prefix
        node (Node): Target node to store the calculated power factor direction
        meter_nodes (Dict[str, Node]): Dictionary containing all meter nodes
        meter_options (EnergyMeterOptions): Configuration options for direction calculation
    """

    pf_node = meter_util.find_node(f"{prefix}power_factor", meter_nodes)
    er_node = meter_util.find_node(f"{prefix}reactive_energy", meter_nodes)

    if pf_node is not None and pf_node.processor.value == 1.00:
        node.processor.set_value(PowerFactorDirection.UNITARY.value)
        if er_node and isinstance(er_node.processor, NumericNodeProcessor):
            er_node.processor.reset_direction()
        return

    if meter_options.negative_reactive_power:
        q_node = meter_util.find_node(f"{prefix}reactive_power", meter_nodes)

        if q_node:

            q_value = meter_util.get_numeric_value(q_node)
            if q_value is not None:
                node.processor.set_value(PowerFactorDirection.LAGGING.value if q_value > 0.0 else PowerFactorDirection.LEADING.value)
            else:
                node.processor.set_value(PowerFactorDirection.UNKNOWN.value)

        else:
            node.processor.set_value(PowerFactorDirection.UNKNOWN.value)
        return

    elif meter_options.read_separate_forward_reverse_energy:
        if er_node and isinstance(er_node.processor, NumericNodeProcessor):
            if er_node.processor.positive_direction:
                node.processor.set_value(PowerFactorDirection.LAGGING.value)
            elif er_node.processor.negative_direction:
                node.processor.set_value(PowerFactorDirection.LEADING.value)
            else:
                node.processor.set_value(PowerFactorDirection.UNKNOWN.value)
        else:
            node.processor.set_value(PowerFactorDirection.UNKNOWN.value)
        return

    node.processor.set_value(PowerFactorDirection.UNKNOWN.value)
