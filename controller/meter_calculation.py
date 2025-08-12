###########EXTERNAL IMPORTS############

from typing import Dict
import math

#######################################

#############LOCAL IMPORTS#############

from controller.types import EnergyMeterOptions, PowerFactorDirection
from controller.node import Node
from controller.meter_nodes import EnergyMeterNodes

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
            phase_key = f"{p}{energy_type}_energy"
            phase_node = meter_nodes.get(phase_key)

            if phase_node is None or phase_node.value is None:
                node.set_value(None)
                return

            total += EnergyMeterNodes.get_scaled_value(phase_node)

        scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
        node.set_value(scaled_total)
        return

    if meter_options.read_separate_forward_reverse_energy:
        forward_key = f"{prefix}forward_{energy_type}_energy"
        reverse_key = f"{prefix}reverse_{energy_type}_energy"
        forward = meter_nodes.get(forward_key)
        reverse = meter_nodes.get(reverse_key)

        if forward and reverse and forward.value is not None and reverse.value is not None:
            scaled_forward = EnergyMeterNodes.get_scaled_value(forward)
            scaled_reverse = EnergyMeterNodes.get_scaled_value(reverse)
            scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_forward - scaled_reverse, node)
            node.set_value(scaled_value)

    elif not meter_options.read_energy_from_meter:
        power_key = f"{prefix}{energy_type}_power"
        power_node = meter_nodes.get(power_key)

        if power_node and power_node.value is not None and power_node.elapsed_time is not None:
            elapsed_hours = power_node.elapsed_time / 3600.0
            scaled_power = EnergyMeterNodes.get_scaled_value(power_node)
            scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_power * elapsed_hours, node)
            node.set_value(scaled_value)


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
            phase_key = f"{p}{power_type}_power"
            phase_node = meter_nodes.get(phase_key)

            if phase_node is None or phase_node.value is None:
                node.set_value(None)
                return

            total += EnergyMeterNodes.get_scaled_value(phase_node)

        scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
        node.set_value(scaled_total)
        return

    # Individual phase
    v = EnergyMeterNodes.get_scaled_value(meter_nodes.get(f"{prefix}voltage"))
    i = EnergyMeterNodes.get_scaled_value(meter_nodes.get(f"{prefix}current"))
    pf_node = meter_nodes.get(f"{prefix}power_factor")
    pf = pf_node.value if pf_node else None

    p = EnergyMeterNodes.get_scaled_value(meter_nodes.get(f"{prefix}active_power"))
    q = EnergyMeterNodes.get_scaled_value(meter_nodes.get(f"{prefix}reactive_power"))
    s = EnergyMeterNodes.get_scaled_value(meter_nodes.get(f"{prefix}apparent_power"))

    val = None

    if power_type == "apparent":
        if p is not None and q is not None:
            val = math.sqrt(p**2 + q**2)
        elif v is not None and i is not None:
            val = v * i

    elif power_type == "reactive":
        if s is not None and p is not None and s >= p:
            val = math.sqrt(s**2 - p**2)
        elif v is not None and i is not None and pf is not None and -1.0 <= pf <= 1.0:
            val = v * i * math.sin(math.acos(pf))

    elif power_type == "active":
        if s is not None and q is not None and s >= q:
            val = math.sqrt(s**2 - q**2)
        elif v is not None and i is not None and pf is not None:
            val = v * i * pf

    scaled_value = EnergyMeterNodes.apply_output_scaling(val, node) if val is not None else None
    node.set_value(scaled_value)


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
            p_node = meter_nodes.get(f"{p}active_power")
            q_node = meter_nodes.get(f"{p}reactive_power")

            p_val = EnergyMeterNodes.get_scaled_value(p_node) if p_node else None
            q_val = EnergyMeterNodes.get_scaled_value(q_node) if q_node else None

            if p_val is None or q_val is None:
                node.set_value(None)
                return

            total_p += p_val
            total_q += q_val

        if total_p == 0:
            node.set_value(0.0)
        else:
            node.set_value(math.cos(math.atan(total_q / total_p)))

        return

    # Per-phase PF
    p_node = meter_nodes.get(f"{prefix}active_power")
    q_node = meter_nodes.get(f"{prefix}reactive_power")

    p = EnergyMeterNodes.get_scaled_value(p_node) if p_node else None
    q = EnergyMeterNodes.get_scaled_value(q_node) if q_node else None

    if p is None or q is None:
        node.set_value(None)
        return

    if p == 0:
        node.set_value(0.0)
    else:
        node.set_value(math.cos(math.atan(q / p)))


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

    pf_node = meter_nodes.get(f"{prefix}power_factor")
    pf = pf_node.value if pf_node else None
    er_node = meter_nodes.get(f"{prefix}reactive_energy")

    if pf is not None and pf == 1.00:
        node.set_value(PowerFactorDirection.UNITARY.value)
        if er_node:
            er_node.reset_direction()
        return

    if meter_options.negative_reactive_power:
        q_node = meter_nodes.get(f"{prefix}reactive_power")

        if q_node and q_node.value is not None:
            node.set_value(PowerFactorDirection.LAGGING.value if q_node.value > 0.0 else PowerFactorDirection.LEADING.value)
        else:
            node.set_value(PowerFactorDirection.UNKNOWN.value)
        return

    elif meter_options.read_separate_forward_reverse_energy:
        if er_node:
            if er_node.positive_direction:
                node.set_value(PowerFactorDirection.LAGGING.value)
            elif er_node.negative_direction:
                node.set_value(PowerFactorDirection.LEADING.value)
            else:
                node.set_value(PowerFactorDirection.UNKNOWN.value)
        else:
            node.set_value(PowerFactorDirection.UNKNOWN.value)
        return

    node.set_value(PowerFactorDirection.UNKNOWN.value)
