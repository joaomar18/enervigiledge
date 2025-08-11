###########EXERTNAL IMPORTS############

from datetime import datetime
import asyncio
import math
import traceback
from typing import Dict, Any
from abc import ABC, abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node import Node
from controller.types import Protocol, EnergyMeterType, PowerFactorDirection, EnergyMeterOptions, EnergyMeterRecord
from controller.meter_nodes import EnergyMeterNodes
from mqtt.client import MQTTMessage
from db.timedb import Measurement
import util.functions_generic as functions_generic
from util.debug import LoggerManager

#######################################


class EnergyMeter(Device):
    """
    Specialized device class representing an energy meter.

    This class extends the generic `Device` class to handle energy meter-specific logic,
    including real-time measurement, node calculation, logging, and MQTT publishing.

    Key Features:
    - Supports single-phase and three-phase configurations.
    - Automatically calculates derived values such as power, energy, power factor, and PF direction.
    - Integrates configurable options for reading raw or calculated energy values.
    - Handles logging of measurements and statistical data.
    - Publishes node values over MQTT in a structured format.

    Attributes:
        meter_type (EnergyMeterType): Type of the energy meter (single or three phase).
        meter_options (EnergyMeterOptions): Configuration flags controlling how energy and power are interpreted.
        meter_nodes (EnergyMeterNodes): Manager for validating and handling node configurations and relationships.
        calculation_methods (Dict[str, Tuple[Callable, Dict[str, Any]]]): Map of suffixes to calculation methods.
        disconnected_calculation (bool): Flag to make the device make one and only calculation of nodes on disconnection
    """

    @staticmethod
    def get_node_phase(node: Node) -> str:
        """
        Returns the phase prefix of a node based on its name.

        Identifies whether a node belongs to a specific phase (e.g., "l1_", "l2_", "l3_"),
        a line-to-line voltage (e.g., "l1_l2_"), or represents a totalized value ("total_").

        Args:
            node (Node): The node whose name is to be analyzed.

        Returns:
            str: The phase prefix ("l1_", "l2_", "l3_", "l1_l2_", etc.), or an empty string if none match.
        """
        for phase in ("l1_l2_", "l2_l3_", "l3_l1_", "l1_", "l2_", "l3_", "total_"):
            if node.name.startswith(phase):
                return phase
        return ""

    def __init__(
        self,
        id: int,
        name: str,
        protocol: Protocol,
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        meter_nodes: set[Node],
    ):

        super().__init__(id=id, name=name, protocol=protocol, publish_queue=publish_queue, measurements_queue=measurements_queue, nodes=meter_nodes)
        self.meter_type = meter_type
        self.meter_options = meter_options

        try:
            self.meter_nodes = EnergyMeterNodes(meter_type=self.meter_type, meter_options=self.meter_options, nodes=meter_nodes)
            self.meter_nodes.set_energy_nodes_incremental()
            self.meter_nodes.validate_nodes()
        except Exception as e:
            raise Exception(f"Failed to initialize EnergyMeter '{name}' with id {id}: {e}")

        self.calculation_methods = {
            "_reactive_energy": (self.calculate_energy, {"energy_type": "reactive"}),
            "_active_energy": (self.calculate_energy, {"energy_type": "active"}),
            "_active_power": (self.calculate_power, {"power_type": "active"}),
            "_reactive_power": (self.calculate_power, {"power_type": "reactive"}),
            "_apparent_power": (self.calculate_power, {"power_type": "apparent"}),
            "_power_factor_direction": (self.calculate_pf_direction, {}),
            "_power_factor": (self.calculate_pf, {}),
        }

        self.disconnected_calculation = False

    @abstractmethod
    def start(self) -> None:
        """
        Starts the energy meter device operations.

        This method should be implemented by subclasses to initialize and start
        the communication protocol, begin data acquisition, and set up any
        necessary background tasks for the specific meter type.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """
        Stops the energy meter device operations.

        This method should be implemented by subclasses to gracefully shutdown
        the communication protocol, stop data acquisition, and clean up any
        resources or background tasks associated with the specific meter type.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    async def process_nodes(self) -> None:
        """
        Executes the full data processing cycle for the energy meter.

        If the meter is connected:
            - Clears disconnected flag
            - Calculates all nodes
            - Logs and publishes values concurrently

        If the meter is disconnected and hasn't been processed since disconnection:
            - Runs one calculation pass
            - Sets disconnected flag to avoid repeated unnecessary processing
        """

        if self.connected:
            self.disconnected_calculation = False
            await self.calculate_nodes()
            await asyncio.gather(self.log_nodes(), self.publish_nodes())
        elif not self.disconnected_calculation:
            await self.calculate_nodes()
            self.disconnected_calculation = True

    async def log_nodes(self) -> None:
        """
        Logs node data for nodes with logging enabled if their logging period has elapsed.

        For each node:
            - Initializes logging timestamp on the first run.
            - Submits a log entry if the configured logging period has passed.
            - Resets directional energy for energy-related nodes not independently logged.
        """

        current_time = datetime.now()

        for node in self.meter_nodes.nodes.values():
            if not node.logging or not node.enabled:
                continue

            if node.last_log_datetime is None:
                node.last_log_datetime = current_time
                continue

            elapsed_time = functions_generic.subtract_datetime_mins(current_time, node.last_log_datetime)

            if elapsed_time >= node.logging_period:
                log_data = [node.submit_log(current_time)]
                log_db = f"{self.name}_{self.id}"
                await self.measurements_queue.put(Measurement(db=log_db, data=log_data))
                self.reset_directional_energy(node)

    def reset_directional_energy(self, node: Node):
        """
        Resets the values of directional and incremental energy nodes if they are not independently logged.
        Also resets corresponding phase nodes if the parent node is a total node.

        Args:
            node (Node): The node used to identify related energy nodes.
        """

        prefix = EnergyMeter.get_node_phase(node)

        for energy_type in ("reactive", "active"):
            if f"_{energy_type}_energy" not in node.name:
                continue

            # Reset directional nodes (forward, reverse)
            for direction in ("forward", "reverse"):
                key = f"{prefix}{direction}_{energy_type}_energy"
                energy_node = self.meter_nodes.nodes.get(key)
                if energy_node and not energy_node.logging:
                    energy_node.reset_value()

            # If total_, also reset all phase equivalents
            if prefix == "total_":
                for p in ("l1_", "l2_", "l3_"):
                    # Directional
                    for direction in ("forward", "reverse"):
                        phase_dir_key = f"{p}{direction}_{energy_type}_energy"
                        phase_dir_node = self.meter_nodes.nodes.get(phase_dir_key)
                        if phase_dir_node and not phase_dir_node.logging:
                            phase_dir_node.reset_value()

                    # Normal incremental
                    phase_norm_key = f"{p}{energy_type}_energy"
                    phase_norm_node = self.meter_nodes.nodes.get(phase_norm_key)
                    if phase_norm_node and not phase_norm_node.logging:
                        phase_norm_node.reset_value()

    async def calculate_nodes(self) -> None:
        """
        Asynchronously calculates values for all nodes marked as 'calculated'.

        For each calculated node:
            - Determines its phase prefix.
            - Finds the appropriate calculation method based on its name.
            - Executes the calculation in a background thread to avoid blocking.

        Notes:
            Uses `asyncio.to_thread` to offload CPU-bound work.
        """

        logger = LoggerManager.get_logger(__name__)

        calculated_nodes: Dict[str, Node] = {name: node for name, node in self.meter_nodes.nodes.items() if node.calculated and node.enabled}
        tasks = []

        for node in calculated_nodes.values():

            prefix = EnergyMeter.get_node_phase(node)

            for key, (func, kwargs) in self.calculation_methods.items():
                if key in node.name:
                    tasks.append(asyncio.to_thread(func, node=node, prefix=prefix, **kwargs))
                    break

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_nodes = []

        for node, result in zip(calculated_nodes.values(), results):
            if isinstance(result, Exception):
                failed_nodes.append((node.name, result))
                continue

        if failed_nodes:
            for name, e in failed_nodes:
                tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                logger.exception(f"Failed to calculate node {name} from device {self.name} with id {self.id}:\n{tb}")

    def calculate_energy(self, prefix: str, energy_type: str, node: Node) -> None:
        """
        Calculates energy value based on the device configuration.

        For total nodes, the value is the sum of the three phase values.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            energy_type (str): "active" or "reactive".
            node (Node): The node that will receive the calculated value.
        """

        if prefix == "total_":
            total = 0.0
            for p in ("l1_", "l2_", "l3_"):
                phase_key = f"{p}{energy_type}_energy"
                phase_node = self.meter_nodes.nodes.get(phase_key)

                if phase_node is None or phase_node.value is None:
                    node.set_value(None)
                    return

                total += EnergyMeterNodes.get_scaled_value(phase_node)

            scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
            node.set_value(scaled_total)
            return

        if self.meter_options.read_separate_forward_reverse_energy:
            forward_key = f"{prefix}forward_{energy_type}_energy"
            reverse_key = f"{prefix}reverse_{energy_type}_energy"
            forward = self.meter_nodes.nodes.get(forward_key)
            reverse = self.meter_nodes.nodes.get(reverse_key)

            if forward and reverse and forward.value is not None and reverse.value is not None:
                scaled_forward = EnergyMeterNodes.get_scaled_value(forward)
                scaled_reverse = EnergyMeterNodes.get_scaled_value(reverse)
                scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_forward - scaled_reverse, node)
                node.set_value(scaled_value)

        elif not self.meter_options.read_energy_from_meter:
            power_key = f"{prefix}{energy_type}_power"
            power_node = self.meter_nodes.nodes.get(power_key)

            if power_node and power_node.value is not None and power_node.elapsed_time is not None:
                elapsed_hours = power_node.elapsed_time / 3600.0
                scaled_power = EnergyMeterNodes.get_scaled_value(power_node)
                scaled_value = EnergyMeterNodes.apply_output_scaling(scaled_power * elapsed_hours, node)
                node.set_value(scaled_value)

    def calculate_power(self, prefix: str, power_type: str, node: Node):
        """
        Calculates the specified type of power (active, reactive, or apparent) for a given phase or total.

        Power calculations prioritize using known power values (e.g., active + reactive)
        over raw voltage/current inputs, which may be less reliable.

        For total nodes, the result is the sum of all corresponding phase power values.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            power_type (str): One of "active", "reactive", or "apparent".
            node (Node): The node to assign the calculated value to.
        """

        if prefix == "total_":
            total = 0.0
            for p in ("l1_", "l2_", "l3_"):
                phase_key = f"{p}{power_type}_power"
                phase_node = self.meter_nodes.nodes.get(phase_key)

                if phase_node is None or phase_node.value is None:
                    node.set_value(None)
                    return

                total += EnergyMeterNodes.get_scaled_value(phase_node)

            scaled_total = EnergyMeterNodes.apply_output_scaling(total, node)
            node.set_value(scaled_total)
            return

        # Individual phase
        v = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}voltage"))
        i = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}current"))
        pf_node = self.meter_nodes.nodes.get(f"{prefix}power_factor")
        pf = pf_node.value if pf_node else None

        p = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}active_power"))
        q = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}reactive_power"))
        s = EnergyMeterNodes.get_scaled_value(self.meter_nodes.nodes.get(f"{prefix}apparent_power"))

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

    def calculate_pf(self, prefix: str, node: Node) -> None:
        """
        Calculates the power factor (PF) for a given phase or total.

        Power factor is computed using the formula:
            PF = cos(atan(Q / P))
        where:
            P = active power
            Q = reactive power

        If P is zero, sets PF to 0.0 to avoid division by zero.
        If any required input is missing, sets PF to None.

        For total power factor, uses the sum of P and Q across all three phases.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_", or "total_").
            node (Node): The node that will receive the calculated PF value.
        """

        if prefix == "total_":
            total_p = 0.0
            total_q = 0.0

            for p in ("l1_", "l2_", "l3_"):
                p_node = self.meter_nodes.nodes.get(f"{p}active_power")
                q_node = self.meter_nodes.nodes.get(f"{p}reactive_power")

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
        p_node = self.meter_nodes.nodes.get(f"{prefix}active_power")
        q_node = self.meter_nodes.nodes.get(f"{prefix}reactive_power")

        p = EnergyMeterNodes.get_scaled_value(p_node) if p_node else None
        q = EnergyMeterNodes.get_scaled_value(q_node) if q_node else None

        if p is None or q is None:
            node.set_value(None)
            return

        if p == 0:
            node.set_value(0.0)
        else:
            node.set_value(math.cos(math.atan(q / p)))

    def calculate_pf_direction(self, prefix: str, node: Node) -> None:
        """
        Calculates the direction of the power factor for a given phase.

        The direction can be:
            - UNITARY: If PF is 1.0.
            - LAGGING or LEADING: Based on reactive power or reactive energy direction.
            - UNKNOWN: If direction cannot be determined.

        Logic flow:
            1. If PF >= 0.99 → UNTIARY and reset energy direction (if applicable).
            2. If using `negative_reactive_power`, sign of Q determines direction.
            3. If using `read_separate_forward_reverse_energy`, direction is taken from reactive energy trend.
            4. Otherwise, defaults to UNKNOWN.

        Args:
            prefix (str): Phase prefix (e.g., "l1_", "l2_", "l3_").
            node (Node): The node that will receive the calculated direction.
        """

        pf_node = self.meter_nodes.nodes.get(f"{prefix}power_factor")
        pf = pf_node.value if pf_node else None
        er_node = self.meter_nodes.nodes.get(f"{prefix}reactive_energy")

        if pf is not None and pf == 1.00:
            node.set_value(PowerFactorDirection.UNITARY.value)
            if er_node:
                er_node.reset_direction()
            return

        if self.meter_options.negative_reactive_power:
            q_node = self.meter_nodes.nodes.get(f"{prefix}reactive_power")

            if q_node and q_node.value is not None:
                node.set_value(PowerFactorDirection.LAGGING.value if q_node.value > 0.0 else PowerFactorDirection.LEADING.value)
            else:
                node.set_value(PowerFactorDirection.UNKNOWN.value)
            return

        elif self.meter_options.read_separate_forward_reverse_energy:
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

    async def publish_nodes(self):
        """
        Publishes the current values of all nodes marked for publishing via MQTT.

        For each node:
            - Checks if the node is marked as `publish=True` and has a non-null value.
            - Serializes the node's data using `get_publish_format()`.

        The result is sent as a single MQTT message to a topic formatted as:
            "<device_name>_<device_id>_nodes"

        Raises:
            Exception: If `get_publish_format()` raises due to missing value.
        """

        publish_nodes: Dict[str, Node] = {name: node for name, node in self.meter_nodes.nodes.items() if node.publish and node.value is not None}

        topic = f"{self.name}_{self.id}_nodes"
        payload: Dict[str, Any] = {}

        for node in publish_nodes.values():
            payload[node.name] = node.get_publish_format()

        if payload:
            await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))

    def get_device_state(self) -> Dict[str, Any]:
        """
        Returns the current state of the energy meter device, including metadata and configuration.

        Returns:
            Dict[str, Any]: A dictionary containing the device's:
                - ID
                - Name
                - Protocol
                - Connection status
                - Meter options
                - Meter type
        """

        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
            "options": self.meter_options.get_meter_options(),
            "type": self.meter_type,
        }

    @abstractmethod
    def get_meter_record(self) -> EnergyMeterRecord:
        """
        Creates a database record representation of the energy meter configuration.

        Returns:
            EnergyMeterRecord: Record containing meter configuration and all associated nodes.
        """
        pass
