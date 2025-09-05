###########EXERTNAL IMPORTS############

from datetime import datetime
import asyncio
import traceback
from typing import Dict, Any, Type, Set, Callable
from abc import abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node import Node
from controller.types import Protocol, EnergyMeterType, EnergyMeterOptions, EnergyMeterRecord, NodeRecord
from controller.meter.nodes import EnergyMeterNodes
import controller.meter.calculation as calculation
from mqtt.client import MQTTMessage
from db.timedb import Measurement
import util.functions.generic as functions_generic
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

    Args:
        id (int): Unique identifier of the energy meter.
        name (str): Display name of the meter.
        protocol (Protocol): Communication protocol used by the device.
        publish_queue (asyncio.Queue): Queue for outgoing MQTT messages.
        measurements_queue (asyncio.Queue): Queue for logging measurements.
        meter_type (EnergyMeterType): Type of the energy meter (single or three phase).
        meter_options (EnergyMeterOptions): Configuration flags controlling how energy and power are interpreted.
        communication_options (Type): Protocol-specific communication configuration (e.g., ModbusRTUOptions, OPCUAOptions).
        nodes (Set[Node]): Set of nodes representing individual measurement points.
        on_connection_change (Callable[[int, bool], None] | None): Optional callback triggered when the device connection state changes.
            Expects two parameters: device id (int) and state (bool).

    Attributes:
        meter_type (EnergyMeterType): Type of the energy meter (single or three phase).
        meter_options (EnergyMeterOptions): Configuration flags controlling how energy and power are interpreted.
        communication_options (Type): Protocol-specific communication configuration (e.g., ModbusRTUOptions, OPCUAOptions).
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
        publish_queue: asyncio.Queue,
        measurements_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        communication_options: Type,
        nodes: Set[Node],
        protocol: Protocol = Protocol.NONE,
        on_connection_change: Callable[[int, bool], None] | None = None,
    ):

        super().__init__(
            id=id,
            name=name,
            protocol=protocol,
            publish_queue=publish_queue,
            measurements_queue=measurements_queue,
            nodes=nodes,
            on_connection_change=on_connection_change,
        )
        self.meter_type = meter_type
        self.meter_options = meter_options
        self.communication_options = communication_options

        try:
            self.meter_nodes = EnergyMeterNodes(meter_type=self.meter_type, meter_options=self.meter_options, nodes=nodes)
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
    async def start(self) -> None:
        """
        Starts the energy meter device operations and communication protocol.
        """

        pass

    @abstractmethod
    async def stop(self) -> None:
        """
        Stops the energy meter device operations and cleans up resources.
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
        Calculates energy values for the specified node using meter configuration.
        """

        calculation.calculate_energy(prefix, energy_type, node, self.meter_nodes.nodes, self.meter_options)

    def calculate_power(self, prefix: str, power_type: str, node: Node):
        """
        Calculates power values for the specified node using meter nodes.
        """

        calculation.calculate_power(prefix, power_type, node, self.meter_nodes.nodes)

    def calculate_pf(self, prefix: str, node: Node) -> None:
        """
        Calculates power factor values for the specified node using meter nodes.
        """

        calculation.calculate_pf(prefix, node, self.meter_nodes.nodes)

    def calculate_pf_direction(self, prefix: str, node: Node) -> None:
        """
        Calculates power factor direction values for the specified node using meter configuration.
        """

        calculation.calculate_pf_direction(prefix, node, self.meter_nodes.nodes, self.meter_options)

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
                - Communication options
                - Meter type
        """

        return {
            "id": self.id,
            "name": self.name,
            "protocol": self.protocol,
            "connected": self.connected,
            "options": self.meter_options.get_meter_options(),
            "communication_options": self.communication_options.get_communication_options(),
            "type": self.meter_type,
        }

    def get_meter_record(self) -> EnergyMeterRecord:
        """
        Creates a database record representation of the energy meter configuration.

        Returns:
            EnergyMeterRecord: Record containing meter configuration and all associated nodes.
        """

        node_records: Set[NodeRecord] = set()

        for node in self.nodes:
            record = node.get_node_record()
            node_records.add(record)

        return EnergyMeterRecord(
            name=self.name,
            id=self.id,
            protocol=self.protocol,
            type=self.meter_type,
            options=self.meter_options.get_meter_options(),
            communication_options=self.communication_options.get_communication_options(),
            nodes=node_records,
        )
