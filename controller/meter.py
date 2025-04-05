###########EXERTNAL IMPORTS############

from datetime import datetime
import asyncio
import math
from typing import List, Dict, Any

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device, Node
from mqtt.client import MQTTMessage
from db.timedb import Measurement
from util.debug import LoggerManager
import util.functions as functions

#######################################


class UnitError(Exception):
    pass


class NodeUnknownError(Exception):
    pass


class NodeMissingError(Exception):
    pass


class EnergyMeterType:
    SINGLE_PHASE = "SINGLE_PHASE"
    THREE_PHASE = "THREE_PHASE"


class PowerFactorDirection:
    UNKNOWN = "UNKNOWN"
    UNITARY = "UNITARY"
    LAGGING = "LAGGING"
    LEADING = "LEADING"


class EnergyMeterOptions:
    def __init__(self, read_energy_from_meter: bool, read_separate_forward_reverse_energy: bool, negative_reactive_power: bool, frequency_reading: bool):

        self.read_energy_from_meter = read_energy_from_meter
        self.read_separate_forward_reverse_energy = read_separate_forward_reverse_energy
        self.negative_reactive_power = negative_reactive_power
        self.frequency_reading = frequency_reading

    def get_meter_options(self) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        output["read_energy_from_meter"] = self.read_energy_from_meter
        output["read_separate_forward_reverse_energy"] = self.read_separate_forward_reverse_energy
        output["negative_reactive_power"] = self.negative_reactive_power
        output["frequency_reading"] = self.frequency_reading
        return output


class EnergyMeterNodes:
    VALID_NODES = {
        "voltage",
        "current",
        "active_power",
        "reactive_power",
        "apparent_power",
        "power_factor",
        "power_factor_direction",
        "frequency",
        "active_energy",
        "reactive_energy",
        "forward_active_energy",
        "reverse_active_energy",
        "forward_reactive_energy",
        "reverse_reactive_energy",
    }

    VALID_UNITS = {
        "voltage": {"V"},
        "current": {"mA", "A"},
        "active_power": {"W", "kW"},
        "reactive_power": {"VAr", "kVAr"},
        "apparent_power": {"VA", "kVA"},
        "power_factor": {""},
        "power_factor_direction": {""},
        "frequency": {"Hz"},
        "active_energy": {"Wh", "kWh"},
        "reactive_energy": {"VArh", "kVArh"},
        "forward_active_energy": {"Wh", "kWh"},
        "reverse_active_energy": {"Wh", "kWh"},
        "forward_reactive_energy": {"VArh", "kVArh"},
        "reverse_reactive_energy": {"VArh", "kVArh"},
    }

    @staticmethod
    def validate_node(node: Node):
        base_name = functions.remove_phase_string(node.name)

        if base_name not in EnergyMeterNodes.VALID_NODES:
            raise NodeUnknownError(f"Invalid node {node.name} with type {node.type}  trying to be initiated")

        if node.unit not in EnergyMeterNodes.VALID_UNITS[base_name]:
            raise UnitError(f"Invalid unit '{node.unit}' for node '{node.name}'. Expected one of {EnergyMeterNodes.VALID_UNITS[base_name]}")

    def __init__(self, meter_type: EnergyMeterType, meter_options: EnergyMeterOptions, nodes: set[Node]):
        self.meter_type = meter_type
        self.meter_options = meter_options
        self.nodes: Dict[str, Node] = {node.name: node for node in nodes}

    def validate_nodes(self):
        for node in self.nodes.values():
            EnergyMeterNodes.validate_node(node)
        for phase in ("l1_", "l2_", "l3_"):
            self.validate_energy_nodes(phase, "active")
            self.validate_energy_nodes(phase, "reactive")
            self.validate_power_nodes(phase, "active")
            self.validate_power_nodes(phase, "reactive")
            self.validate_power_nodes(phase, "apparent")
            self.validate_pf_nodes(phase)
            self.validate_pf_direction_nodes(phase)

    def validate_energy_nodes(self, phase: str, energy_type: str):
        node_name = f"{phase}{energy_type}_energy"
        node = self.nodes.get(node_name)
        if node:
            if node.calculated:
                if self.meter_options.read_separate_forward_reverse_energy:
                    forward = self.nodes.get(f"{phase}forward_{energy_type}_energy")
                    reverse = self.nodes.get(f"{phase}reverse_{energy_type}_energy")

                    if forward and reverse:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")

                elif not self.meter_options.read_energy_from_meter:
                    power = self.nodes.get(f"{phase}{energy_type}_power")

                    if power:
                        return
                    raise NodeMissingError(f"Missing node for {node_name} calculation: Check nodes")

    def validate_power_nodes(self, phase: str, power_type: str):
        node_name = f"{phase}{power_type}_power"
        node = self.nodes.get(node_name)
        if node:
            if node.calculated:
                v = self.nodes.get(phase + "voltage")
                i = self.nodes.get(phase + "current")

                if power_type == "active" or power_type == "reactive":
                    pf = self.nodes.get(phase + "power_factor")

                    if v and i and pf:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")

                elif power_type == "apparent":

                    if v and i:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")

    def validate_pf_nodes(self, phase: str):
        node_name = phase + "power_factor"
        node = self.nodes.get(node_name)
        if node:
            if node.calculated:
                p = self.nodes.get(phase + "active_power")
                q = self.nodes.get(phase + "reactive_power")
                if p and q:
                    return
                raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")

    def validate_pf_direction_nodes(self, phase: str):
        node_name = phase + "power_factor_direction"
        node = self.nodes.get(node_name)
        if node:
            if node.calculated:
                pf = self.nodes.get(phase + "power_factor")
                if self.meter_options.negative_reactive_power:
                    q = self.nodes.get(phase + "reactive_power")

                    if pf and q:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")
                elif self.meter_options.read_separate_forward_reverse_energy:
                    er = self.nodes.get(phase + "reactive_energy")

                    if pf and er:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")
                else:
                    if pf:
                        return
                    raise NodeMissingError(f"Missing nodes for {node_name} calculation: Check nodes")

    def set_energy_nodes_incremental(self):
        for node in self.nodes.values():
            if "energy" in node.name:
                node.set_incremental_node(True)


class EnergyMeter(Device):

    @staticmethod
    def get_node_phase(node: Node) -> str:
        for phase in ("l1_", "l2_", "l3_"):
            if node.name.startswith(phase):
                return phase
        return ""

    def __init__(
        self,
        id: int,
        name: str,
        protocol: int,
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
            raise Exception(f"Failed to initialize EnergyMeter '{name}' with id {id} due to invalid definitions. {e}")

        self.calculation_methods = {
            "_reactive_energy": (self.calculate_energy, {"energy_type": "reactive"}),
            "_active_energy": (self.calculate_energy, {"energy_type": "active"}),
            "_active_power": (self.calculate_power, {"power_type": "active"}),
            "_reactive_power": (self.calculate_power, {"power_type": "reactive"}),
            "_apparent_power": (self.calculate_power, {"power_type": "apparent"}),
            "_power_factor_direction": (self.calculate_pf_direction, {}),
            "_power_factor": (self.calculate_pf, {}),
        }

    async def process_nodes(self):
        if self.connected:
            await self.calculate_nodes()
            await self.log_nodes()
            await self.publish_nodes()

    async def log_nodes(self):
        current_date_time = datetime.now()
        for node in self.meter_nodes.nodes.values():
            if node.logging:
                if node.last_log_datetime is None:
                    node.last_log_datetime = current_date_time
                elif functions.subtracte_datetime_mins(current_date_time, node.last_log_datetime) >= node.logging_period:
                    log_data: List[Dict] = [node.submit_log(current_date_time)]
                    log_db = f"{self.name}_{self.id}"
                    await self.measurements_queue.put(Measurement(db=log_db, data=log_data))
                    self.reset_directional_energy(node)

    def reset_directional_energy(self, node: Node):
        prefix = EnergyMeter.get_node_phase(node)

        for energy_type in ("reactive", "active"):
            if f"_{energy_type}_energy" in node.name:
                for direction in ("forward", "reverse"):
                    key = f"{prefix}{direction}_{energy_type}_energy"
                    energy_node = self.meter_nodes.nodes.get(key)
                    if energy_node and not energy_node.logging:
                        energy_node.reset_value()

    async def calculate_nodes(self):

        for node in self.meter_nodes.nodes.values():
            if not node.calculated:
                continue

            prefix = EnergyMeter.get_node_phase(node)

            for key, (func, kwargs) in self.calculation_methods.items():
                if key in node.name:
                    func(node=node, prefix=prefix, **kwargs)
                    break

    def calculate_energy(self, prefix: str, energy_type: str, node: Node):
        if self.meter_options.read_separate_forward_reverse_energy:
            forward = self.meter_nodes.nodes[prefix + f"forward_{energy_type}_energy"].value
            reverse = self.meter_nodes.nodes[prefix + f"reverse_{energy_type}_energy"].value
            node.set_value((forward - reverse) if forward is not None and reverse is not None else None)

        elif not self.meter_options.read_energy_from_meter:
            power_node = self.meter_nodes.nodes[prefix + f"{energy_type}_power"]
            elapsed_time = power_node.elapsed_time / 3600.0  # convert seconds to hours
            node.set_value((power_node.value * elapsed_time) if power_node.value is not None and elapsed_time is not None else None)

    def calculate_power(self, prefix: str, power_type: str, node: Node):
        v = self.meter_nodes.nodes[prefix + "voltage"].value
        i = self.meter_nodes.nodes[prefix + "current"].value

        if power_type == "active":
            pf = self.meter_nodes.nodes[prefix + "power_factor"].value
            node.set_value((v * i * pf) if v is not None and i is not None and pf is not None else None)

        elif power_type == "reactive":
            pf = self.meter_nodes.nodes[prefix + "power_factor"].value
            node.set_value((v * i * math.sin(math.acos(pf))) if v is not None and i is not None and pf is not None else None)

        elif power_type == "apparent":
            node.set_value((v * i) if v is not None and i is not None else None)

    def calculate_pf(self, prefix: str, node: Node):
        p = self.meter_nodes.nodes[prefix + "active_power"].value
        q = self.meter_nodes.nodes[prefix + "reactive_power"].value
        valid = p is not None and q is not None
        if not valid:
            return
        node.set_value((math.cos(math.atan(q / p))) if p != 0 else 0.0)

    def calculate_pf_direction(self, prefix: str, node: Node):
        pf = self.meter_nodes.nodes[prefix + "power_factor"].value
        er = self.meter_nodes.nodes.get(prefix + "reactive_energy")
        if pf:
            if pf >= 0.99:
                node.set_value(PowerFactorDirection.UNITARY)
                if er:
                    er.reset_direction()
                return

        if self.meter_options.negative_reactive_power:
            q = self.meter_nodes.nodes[prefix + "reactive_power"]
            node.set_value(PowerFactorDirection.LAGGING if q.value > 0.0 else PowerFactorDirection.LEADING)

        elif self.meter_options.read_separate_forward_reverse_energy:
            er_direction_pos = er.positive_direction
            er_direction_neg = er.negative_direction

            if er_direction_pos:
                node.set_value(PowerFactorDirection.LAGGING)
            elif er_direction_neg:
                node.set_value(PowerFactorDirection.LEADING)
            else:
                node.set_value(PowerFactorDirection.UNKNOWN)

        else:
            node.set_value(PowerFactorDirection.UNKNOWN)

    async def publish_nodes(self):
        topic = f"{self.name}_{self.id}_nodes"
        payload: Dict[str, Any] = {}
        for node in self.meter_nodes.nodes.values():
            if node.publish and node.value is not None:
                payload[node.name] = node.get_publish_format()
        await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))

    def get_device_state(self) -> Dict[str, Any]:
        output: Dict[str, Any] = {}
        output["id"] = self.id
        output["name"] = self.name
        output["protocol"] = self.protocol
        output["connected"] = self.connected
        output["options"] = self.meter_options.get_meter_options()
        output["type"] = self.meter_type
        return output
