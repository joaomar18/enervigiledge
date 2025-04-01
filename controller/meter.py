###########EXERTNAL IMPORTS############

import asyncio
import math

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device, Node, NodeType
from mqtt.client import MQTTMessage
import util.debug as debug

#######################################


class UnitError(Exception):
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
    def __init__(
        self, read_energy_from_meter: bool, read_separate_forward_reverse_energy: bool, negative_reactive_power: bool, frequency_reading: bool
    ):

        self.read_energy_from_meter = read_energy_from_meter
        self.read_separate_forward_reverse_energy = read_separate_forward_reverse_energy
        self.negative_reactive_power = negative_reactive_power
        self.frequency_reading = frequency_reading

    def get_meter_options(self) -> dict[str]:
        state_dict: dict[str] = dict()
        state_dict["read_energy_from_meter"] = self.read_energy_from_meter
        state_dict["read_separate_forward_reverse_energy"] = self.read_separate_forward_reverse_energy
        state_dict["negative_reactive_power"] = self.negative_reactive_power
        state_dict["frequency_reading"] = self.frequency_reading
        return state_dict


class EnergyMeterNodes:
    VALID_UNITS = {
        "voltage": {"V"},
        "current": {"mA", "A"},
        "active_power": {"W", "kW"},
        "reactive_power": {"VAr", "kVAr"},
        "apparent_power": {"VA", "kVA"},
        "power_factor": {""},
        "frequency": {"Hz"},
        "active_energy": {"Wh", "kWh"},
        "reactive_energy": {"VArh", "kVArh"},
    }

    def __init__(self, meter_type: EnergyMeterType, meter_options: EnergyMeterOptions, base_nodes: set[Node]):
        self.meter_type = meter_type
        self.meter_options = meter_options
        self.nodes: dict[str, Node] = {node.name: node for node in base_nodes}

    def _add_node(self, name: str, type: NodeType, unit: str, **kwargs):
        base_name = name.split("_")[-1] if "_" in name else name
        if base_name in self.VALID_UNITS and unit not in self.VALID_UNITS[base_name]:
            raise UnitError(f"Invalid unit '{unit}' for node '{name}'. Expected one of {self.VALID_UNITS[base_name]}")
        if name not in self.nodes:
            self.nodes[name] = Node(name=name, type=type, unit=unit, **kwargs)
        else:
            node = self.nodes[name]
            for key, value in kwargs.items():
                if hasattr(node, key):
                    setattr(node, key, value)

    def _require_units(self, units: dict[str, str], required_keys: list[str]):
        for key in required_keys:
            if key not in units:
                raise NodeMissingError(f"Missing unit for expected node '{key}'")

    def _add_energy_nodes(self, units: dict[str, str], prefix: str = ""):
        if self.meter_options.read_energy_from_meter:
            if self.meter_options.read_separate_forward_reverse_energy:
                keys = [
                    f"{prefix}{k}" for k in ["forward_active_energy", "reverse_active_energy", "forward_reactive_energy", "reverse_reactive_energy"]
                ]
                self._require_units(units, keys)
                for k in keys:
                    self._add_node(k, NodeType.FLOAT, units[k], publish=False)
                if "kWh" in (units[f"{prefix}forward_active_energy"], units[f"{prefix}reverse_active_energy"]):
                    self._add_node(f"{prefix}active_energy", NodeType.FLOAT, "kWh", calculated=True)
                elif all(units[f"{prefix}{et}"] == "Wh" for et in ["forward_active_energy", "reverse_active_energy"]):
                    self._add_node(f"{prefix}active_energy", NodeType.FLOAT, "Wh", calculated=True)
                if "kVArh" in (units[f"{prefix}forward_reactive_energy"], units[f"{prefix}reverse_reactive_energy"]):
                    self._add_node(f"{prefix}reactive_energy", NodeType.FLOAT, "kVArh", calculated=True)
                elif all(units[f"{prefix}{et}"] == "VArh" for et in ["forward_reactive_energy", "reverse_reactive_energy"]):
                    self._add_node(f"{prefix}reactive_energy", NodeType.FLOAT, "VArh", calculated=True)
            else:
                keys = [f"{prefix}active_energy", f"{prefix}reactive_energy"]
                self._require_units(units, keys)
                for k in keys:
                    self._add_node(k, NodeType.FLOAT, units[k], incremental_node=True)
        else:
            self._require_units(units, [f"{prefix}active_power", f"{prefix}reactive_power"])
            self._add_node(
                f"{prefix}active_energy",
                NodeType.FLOAT,
                "kWh" if units[f"{prefix}active_power"] == "kW" else "Wh",
                calculated=True,
                incremental_node=True,
                positive_incremental=True,
            )
            self._add_node(
                f"{prefix}reactive_energy",
                NodeType.FLOAT,
                "kVArh" if units[f"{prefix}reactive_power"] == "kVAr" else "VArh",
                calculated=True,
                incremental_node=True,
                positive_incremental=True,
            )

    def _add_power_factor_nodes(self, units: dict[str, str], prefix: str = ""):
        self._require_units(units, [f"{prefix}active_power"])
        apparent_unit = "kVA" if units[f"{prefix}active_power"] == "kW" else "VA"
        self._add_node(f"{prefix}apparent_power", NodeType.FLOAT, apparent_unit, calculated=True)
        self._add_node(f"{prefix}power_factor", NodeType.FLOAT, "", calculated=True)
        self._add_node(f"{prefix}power_factor_direction", NodeType.STRING, "", calculated=True)

    def init_nodes(self, units: dict[str, str]) -> bool:
        try:
            if self.meter_type == EnergyMeterType.SINGLE_PHASE:
                self._require_units(units, ["voltage", "current", "active_power", "reactive_power"])
                for name in ["voltage", "current", "active_power", "reactive_power"]:
                    self._add_node(name, NodeType.FLOAT, units[name])
                self._add_energy_nodes(units)
                self._add_power_factor_nodes(units)
                if self.meter_options.frequency_reading:
                    self._require_units(units, ["frequency"])
                    self._add_node("frequency", NodeType.FLOAT, units["frequency"])
            elif self.meter_type == EnergyMeterType.THREE_PHASE:
                for phase in ["l1", "l2", "l3"]:
                    self._require_units(units, [f"{phase}_{param}" for param in ["voltage", "current", "active_power", "reactive_power"]])
                    for param in ["voltage", "current", "active_power", "reactive_power"]:
                        self._add_node(f"{phase}_{param}", NodeType.FLOAT, units[f"{phase}_{param}"])
                    self._add_energy_nodes(units, prefix=f"{phase}_")
                    self._add_power_factor_nodes(units, prefix=f"{phase}_")
                if self.meter_options.frequency_reading:
                    self._require_units(units, ["frequency"])
                    self._add_node("frequency", NodeType.FLOAT, units["frequency"])
            return True
        except Exception as e:
            debug.logger.exception(f"Error initializing meter nodes: {e}")
            return False

    def set_energy_nodes_incremental(self):
        for node in self.nodes.values():
            if "energy" in node.name:
                node.set_incremental_node(True)


class EnergyMeter(Device):
    def __init__(
        self,
        id: int,
        name: str,
        protocol: int,
        publish_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        meter_nodes: set[Node],
    ):
        super().__init__(id=id, name=name, protocol=protocol, publish_queue=publish_queue)
        self.meter_type = meter_type
        self.meter_options = meter_options
        units = {node.name: node.unit for node in meter_nodes}
        self.meter_nodes = EnergyMeterNodes(meter_type=self.meter_type, meter_options=self.meter_options, base_nodes=meter_nodes)
        self.meter_nodes.set_energy_nodes_incremental()
        if not self.meter_nodes.init_nodes(units):
            raise Exception(f"Failed to initialize EnergyMeter '{name}' with id {id} due to invalid or missing node definitions.")

    async def process_nodes(self):

        for node in self.meter_nodes.nodes.values():
            if node.calculated:
                prefix = ""
                if node.name.startswith("l1_"):
                    prefix = "l1_"
                elif node.name.startswith("l2_"):
                    prefix = "l2_"
                elif node.name.startswith("l3_"):
                    prefix = "l3_"

                if "_reactive_energy" in node.name:
                    if self.meter_options.read_separate_forward_reverse_energy:
                        forward = self.meter_nodes.nodes[prefix + "forward_reactive_energy"].value
                        reverse = self.meter_nodes.nodes[prefix + "reverse_reactive_energy"].value
                        if forward is not None and reverse is not None:
                            node.set_value(forward - reverse)

                    elif not self.meter_options.read_energy_from_meter:
                        reactive_power = self.meter_nodes.nodes[prefix + "reactive_power"].value
                        elapsed_time = self.meter_nodes.nodes[prefix + "reactive_power"].elapsed_time / 3600.0  # convert seconds to hours
                        node.set_value(reactive_power * elapsed_time)

                elif "_active_energy" in node.name:
                    if self.meter_options.read_separate_forward_reverse_energy:
                        forward = self.meter_nodes.nodes[prefix + "forward_active_energy"].value
                        reverse = self.meter_nodes.nodes[prefix + "reverse_active_energy"].value
                        if forward is not None and reverse is not None:
                            node.set_value(forward - reverse)

                    elif not self.meter_options.read_energy_from_meter:
                        active_power = self.meter_nodes.nodes[prefix + "active_power"].value
                        elapsed_time = self.meter_nodes.nodes[prefix + "active_power"].elapsed_time / 3600.0  # convert seconds to hours
                        node.set_value(active_power * elapsed_time)

                elif "_apparent_power" in node.name:
                    p = self.meter_nodes.nodes[prefix + "active_power"].value
                    q = self.meter_nodes.nodes[prefix + "reactive_power"].value
                    if p is not None and q is not None:
                        node.set_value(math.sqrt(p**2 + q**2))

                elif "_power_factor_direction" in node.name:
                    pf = self.meter_nodes.nodes[prefix + "power_factor"].value
                    if self.meter_options.negative_reactive_power:
                        if pf == 1.0:
                            node.set_value(PowerFactorDirection.UNITARY)
                            self.meter_nodes.nodes[prefix + "reactive_energy"].reset_direction()
                        else:
                            node.set_value(PowerFactorDirection.LAGGING if q > 0 else PowerFactorDirection.LEADING)
                    elif self.meter_options.read_separate_forward_reverse_energy:
                        er_direction_pos = self.meter_nodes.nodes[prefix + "reactive_energy"].positive_direction
                        er_direction_neg = self.meter_nodes.nodes[prefix + "reactive_energy"].negative_direction

                        if pf == 1.0:
                            node.set_value(PowerFactorDirection.UNITARY)
                            self.meter_nodes.nodes[prefix + "reactive_energy"].reset_direction()
                        else:
                            if er_direction_pos:
                                node.set_value(PowerFactorDirection.LAGGING)
                            elif er_direction_neg:
                                node.set_value(PowerFactorDirection.LEADING)
                            else:
                                node.set_value(PowerFactorDirection.UNKNOWN)
                    else:
                        if pf == 1.0:
                            node.set_value(PowerFactorDirection.UNITARY)
                            self.meter_nodes.nodes[prefix + "reactive_energy"].reset_direction()
                        else:
                            node.set_value(PowerFactorDirection.UNKNOWN)

                elif "_power_factor" in node.name:
                    p = self.meter_nodes.nodes[prefix + "active_power"].value
                    q = self.meter_nodes.nodes[prefix + "reactive_power"].value
                    if p is not None and q is not None and p != 0:
                        node.set_value(math.cos(math.atan(q / p)))
                    elif p == 0:
                        node.set_value(0.0)

    async def publish_nodes(self):
        topic = f"{self.name}_{self.id}_nodes"
        payload: dict[str] = dict()
        for node in self.meter_nodes.nodes.values():
            if node.publish:
                payload[node.name] = node.get_publish_format()
        await self.publish_queue.put(MQTTMessage(qos=1, topic=topic, payload=payload))

    def get_device_state(self) -> dict[str]:
        state_dict: dict[str] = dict()
        state_dict["id"] = self.id
        state_dict["name"] = self.name
        state_dict["protocol"] = self.protocol
        state_dict["connected"] = self.connected
        state_dict["options"] = self.meter_options.get_meter_options()
        state_dict["type"] = self.meter_type
        return state_dict
