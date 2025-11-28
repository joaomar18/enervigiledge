###########EXERTNAL IMPORTS############

from typing import Set

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from model.controller.general import Protocol
from model.controller.device import EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions
from model.controller.node import NodeRecord, NodeConfig, NodeAttributes, NodePhase, NodeType, CounterMode
from protocol.modbus_rtu.rtu_device import ModbusRTUOptions, ModbusRTUNode
from protocol.opcua.opcua_device import OPCUAOptions, OPCUANode

#######################################


def get_orno_we_516_db() -> EnergyMeterRecord:
    meter_options = EnergyMeterOptions().get_meter_options()

    communication_options = ModbusRTUOptions(
        slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1, retries=0
    ).get_communication_options()

    def cfg(name: str, type: NodeType, unit: str, *, phase: NodePhase, logging: bool = False, logging_period: int = 15, **extra) -> NodeConfig:
        protocol = Protocol.MODBUS_RTU if not extra.get("calculated", None) else Protocol.NONE
        return NodeConfig(
            name=name, type=type, unit=unit, protocol=protocol, logging=logging, logging_period=logging_period, attributes=NodeAttributes(phase=phase), **extra
        )

    nodes: Set[Node] = set()

    # L1
    nodes.add(
        ModbusRTUNode(configuration=cfg("l1_voltage", NodeType.FLOAT, "V", phase=NodePhase.L1, logging=True, logging_period=1), register=0x000E)
    )
    nodes.add(ModbusRTUNode(configuration=cfg("l1_current", NodeType.FLOAT, "A", phase=NodePhase.L1), register=0x0016))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_active_power", NodeType.FLOAT, "kW", phase=NodePhase.L1), register=0x001E))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reactive_power", NodeType.FLOAT, "kVAr", phase=NodePhase.L1), register=0x0026))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_forward_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x010A))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reverse_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0112))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_forward_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0122))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reverse_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x012A))
    nodes.add(
        Node(
            cfg(
                "l1_active_energy",
                NodeType.FLOAT,
                "kWh",
                phase=NodePhase.L1,
                is_counter=True,
                counter_mode=CounterMode.CUMULATIVE,
                calculated=True,
                logging=False,
                logging_period=5,
            )
        )
    )
    nodes.add(
        Node(
            cfg(
                "l1_reactive_energy",
                NodeType.FLOAT,
                "kVArh",
                phase=NodePhase.L1,
                is_counter=True,
                counter_mode=CounterMode.CUMULATIVE,
                calculated=True,
                logging=False,
                logging_period=5,
            )
        )
    )
    nodes.add(Node(cfg("l1_apparent_power", NodeType.FLOAT, "kVA", phase=NodePhase.L1, calculated=True)))
    nodes.add(Node(cfg("l1_power_factor", NodeType.FLOAT, "", phase=NodePhase.L1, calculated=True)))

    # L2
    nodes.add(
        ModbusRTUNode(configuration=cfg("l2_voltage", NodeType.FLOAT, "V", phase=NodePhase.L2, logging=True, logging_period=1), register=0x0010)
    )
    nodes.add(ModbusRTUNode(configuration=cfg("l2_current", NodeType.FLOAT, "A", phase=NodePhase.L2), register=0x0018))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_active_power", NodeType.FLOAT, "kW", phase=NodePhase.L2), register=0x0020))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reactive_power", NodeType.FLOAT, "kVAr", phase=NodePhase.L2), register=0x0028))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_forward_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x010C))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reverse_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0114))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_forward_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0124))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reverse_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x012C))
    nodes.add(
        Node(cfg("l2_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True)))
    nodes.add(
        Node(
            cfg("l2_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True)
        )
    )
    nodes.add(Node(cfg("l2_apparent_power", NodeType.FLOAT, "kVA", phase=NodePhase.L2, calculated=True)))
    nodes.add(Node(cfg("l2_power_factor", NodeType.FLOAT, "", phase=NodePhase.L2, calculated=True)))

    # L3
    nodes.add(
        ModbusRTUNode(configuration=cfg("l3_voltage", NodeType.FLOAT, "V", phase=NodePhase.L3, logging=True, logging_period=1), register=0x0012)
    )
    nodes.add(ModbusRTUNode(configuration=cfg("l3_current", NodeType.FLOAT, "A", phase=NodePhase.L3), register=0x001A))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_active_power", NodeType.FLOAT, "kW", phase=NodePhase.L3), register=0x0022))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reactive_power", NodeType.FLOAT, "kVAr", phase=NodePhase.L3), register=0x002A))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_forward_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x010E))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reverse_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0116))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_forward_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x0126))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reverse_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), register=0x012E))
    nodes.add(
        ModbusRTUNode(configuration=cfg("frequency", NodeType.FLOAT, "Hz", phase=NodePhase.L3, logging=True, logging_period=1), register=0x0014)
    )
    nodes.add(
        Node(cfg("l3_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True))
    )
    nodes.add(
        Node(
            cfg("l3_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True)
        )
    )
    nodes.add(Node(cfg("l3_apparent_power", NodeType.FLOAT, "kVA", phase=NodePhase.L3, calculated=True)))
    nodes.add(Node(cfg("l3_power_factor", NodeType.FLOAT, "", phase=NodePhase.L3, calculated=True)))

    # Totals
    nodes.add(
        Node(
            cfg(
                "total_active_energy",
                NodeType.FLOAT,
                "kWh",
                phase=NodePhase.TOTAL,
                is_counter=True,
                counter_mode=CounterMode.CUMULATIVE,
                calculated=True,
                logging=False,
                logging_period=5,
            )
        )
    )
    nodes.add(
        Node(
            cfg(
                "total_reactive_energy",
                NodeType.FLOAT,
                "kVArh",
                phase=NodePhase.TOTAL,
                is_counter=True,
                counter_mode=CounterMode.CUMULATIVE,
                calculated=True,
                logging=False,
                logging_period=5,
            )
        )
    )
    nodes.add(Node(cfg("total_power_factor", NodeType.FLOAT, "", phase=NodePhase.TOTAL, calculated=True, logging=True, logging_period=1)))
    nodes.add(Node(cfg("total_active_power", NodeType.FLOAT, "kW", phase=NodePhase.TOTAL, calculated=True, logging=True, logging_period=1)))
    nodes.add(Node(cfg("total_reactive_power", NodeType.FLOAT, "kVAr", phase=NodePhase.TOTAL, calculated=True, logging=True, logging_period=1)))
    nodes.add(Node(cfg("total_apparent_power", NodeType.FLOAT, "kVA", phase=NodePhase.TOTAL, calculated=True, logging=True, logging_period=1)))

    node_records: set[NodeRecord] = {node.get_node_record() for node in nodes}

    return EnergyMeterRecord(
        name="OR-WE-516 Energy Meter",
        protocol=Protocol.MODBUS_RTU,
        type=EnergyMeterType.THREE_PHASE,
        options=meter_options,
        communication_options=communication_options,
        nodes=node_records,
    )


def get_sm1238_db() -> EnergyMeterRecord:
    meter_options = EnergyMeterOptions().get_meter_options()

    communication_options = OPCUAOptions(url="opc.tcp://192.168.10.10:4840").get_communication_options()

    def cfg(name: str, type: NodeType, unit: str, phase: NodePhase, logging: bool = False, logging_period: int = 15, **extra) -> NodeConfig:
        protocol = Protocol.OPC_UA if not extra.get("calculated", None) else Protocol.NONE
        return NodeConfig(
            name=name, type=type, protocol=protocol, unit=unit, logging=logging, logging_period=logging_period, attributes=NodeAttributes(phase=phase), **extra
        )

    nodes: Set[Node] = set()

    # L1
    nodes.add(
        OPCUANode(configuration=cfg("l1_voltage", NodeType.FLOAT, "V", phase=NodePhase.L1, logging=True, logging_period=15), node_id="ns=4;i=7")
    )
    nodes.add(OPCUANode(configuration=cfg("l1_current", NodeType.FLOAT, "mA", phase=NodePhase.L1), node_id="ns=4;i=6"))
    nodes.add(OPCUANode(configuration=cfg("l1_active_power", NodeType.FLOAT, "W", phase=NodePhase.L1), node_id="ns=4;i=8"))
    nodes.add(OPCUANode(configuration=cfg("l1_reactive_power", NodeType.FLOAT, "VAr", phase=NodePhase.L1), node_id="ns=4;i=9"))
    nodes.add(OPCUANode(configuration=cfg("l1_apparent_power", NodeType.FLOAT, "VA", phase=NodePhase.L1), node_id="ns=4;i=10"))
    nodes.add(OPCUANode(configuration=cfg("l1_power_factor", NodeType.FLOAT, "", phase=NodePhase.L1), node_id="ns=4;i=11"))
    nodes.add(
        Node(
            cfg(
                "l1_active_energy",
                NodeType.FLOAT,
                "kWh",
                phase=NodePhase.L1,
                is_counter=True,
                counter_mode=CounterMode.DELTA,
                calculated=True,
                logging=True,
                logging_period=5,
            )
        )
    )
    nodes.add(
        Node(
            cfg(
                "l1_reactive_energy",
                NodeType.FLOAT,
                "kVArh",
                phase=NodePhase.L1,
                is_counter=True,
                counter_mode=CounterMode.DELTA,
                calculated=True,
                logging=True,
                logging_period=5,
            )
        )
    )

    # L2
    nodes.add(
        OPCUANode(configuration=cfg("l2_voltage", NodeType.FLOAT, "V", phase=NodePhase.L2, logging=True, logging_period=15), node_id="ns=4;i=14")
    )
    nodes.add(OPCUANode(configuration=cfg("l2_current", NodeType.FLOAT, "mA", phase=NodePhase.L2), node_id="ns=4;i=13"))
    nodes.add(OPCUANode(configuration=cfg("l2_active_power", NodeType.FLOAT, "W", phase=NodePhase.L2), node_id="ns=4;i=15"))
    nodes.add(OPCUANode(configuration=cfg("l2_reactive_power", NodeType.FLOAT, "VAr", phase=NodePhase.L2), node_id="ns=4;i=16"))
    nodes.add(OPCUANode(configuration=cfg("l2_apparent_power", NodeType.FLOAT, "VA", phase=NodePhase.L2), node_id="ns=4;i=17"))
    nodes.add(OPCUANode(configuration=cfg("l2_power_factor", NodeType.FLOAT, "", phase=NodePhase.L2), node_id="ns=4;i=18"))
    nodes.add(
        Node(cfg("l2_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True))
    )
    nodes.add(
        Node(
            cfg("l2_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True)
        )
    )

    # L3
    nodes.add(
        OPCUANode(configuration=cfg("l3_voltage", NodeType.FLOAT, "V", phase=NodePhase.L3, logging=True, logging_period=15), node_id="ns=4;i=21")
    )
    nodes.add(OPCUANode(configuration=cfg("l3_current", NodeType.FLOAT, "mA", phase=NodePhase.L3), node_id="ns=4;i=20"))
    nodes.add(OPCUANode(configuration=cfg("l3_active_power", NodeType.FLOAT, "W", phase=NodePhase.L3), node_id="ns=4;i=22"))
    nodes.add(OPCUANode(configuration=cfg("l3_reactive_power", NodeType.FLOAT, "VAr", phase=NodePhase.L3), node_id="ns=4;i=23"))
    nodes.add(OPCUANode(configuration=cfg("l3_apparent_power", NodeType.FLOAT, "VA", phase=NodePhase.L3), node_id="ns=4;i=24"))
    nodes.add(OPCUANode(configuration=cfg("l3_power_factor", NodeType.FLOAT, "", phase=NodePhase.L3), node_id="ns=4;i=25"))
    nodes.add(
        Node(cfg("l3_active_energy", NodeType.FLOAT, "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True))
    )
    nodes.add(
        Node(
            cfg("l3_reactive_energy", NodeType.FLOAT, "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True)
        )
    )

    # General & Totals
    nodes.add(OPCUANode(configuration=cfg("frequency", NodeType.FLOAT, "Hz", phase=NodePhase.GENERAL), node_id="ns=4;i=33"))
    nodes.add(OPCUANode(configuration=cfg("l1_l2_voltage", NodeType.FLOAT, "V", phase=NodePhase.GENERAL), node_id="ns=4;i=26"))
    nodes.add(OPCUANode(configuration=cfg("l2_l3_voltage", NodeType.FLOAT, "V", phase=NodePhase.GENERAL), node_id="ns=4;i=27"))
    nodes.add(OPCUANode(configuration=cfg("l3_l1_voltage", NodeType.FLOAT, "V", phase=NodePhase.GENERAL), node_id="ns=4;i=28"))
    nodes.add(OPCUANode(configuration=cfg("total_power_factor", NodeType.FLOAT, "", phase=NodePhase.TOTAL), node_id="ns=4;i=29"))

    nodes.add(
        Node(
            cfg(
                "total_active_energy",
                NodeType.FLOAT,
                "kWh",
                phase=NodePhase.TOTAL,
                is_counter=True,
                counter_mode=CounterMode.DELTA,
                calculated=True,
                logging=True,
                logging_period=5,
            )
        )
    )
    nodes.add(
        Node(
            cfg(
                "total_reactive_energy",
                NodeType.FLOAT,
                "kVArh",
                phase=NodePhase.TOTAL,
                is_counter=True,
                counter_mode=CounterMode.DELTA,
                calculated=True,
                logging=True,
                logging_period=5,
            )
        )
    )
    nodes.add(Node(cfg("total_active_power", NodeType.FLOAT, "kW", phase=NodePhase.TOTAL, calculated=True, logging=True)))
    nodes.add(Node(cfg("total_reactive_power", NodeType.FLOAT, "kVAr", phase=NodePhase.TOTAL, calculated=True, logging=True)))
    nodes.add(Node(cfg("total_apparent_power", NodeType.FLOAT, "kVA", phase=NodePhase.TOTAL, calculated=True, logging=True)))

    node_records: set[NodeRecord] = {node.get_node_record() for node in nodes}

    return EnergyMeterRecord(
        name="SM1238 S7-1200 Meter",
        protocol=Protocol.OPC_UA,
        type=EnergyMeterType.THREE_PHASE,
        options=meter_options,
        communication_options=communication_options,
        nodes=node_records,
    )
