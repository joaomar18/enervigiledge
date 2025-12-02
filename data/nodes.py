###########EXERTNAL IMPORTS############

from typing import Set

#######################################

#############LOCAL IMPORTS#############

from controller.node.node import Node
from model.controller.general import Protocol
from model.controller.device import EnergyMeterRecord, EnergyMeterType, EnergyMeterOptions
from model.controller.node import NodeRecord, NodeConfig, NodeAttributes, NodePhase, NodeType, CounterMode
from model.controller.protocol.no_protocol import NoProtocolNodeOptions, NoProtocolType, NONE_TO_INTERAL_TYPE_MAP
from model.controller.protocol.modbus_rtu import ModbusRTUNodeOptions, ModbusRTUNodeType, ModbusRTUNodeMode, MODBUS_RTU_TO_INTERAL_TYPE_MAP
from model.controller.protocol.opcua import OPCUANodeOptions, OPCUANodeType
from protocol.modbus_rtu.rtu_device import ModbusRTUOptions, ModbusRTUNode
from protocol.opcua.opcua_device import OPCUAOptions, OPCUANode

#######################################


def get_orno_we_516_db() -> EnergyMeterRecord:
    meter_options = EnergyMeterOptions().get_meter_options()
    communication_options = ModbusRTUOptions(slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1, retries=0).get_communication_options()

    def cfg(name: str, unit: str, phase: NodePhase, logging: bool = False, logging_period: int = 15, **extra) -> NodeConfig:
        protocol = Protocol.MODBUS_RTU if not extra.get("calculated", None) else Protocol.NONE
        return NodeConfig(
            name=name, unit=unit, type=NodeType.FLOAT, protocol=protocol, logging=logging, logging_period=logging_period, attributes=NodeAttributes(phase=phase), **extra
        )

    nodes: Set[Node] = set()
    
    # L1
    nodes.add(ModbusRTUNode(configuration=cfg("l1_voltage", "V", phase=NodePhase.L1, logging=True, logging_period=1), protocol_options=ModbusRTUNodeOptions(first_register=0x000E, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_current", "A", phase=NodePhase.L1), protocol_options=ModbusRTUNodeOptions(first_register=0x0016, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_active_power", "kW", phase=NodePhase.L1), protocol_options=ModbusRTUNodeOptions(first_register=0x001E, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reactive_power", "kVAr", phase=NodePhase.L1), protocol_options=ModbusRTUNodeOptions(first_register=0x0026, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(ModbusRTUNode(configuration=cfg("l1_forward_active_energy", "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x010A, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reverse_active_energy", "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0112, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_forward_reactive_energy", "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0122, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l1_reverse_reactive_energy", "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x012A, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(Node(configuration=cfg("l1_active_energy", "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l1_reactive_energy", "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))

    nodes.add(Node(configuration=cfg("l1_apparent_power", "kVA", phase=NodePhase.L1, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l1_power_factor", "", phase=NodePhase.L1, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))

    # L2
    nodes.add(ModbusRTUNode(configuration=cfg("l2_voltage", "V", phase=NodePhase.L2, logging=True, logging_period=1), protocol_options=ModbusRTUNodeOptions(first_register=0x0010, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_current", "A", phase=NodePhase.L2), protocol_options=ModbusRTUNodeOptions(first_register=0x0018, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_active_power", "kW", phase=NodePhase.L2), protocol_options=ModbusRTUNodeOptions(first_register=0x0020, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reactive_power", "kVAr", phase=NodePhase.L2), protocol_options=ModbusRTUNodeOptions(first_register=0x0028, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(ModbusRTUNode(configuration=cfg("l2_forward_active_energy", "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x010C, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reverse_active_energy", "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0114, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_forward_reactive_energy", "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0124, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l2_reverse_reactive_energy", "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x012C, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(Node(configuration=cfg("l2_active_energy", "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l2_reactive_energy", "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))

    nodes.add(Node(configuration=cfg("l2_apparent_power", "kVA", phase=NodePhase.L2, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l2_power_factor", "", phase=NodePhase.L2, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    
    # L3
    nodes.add(ModbusRTUNode(configuration=cfg("l3_voltage", "V", phase=NodePhase.L3, logging=True, logging_period=1), protocol_options=ModbusRTUNodeOptions(first_register=0x0012, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_current", "A", phase=NodePhase.L3), protocol_options=ModbusRTUNodeOptions(first_register=0x001A, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_active_power", "kW", phase=NodePhase.L3), protocol_options=ModbusRTUNodeOptions(first_register=0x0022, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reactive_power", "kVAr", phase=NodePhase.L3), protocol_options=ModbusRTUNodeOptions(first_register=0x002A, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(ModbusRTUNode(configuration=cfg("l3_forward_active_energy", "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x010E, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reverse_active_energy", "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0116, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_forward_reactive_energy", "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x0126, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    nodes.add(ModbusRTUNode(configuration=cfg("l3_reverse_reactive_energy", "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DIRECT), protocol_options=ModbusRTUNodeOptions(first_register=0x012E, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))
    
    nodes.add(Node(configuration=cfg("l3_active_energy", "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l3_reactive_energy", "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))

    nodes.add(Node(configuration=cfg("l3_apparent_power", "kVA", phase=NodePhase.L3, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("l3_power_factor", "", phase=NodePhase.L3, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    
    # Total
    nodes.add(Node(configuration=cfg("total_active_energy", "kWh", phase=NodePhase.TOTAL, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("total_reactive_energy", "kVArh", phase=NodePhase.TOTAL, is_counter=True, counter_mode=CounterMode.CUMULATIVE, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    
    nodes.add(Node(configuration=cfg("total_power_factor", "", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("total_active_power", "kW", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("total_reactive_power", "kVAr", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("total_apparent_power", "kVA", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    
    # General    
    
    nodes.add(ModbusRTUNode(configuration=cfg("frequency", "Hz", phase=NodePhase.GENERAL, logging=True, logging_period=1), protocol_options=ModbusRTUNodeOptions(first_register=0x0014, coil=None, type=ModbusRTUNodeType.FLOAT_32, endian_mode=ModbusRTUNodeMode.BIG_ENDIAN)))

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

    def cfg(name: str, unit: str, phase: NodePhase, logging: bool = False, logging_period: int = 15, **extra) -> NodeConfig:
        protocol = Protocol.OPC_UA if not extra.get("calculated", None) else Protocol.NONE
        return NodeConfig(
            name=name, protocol=protocol, type=NodeType.FLOAT, unit=unit, logging=logging, logging_period=logging_period, attributes=NodeAttributes(phase=phase), **extra
        )

    nodes: Set[Node] = set()

    # L1
    nodes.add(OPCUANode(configuration=cfg("l1_voltage", "V", phase=NodePhase.L1, logging=True, logging_period=15), protocol_options=OPCUANodeOptions(node_id="ns=4;i=7", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_current", "mA", phase=NodePhase.L1), protocol_options=OPCUANodeOptions(node_id="ns=4;i=6", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_active_power", "W", phase=NodePhase.L1), protocol_options=OPCUANodeOptions(node_id="ns=4;i=8", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_reactive_power", "VAr", phase=NodePhase.L1), protocol_options=OPCUANodeOptions(node_id="ns=4;i=9", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_apparent_power", "VA", phase=NodePhase.L1), protocol_options=OPCUANodeOptions(node_id="ns=4;i=10", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_power_factor", "", phase=NodePhase.L1), protocol_options=OPCUANodeOptions(node_id="ns=4;i=11", type=OPCUANodeType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l1_active_energy", "kWh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l1_reactive_energy", "kVArh", phase=NodePhase.L1, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))

    # L2
    nodes.add(OPCUANode(configuration=cfg("l2_voltage", "V", phase=NodePhase.L2, logging=True, logging_period=15), protocol_options=OPCUANodeOptions(node_id="ns=4;i=14", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_current", "mA", phase=NodePhase.L2), protocol_options=OPCUANodeOptions(node_id="ns=4;i=13", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_active_power", "W", phase=NodePhase.L2), protocol_options=OPCUANodeOptions(node_id="ns=4;i=15", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_reactive_power", "VAr", phase=NodePhase.L2), protocol_options=OPCUANodeOptions(node_id="ns=4;i=16", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_apparent_power", "VA", phase=NodePhase.L2), protocol_options=OPCUANodeOptions(node_id="ns=4;i=17", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_power_factor", "", phase=NodePhase.L2), protocol_options=OPCUANodeOptions(node_id="ns=4;i=18", type=OPCUANodeType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l2_active_energy", "kWh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l2_reactive_energy", "kVArh", phase=NodePhase.L2, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    
    # L3
    nodes.add(OPCUANode(configuration=cfg("l3_voltage", "V", phase=NodePhase.L3, logging=True, logging_period=15), protocol_options=OPCUANodeOptions(node_id="ns=4;i=21", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_current", "mA", phase=NodePhase.L3), protocol_options=OPCUANodeOptions(node_id="ns=4;i=20", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_active_power", "W", phase=NodePhase.L3), protocol_options=OPCUANodeOptions(node_id="ns=4;i=22", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_reactive_power", "VAr", phase=NodePhase.L3), protocol_options=OPCUANodeOptions(node_id="ns=4;i=23", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_apparent_power", "VA", phase=NodePhase.L3), protocol_options=OPCUANodeOptions(node_id="ns=4;i=24", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_power_factor", "", phase=NodePhase.L3), protocol_options=OPCUANodeOptions(node_id="ns=4;i=25", type=OPCUANodeType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l3_active_energy", "kWh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("l3_reactive_energy", "kVArh", phase=NodePhase.L3, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    
    # Total
    nodes.add(OPCUANode(configuration=cfg("total_power_factor", "", phase=NodePhase.TOTAL), protocol_options=OPCUANodeOptions(node_id="ns=4;i=29", type=OPCUANodeType.FLOAT)))    
    nodes.add(Node(configuration=cfg("total_active_energy", "kWh", phase=NodePhase.TOTAL, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))    
    nodes.add(Node(configuration=cfg("total_reactive_energy", "kVArh", phase=NodePhase.TOTAL, is_counter=True, counter_mode=CounterMode.DELTA, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("total_active_power", "kW", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("total_reactive_power", "kVAr", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    nodes.add(Node(configuration=cfg("total_apparent_power", "kVA", phase=NodePhase.TOTAL, calculated=True), protocol_options=NoProtocolNodeOptions(type=NoProtocolType.FLOAT)))
    
    # General
    nodes.add(OPCUANode(configuration=cfg("frequency", "Hz", phase=NodePhase.GENERAL), protocol_options=OPCUANodeOptions(node_id="ns=4;i=33", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l1_l2_voltage", "V", phase=NodePhase.GENERAL), protocol_options=OPCUANodeOptions(node_id="ns=4;i=26", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l2_l3_voltage", "V", phase=NodePhase.GENERAL), protocol_options=OPCUANodeOptions(node_id="ns=4;i=27", type=OPCUANodeType.FLOAT)))    
    nodes.add(OPCUANode(configuration=cfg("l3_l1_voltage", "V", phase=NodePhase.GENERAL), protocol_options=OPCUANodeOptions(node_id="ns=4;i=28", type=OPCUANodeType.FLOAT)))    

    node_records: set[NodeRecord] = {node.get_node_record() for node in nodes}

    return EnergyMeterRecord(
        name="SM1238 S7-1200 Meter",
        protocol=Protocol.OPC_UA,
        type=EnergyMeterType.THREE_PHASE,
        options=meter_options,
        communication_options=communication_options,
        nodes=node_records,
    )
