###########EXERTNAL IMPORTS############

import asyncio
from typing import Dict, Set, Any

#######################################

#############LOCAL IMPORTS#############

from db.db import NodeRecord, EnergyMeterRecord
from controller.node import Node, NodeType
from controller.types import Protocol
from controller.meter import EnergyMeterType, EnergyMeterOptions
from protocol.modbus_rtu.rtu_device import ModbusRTUOptions, ModbusRTUNode
from protocol.opcua.opcua_device import OPCUAOptions, OPCUANode

#######################################


def get_orno_we_516_db() -> EnergyMeterRecord:
    meter_options = EnergyMeterOptions(
        read_energy_from_meter=True, read_separate_forward_reverse_energy=True, negative_reactive_power=False, frequency_reading=True
    ).get_meter_options()
    connection_options = ModbusRTUOptions(
        slave_id=1, port="/dev/ttyAMA0", baudrate=9600, stopbits=1, parity="E", bytesize=8, read_period=5, timeout=1, retries=0
    ).get_connection_options()
    nodes: Set[Node] = set()
    nodes.add(ModbusRTUNode(name="l1_voltage", type=NodeType.FLOAT, register=0x000E, unit="V", logging=True, logging_period=1))
    nodes.add(ModbusRTUNode(name="l1_current", type=NodeType.FLOAT, register=0x0016, unit="A"))
    nodes.add(ModbusRTUNode(name="l1_active_power", type=NodeType.FLOAT, register=0x001E, unit="kW"))
    nodes.add(ModbusRTUNode(name="l1_reactive_power", type=NodeType.FLOAT, register=0x0026, unit="kVAr"))
    nodes.add(ModbusRTUNode(name="l1_forward_active_energy", type=NodeType.FLOAT, register=0x010A, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l1_reverse_active_energy", type=NodeType.FLOAT, register=0x0112, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l1_forward_reactive_energy", type=NodeType.FLOAT, register=0x0122, unit="kVArh"))
    nodes.add(ModbusRTUNode(name="l1_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012A, unit="kVArh"))
    nodes.add(
        Node(
            name="l1_active_energy",
            type=NodeType.FLOAT,
            unit="kWh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=False,
            logging_period=5,
        )
    )
    nodes.add(
        Node(
            name="l1_reactive_energy",
            type=NodeType.FLOAT,
            unit="kVArh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=False,
            logging_period=5,
        )
    )
    nodes.add(Node(name="l1_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True))
    nodes.add(Node(name="l1_power_factor", type=NodeType.FLOAT, unit="", calculated=True))
    nodes.add(Node(name="l1_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(ModbusRTUNode(name="l2_voltage", type=NodeType.FLOAT, register=0x0010, unit="V", logging=True, logging_period=1))
    nodes.add(ModbusRTUNode(name="l2_current", type=NodeType.FLOAT, register=0x0018, unit="A"))
    nodes.add(ModbusRTUNode(name="l2_active_power", type=NodeType.FLOAT, register=0x0020, unit="kW"))
    nodes.add(ModbusRTUNode(name="l2_reactive_power", type=NodeType.FLOAT, register=0x0028, unit="kVAr"))
    nodes.add(ModbusRTUNode(name="l2_forward_active_energy", type=NodeType.FLOAT, register=0x010C, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l2_reverse_active_energy", type=NodeType.FLOAT, register=0x0114, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l2_forward_reactive_energy", type=NodeType.FLOAT, register=0x0124, unit="kVArh"))
    nodes.add(ModbusRTUNode(name="l2_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012C, unit="kVArh"))
    nodes.add(Node(name="l2_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, calculate_increment=False, calculated=True))
    nodes.add(Node(name="l2_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, calculate_increment=False, calculated=True))
    nodes.add(Node(name="l2_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True))
    nodes.add(Node(name="l2_power_factor", type=NodeType.FLOAT, unit="", calculated=True))
    nodes.add(Node(name="l2_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(ModbusRTUNode(name="l3_voltage", type=NodeType.FLOAT, register=0x0012, unit="V", logging=True, logging_period=1))
    nodes.add(ModbusRTUNode(name="l3_current", type=NodeType.FLOAT, register=0x001A, unit="A"))
    nodes.add(ModbusRTUNode(name="l3_active_power", type=NodeType.FLOAT, register=0x0022, unit="kW"))
    nodes.add(ModbusRTUNode(name="l3_reactive_power", type=NodeType.FLOAT, register=0x002A, unit="kVAr"))
    nodes.add(ModbusRTUNode(name="l3_forward_active_energy", type=NodeType.FLOAT, register=0x010E, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l3_reverse_active_energy", type=NodeType.FLOAT, register=0x0116, unit="kWh"))
    nodes.add(ModbusRTUNode(name="l3_forward_reactive_energy", type=NodeType.FLOAT, register=0x0126, unit="kVArh"))
    nodes.add(ModbusRTUNode(name="l3_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012E, unit="kVArh"))
    nodes.add(ModbusRTUNode(name="frequency", type=NodeType.FLOAT, register=0x0014, unit="Hz", logging=True, logging_period=1))
    nodes.add(Node(name="l3_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, calculate_increment=False, calculated=True))
    nodes.add(Node(name="l3_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, calculate_increment=False, calculated=True))
    nodes.add(Node(name="l3_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True))
    nodes.add(Node(name="l3_power_factor", type=NodeType.FLOAT, unit="", calculated=True))
    nodes.add(Node(name="l3_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(
        Node(
            name="total_active_energy",
            type=NodeType.FLOAT,
            unit="kWh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=False,
            logging_period=5,
        )
    )

    nodes.add(
        Node(
            name="total_reactive_energy",
            type=NodeType.FLOAT,
            unit="kVArh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=False,
            logging_period=5,
        )
    )
    nodes.add(Node(name="total_power_factor", type=NodeType.FLOAT, unit="", calculated=True, logging=True, logging_period=1))
    nodes.add(Node(name="total_power_factor_direction", type=NodeType.STRING, unit="", calculated=True, logging=True, logging_period=1))
    nodes.add(Node(name="total_active_power", type=NodeType.FLOAT, unit="kW", calculated=True, logging=True, logging_period=1))
    nodes.add(Node(name="total_reactive_power", type=NodeType.FLOAT, unit="kVAr", calculated=True, logging=True, logging_period=1))
    nodes.add(Node(name="total_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True, logging=True, logging_period=1))

    node_records: set[NodeRecord] = set()
    for node in nodes:
        record = node.get_node_record()
        node_records.add(record)

    return EnergyMeterRecord(
        name="OR-WE-516 Energy Meter",
        protocol=Protocol.MODBUS_RTU,
        device_type=EnergyMeterType.THREE_PHASE,
        meter_options=meter_options,
        connection_options=connection_options,
        nodes=node_records,
    )


def get_sm1238_db() -> EnergyMeterRecord:
    meter_options = EnergyMeterOptions(
        read_energy_from_meter=False, read_separate_forward_reverse_energy=False, negative_reactive_power=True, frequency_reading=True
    ).get_meter_options()
    connection_options = OPCUAOptions(url="opc.tcp://192.168.10.10:4840").get_connection_options()
    nodes: Set[Node] = set()

    nodes.add(OPCUANode(name="l1_voltage", type=NodeType.FLOAT, node_id="ns=4;i=7", unit="V", logging=True, logging_period=15))
    nodes.add(OPCUANode(name="l1_current", type=NodeType.FLOAT, node_id="ns=4;i=6", unit="mA"))
    nodes.add(OPCUANode(name="l1_active_power", type=NodeType.FLOAT, node_id="ns=4;i=8", unit="W"))
    nodes.add(OPCUANode(name="l1_reactive_power", type=NodeType.FLOAT, node_id="ns=4;i=9", unit="VAr"))
    nodes.add(OPCUANode(name="l1_apparent_power", type=NodeType.FLOAT, node_id="ns=4;i=10", unit="VA"))
    nodes.add(OPCUANode(name="l1_power_factor", type=NodeType.FLOAT, node_id="ns=4;i=11", unit=""))
    nodes.add(
        Node(
            name="l1_active_energy",
            type=NodeType.FLOAT,
            unit="kWh",
            incremental_node=True,
            positive_incremental=True,
            calculated=True,
            logging=True,
            logging_period=5,
        )
    )
    nodes.add(
        Node(
            name="l1_reactive_energy",
            type=NodeType.FLOAT,
            unit="kVArh",
            incremental_node=True,
            positive_incremental=True,
            calculated=True,
            logging=True,
            logging_period=5,
        )
    )
    nodes.add(Node(name="l1_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(OPCUANode(name="l2_voltage", type=NodeType.FLOAT, node_id="ns=4;i=14", unit="V", logging=True, logging_period=15))
    nodes.add(OPCUANode(name="l2_current", type=NodeType.FLOAT, node_id="ns=4;i=13", unit="mA"))
    nodes.add(OPCUANode(name="l2_active_power", type=NodeType.FLOAT, node_id="ns=4;i=15", unit="W"))
    nodes.add(OPCUANode(name="l2_reactive_power", type=NodeType.FLOAT, node_id="ns=4;i=16", unit="VAr"))
    nodes.add(OPCUANode(name="l2_apparent_power", type=NodeType.FLOAT, node_id="ns=4;i=17", unit="VA"))
    nodes.add(OPCUANode(name="l2_power_factor", type=NodeType.FLOAT, node_id="ns=4;i=18", unit=""))
    nodes.add(Node(name="l2_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, positive_incremental=True, calculated=True))
    nodes.add(Node(name="l2_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, positive_incremental=True, calculated=True))
    nodes.add(Node(name="l2_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(OPCUANode(name="l3_voltage", type=NodeType.FLOAT, node_id="ns=4;i=21", unit="V", logging=True, logging_period=15))
    nodes.add(OPCUANode(name="l3_current", type=NodeType.FLOAT, node_id="ns=4;i=20", unit="mA"))
    nodes.add(OPCUANode(name="l3_active_power", type=NodeType.FLOAT, node_id="ns=4;i=22", unit="W"))
    nodes.add(OPCUANode(name="l3_reactive_power", type=NodeType.FLOAT, node_id="ns=4;i=23", unit="VAr"))
    nodes.add(OPCUANode(name="l3_apparent_power", type=NodeType.FLOAT, node_id="ns=4;i=24", unit="VA"))
    nodes.add(OPCUANode(name="l3_power_factor", type=NodeType.FLOAT, node_id="ns=4;i=25", unit=""))
    nodes.add(Node(name="l3_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, positive_incremental=True, calculated=True))
    nodes.add(Node(name="l3_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, positive_incremental=True, calculated=True))
    nodes.add(Node(name="l3_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))

    nodes.add(OPCUANode(name="frequency", type=NodeType.FLOAT, node_id="ns=4;i=33", unit="Hz"))
    nodes.add(OPCUANode(name="l1_l2_voltage", type=NodeType.FLOAT, node_id="ns=4;i=26", unit="V"))
    nodes.add(OPCUANode(name="l2_l3_voltage", type=NodeType.FLOAT, node_id="ns=4;i=27", unit="V"))
    nodes.add(OPCUANode(name="l3_l1_voltage", type=NodeType.FLOAT, node_id="ns=4;i=28", unit="V"))
    nodes.add(OPCUANode(name="total_power_factor", type=NodeType.FLOAT, node_id="ns=4;i=29", unit=""))

    nodes.add(
        Node(
            name="total_active_energy",
            type=NodeType.FLOAT,
            unit="kWh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=True,
            logging_period=5,
        )
    )
    nodes.add(
        Node(
            name="total_reactive_energy",
            type=NodeType.FLOAT,
            unit="kVArh",
            incremental_node=True,
            calculate_increment=False,
            calculated=True,
            logging=True,
            logging_period=5,
        )
    )

    nodes.add(Node(name="total_power_factor_direction", type=NodeType.STRING, unit="", calculated=True))
    nodes.add(Node(name="total_active_power", type=NodeType.FLOAT, unit="kW", calculated=True, logging=True))
    nodes.add(Node(name="total_reactive_power", type=NodeType.FLOAT, unit="kVAr", calculated=True, logging=True))
    nodes.add(Node(name="total_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True, logging=True))

    node_records: set[NodeRecord] = set()
    for node in nodes:
        record = node.get_node_record()
        node_records.add(record)

    return EnergyMeterRecord(
        name="SM1238 S7-1200 Meter",
        protocol=Protocol.OPC_UA,
        device_type=EnergyMeterType.THREE_PHASE,
        meter_options=meter_options,
        connection_options=connection_options,
        nodes=node_records,
    )
