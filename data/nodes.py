###########EXERTNAL IMPORTS############

import asyncio

#######################################

#############LOCAL IMPORTS#############

from controller.device import Node, NodeType
from protocol.modbus_rtu.rtu_device import ModbusRTUNode

#######################################


def get_orno_we_516_nodes() -> {ModbusRTUNode}:

    return {
        ModbusRTUNode(name="l1_voltage", type=NodeType.FLOAT, register=0x000E, unit="V", logging=True, logging_period=1),
        ModbusRTUNode(name="l1_current", type=NodeType.FLOAT, register=0x0016, unit="A"),
        ModbusRTUNode(name="l1_active_power", type=NodeType.FLOAT, register=0x001E, unit="kW"),
        ModbusRTUNode(name="l1_reactive_power", type=NodeType.FLOAT, register=0x0026, unit="kVAr"),
        ModbusRTUNode(name="l1_forward_active_energy", type=NodeType.FLOAT, register=0x010A, unit="kWh"),
        ModbusRTUNode(name="l1_reverse_active_energy", type=NodeType.FLOAT, register=0x0112, unit="kWh"),
        ModbusRTUNode(name="l1_forward_reactive_energy", type=NodeType.FLOAT, register=0x0122, unit="kVArh"),
        ModbusRTUNode(name="l1_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012A, unit="kVArh"),
        ModbusRTUNode(name="l2_voltage", type=NodeType.FLOAT, register=0x0010, unit="V"),
        ModbusRTUNode(name="l2_current", type=NodeType.FLOAT, register=0x0018, unit="A"),
        ModbusRTUNode(name="l2_active_power", type=NodeType.FLOAT, register=0x0020, unit="kW"),
        ModbusRTUNode(name="l2_reactive_power", type=NodeType.FLOAT, register=0x0028, unit="kVAr"),
        ModbusRTUNode(name="l2_forward_active_energy", type=NodeType.FLOAT, register=0x010C, unit="kWh"),
        ModbusRTUNode(name="l2_reverse_active_energy", type=NodeType.FLOAT, register=0x0114, unit="kWh"),
        ModbusRTUNode(name="l2_forward_reactive_energy", type=NodeType.FLOAT, register=0x0124, unit="kVArh"),
        ModbusRTUNode(name="l2_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012C, unit="kVArh"),
        ModbusRTUNode(name="l3_voltage", type=NodeType.FLOAT, register=0x0012, unit="V"),
        ModbusRTUNode(name="l3_current", type=NodeType.FLOAT, register=0x001A, unit="A"),
        ModbusRTUNode(name="l3_active_power", type=NodeType.FLOAT, register=0x0022, unit="kW"),
        ModbusRTUNode(name="l3_reactive_power", type=NodeType.FLOAT, register=0x002A, unit="kVAr"),
        ModbusRTUNode(name="l3_forward_active_energy", type=NodeType.FLOAT, register=0x010E, unit="kWh"),
        ModbusRTUNode(name="l3_reverse_active_energy", type=NodeType.FLOAT, register=0x0116, unit="kWh"),
        ModbusRTUNode(name="l3_forward_reactive_energy", type=NodeType.FLOAT, register=0x0126, unit="kVArh"),
        ModbusRTUNode(name="l3_reverse_reactive_energy", type=NodeType.FLOAT, register=0x012E, unit="kVArh"),
        ModbusRTUNode(name="frequency", type=NodeType.FLOAT, register=0x0014, unit="Hz"),
        
        #Calculated Nodes
        Node(name="l1_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, calculate_increment=False, calculated=True, logging=True),
        Node(name="l1_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, calculate_increment=False, calculated=True),
        Node(name="l1_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True),
        Node(name="l1_power_factor", type=NodeType.FLOAT, unit="", calculated=True),
        Node(name="l1_power_factor_direction", type=NodeType.STRING, unit="", calculated=True),
        
        
        Node(name="l2_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, calculate_increment=False, calculated=True, logging=True),
        Node(name="l2_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, calculate_increment=False, calculated=True),
        Node(name="l2_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True),
        Node(name="l2_power_factor", type=NodeType.FLOAT, unit="", calculated=True),
        Node(name="l2_power_factor_direction", type=NodeType.STRING, unit="", calculated=True),
        
        
        Node(name="l3_active_energy", type=NodeType.FLOAT, unit="kWh", incremental_node=True, calculate_increment=False, calculated=True, logging=True),
        Node(name="l3_reactive_energy", type=NodeType.FLOAT, unit="kVArh", incremental_node=True, calculate_increment=False, calculated=True),
        Node(name="l3_apparent_power", type=NodeType.FLOAT, unit="kVA", calculated=True),
        Node(name="l3_power_factor", type=NodeType.FLOAT, unit="", calculated=True),
        Node(name="l3_power_factor_direction", type=NodeType.STRING, unit="", calculated=True),
    }
