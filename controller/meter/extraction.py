###########EXERTNAL IMPORTS############

import asyncio
import traceback
from typing import Dict, Any, Set, Callable
from abc import abstractmethod

#######################################

#############LOCAL IMPORTS#############

from controller.device import Device
from controller.node.node import Node
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterOptions, EnergyMeterRecord, BaseCommunicationOptions
from model.controller.node import NodePhase, NodeDirection
from controller.meter.nodes import EnergyMeterNodes
import controller.meter.calculation as calculation
from mqtt.client import MQTTMessage
from db.timedb import TimeDBClient
import util.functions.date as date
import util.functions.meter as meter_util
from util.debug import LoggerManager

#######################################

##########     T O     D O     ##########
def get_meter_energy_consumption(phase: NodePhase, direction: NodeDirection, meter_nodes: Dict[str, Node], timedb: TimeDBClient) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output

##########     T O     D O     ##########
def get_meter_energy_efficiency(phase: NodePhase, meter_nodes: Dict[str, Node], timedb: TimeDBClient) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output

##########     T O     D O     ##########
def get_meter_peak_power(phase: NodePhase, meter_nodes: Dict[str, Node], timedb: TimeDBClient) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output

##########     T O     D O     ##########
def get_meter_phase_balance(meter_nodes: Dict[str, Node], timedb: TimeDBClient) -> Dict[str, Any]:
    output: Dict[str, Any] = {}
    return output
