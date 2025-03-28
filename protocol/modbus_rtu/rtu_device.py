###########EXERTNAL IMPORTS############

import asyncio
import struct
from pymodbus.client import ModbusSerialClient as ModbusRTUClient

#######################################

#############LOCAL IMPORTS#############

import util.debug as debug
from controller.device import Device, Node, Protocol, NodeType
from controller.meter import EnergyMeter, EnergyMeterType, EnergyMeterOptions

#######################################


class ModbusRTUOptions:
    def __init__(
        self,
        slave_id: int,
        port: str,
        baudrate: int,
        stopbits: int,
        parity: str,
        bytesize: int,
        read_period: int,
        timeout: float,
    ):
        self.slave_id = slave_id
        self.port = port
        self.baudrate = baudrate
        self.stopbits = stopbits
        self.parity = parity
        self.bytesize = bytesize
        self.read_period = read_period
        self.timeout = timeout

    def set_slave_id(self, slave_id: int):
        self.slave_id = slave_id

    def set_port(self, port: str):
        self.port = port

    def set_baudrate(self, baudrate: int):
        self.baudrate = baudrate

    def set_stopbits(self, stopbits: int):
        self.stopbits = stopbits

    def set_parity(self, parity: str):
        self.parity = parity

    def set_bytesize(self, bytesize: int):
        self.bytesize = bytesize

    def set_timeout(self, timeout: float):
        self.timeout = timeout


class ModbusRTUNode(Node):
    def __init__(
        self,
        name: str,
        type: NodeType,
        register: int,
        unit: str,
        publish: bool = True,
        calculated: bool = False,
        logging: bool = False,
        logging_period: int = 0,
        min_alarm: bool = False,
        max_alarm: bool = False,
        min_alarm_value: float = 0.0,
        max_alarm_value: float = 0.0,
    ):
        super().__init__(
            name=name,
            type=type,
            unit=unit,
            publish=publish,
            calculated=calculated,
            logging=logging,
            logging_period=logging_period,
            min_alarm=min_alarm,
            max_alarm=max_alarm,
            min_alarm_value=min_alarm_value,
            max_alarm_value=max_alarm_value,
        )

        self.register = register


class ModbusRTUEnergyMeter(EnergyMeter):
    def __init__(
        self,
        id: int,
        name: str,
        publish_queue: asyncio.Queue,
        meter_type: EnergyMeterType,
        meter_options: EnergyMeterOptions,
        connection_options: ModbusRTUOptions,
        nodes: set[ModbusRTUNode] = set(),
    ):
        super().__init__(
            id=id,
            name=name,
            protocol=Protocol.MODBUS_RTU,
            publish_queue=publish_queue,
            meter_type=meter_type,
            meter_options=meter_options,
            meter_nodes=nodes,
        )

        self.nodes = nodes    
            
        self.connection_options = connection_options

        self.client = ModbusRTUClient(
            port=self.connection_options.port,
            baudrate=self.connection_options.baudrate,
            stopbits=self.connection_options.stopbits,
            parity=self.connection_options.parity,
            bytesize=self.connection_options.bytesize,
            timeout=self.connection_options.timeout,
        )

        self.start()

    def start(self):
        self.main_task = asyncio.get_event_loop().create_task(self._main())
        self.receiver_task = asyncio.get_event_loop().create_task(self._receiver())

    async def _main(self):
        while True:
            try:
                debug.logger.debug(
                    f"Trying to connect to client {self.name} with id {self.id}..."
                )
                self.connected = self.client.connect()
                if not self.connected:
                    raise Exception(
                        "Failed to connect to client {self.name} with id {self.id}"
                    )
                debug.logger.debug(f"Client {self.name} with id {self.id} connected")
                while True:
                    await asyncio.sleep(2)
                    if not self.client.connected:
                        raise Exception(
                            f"Client {self.name} with id {self.id} disconnected..."
                        )
            except Exception as e:
                if self.connected:
                    debug.logger.debug(f"Connection lost: %s" % (e))
                    self.connected = False
                await asyncio.sleep(2)

    async def _receiver(self):
        while True:
            try:
                if self.connected:
                    for node in self.nodes:
                        node.set_value(self._read_float(node))
                    await self.process_nodes()
                    await self.publish_nodes()
            except Exception as e:
                debug.logger.debug(f"{e}")
                self.connected = False
                self.client.close()
            await asyncio.sleep(self.connection_options.read_period)

    def _read_float(self, node: ModbusRTUNode):
        try:
            response = self.client.read_holding_registers(
                address=node.register,
                count=2,
                slave=self.connection_options.slave_id,
                no_response_expected=False,
            )
        
        except Exception as e:
            raise Exception(f"Exception while reading from device {node.name} at address {node.register}: {e}")

        if response is None:
            raise Exception(f"No response from device when reading {node.name} (address {node.register})")

        if response.isError():
            raise Exception(f"Error reading float register {node.name} with register address {node.register}: {response}")

        if not hasattr(response, "registers") or len(response.registers) < 2:
            raise Exception(f"Incomplete data received from {node.name} at register {node.register}")
        
        raw_value = struct.pack(">HH", response.registers[0], response.registers[1])
        return struct.unpack(">f", raw_value)[0]
