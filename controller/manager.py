###########EXTERNAL IMPORTS############

import asyncio
from typing import Optional, Dict, Set, Any

#######################################

#############LOCAL IMPORTS#############

from mqtt.client import MQTTMessage
from db.db import EnergyMeterRecord, SQLiteDBClient
from controller.enums import Protocol
from controller.device import Device
from controller.node import Node, NodeType
from controller.meter import EnergyMeterOptions
from protocol.modbus_rtu.rtu_device import ModbusRTUNode, ModbusRTUEnergyMeter, ModbusRTUOptions
from protocol.opcua.opcua_device import OPCUANode, OPCUAEnergyMeter, OPCUAOptions
from util.debug import LoggerManager

#######################################


class DeviceManager:
    """
    Manages a collection of energy meters, including their registration,
    database persistence, and periodic state publishing via MQTT.

    Responsibilities:
        - Maintains a set of active devices (EnergyMeter instances).
        - Publishes device status and data to an MQTT broker.
        - Interfaces with the SQLite database for persisting and retrieving device configurations.
        - Routes measurement data from devices to the appropriate processing queue.

    Attributes:
        devices (Set[Device]): A set of registered devices currently managed in memory.
        publish_queue (asyncio.Queue): Queue used to send MQTT messages from devices.
        measurements_queue (asyncio.Queue): Queue used to send measurement data from devices.
        devices_db (SQLiteDBClient): Database client used for persisting and loading devices and their nodes.
    """

    def __init__(self, publish_queue: asyncio.Queue, measurements_queue: asyncio.Queue, devices_db: SQLiteDBClient):
        self.devices: Set[Device] = set()
        self.publish_queue = publish_queue
        self.measurements_queue = measurements_queue
        self.devices_db = devices_db
        self.start()

    def start(self) -> None:
        """
        Loads all devices from the database, initializes them, and starts
        the background task that handles periodic device state publishing.
        """

        logger = LoggerManager.get_logger(__name__)

        loop = asyncio.get_event_loop()

        try:
            meter_records = self.devices_db.get_all_energy_meters()

            for record in meter_records:
                device = self.create_device_from_record(record)
                self.devices.add(device)

            self.handler_task = loop.create_task(self.handle_devices())

        except Exception as e:
            logger.exception(f"Failed to start DeviceManager: {e}")

    def add_device(self, device: Device):
        """
        Registers a new device.

        Args:
            device (Device): The device instance to add.
        """

        self.devices.add(device)

    def delete_device(self, device: Device) -> None:
        """
        Removes a device from the manager if it exists.

        Args:
            device (Device): The device instance to remove.
        """

        self.devices.discard(device)

    def get_device(self, name: str, device_id: int) -> Optional[Device]:
        """
        Retrieves a device by name and ID.

        Args:
            name (str): The name of the device.
            device_id (int): The unique identifier of the device.

        Returns:
            Optional[Device]: The matched device, or None if not found.
        """

        return next((device for device in self.devices if device.name == name and device.id == device_id), None)

    async def handle_devices(self):
        """
        Periodically publishes the state of all registered devices.
        """

        while True:
            await self.publish_devices_state()
            await asyncio.sleep(10)

    async def publish_devices_state(self):
        """
        Publishes a dictionary of all device states to the MQTT queue.
        """

        topic = "devices_state"
        payload: Dict[int, Dict[str, Any]] = {device.id: device.get_device_state() for device in self.devices}
        await self.publish_queue.put(MQTTMessage(qos=0, topic=topic, payload=payload))

    def create_device_from_record(self, record: EnergyMeterRecord) -> Device:
        """
        Reconstructs a Device instance (e.g., ModbusRTUEnergyMeter or OPCUAEnergyMeter)
        from a persisted EnergyMeterRecord retrieved from the database.

        This method initializes the appropriate device class based on the protocol
        specified in the record, attaches the publish and measurement queues,
        and reconstructs the list of configured nodes.

        Args:
            record (EnergyMeterRecord): The device configuration record including metadata,
                options, and associated node records.

        Returns:
            Device: A fully initialized energy meter instance based on the protocol.

        Raises:
            ValueError: If the protocol specified in the record is unsupported.
        """

        if record.protocol == Protocol.MODBUS_RTU:
            return ModbusRTUEnergyMeter(
                id=record.id,
                name=record.name,
                publish_queue=self.publish_queue,
                measurements_queue=self.measurements_queue,
                meter_type=record.device_type,
                meter_options=EnergyMeterOptions(**record.meter_options),
                connection_options=ModbusRTUOptions(**record.connection_options),
                nodes=self.create_nodes(record),
            )
        elif record.protocol == Protocol.OPC_UA:
            return OPCUAEnergyMeter(
                id=record.id,
                name=record.name,
                publish_queue=self.publish_queue,
                measurements_queue=self.measurements_queue,
                meter_type=record.device_type,
                meter_options=EnergyMeterOptions(**record.meter_options),
                connection_options=OPCUAOptions(**record.connection_options),
                nodes=self.create_nodes(record),
            )
        else:
            raise ValueError(f"Unsupported protocol: {record.protocol}")

    def create_nodes(self, record: EnergyMeterRecord) -> Set[Node]:
        """
        Creates a set of Node instances based on the NodeRecords in the given EnergyMeterRecord.

        Args:
            record (EnergyMeterRecord): The record containing node configurations.

        Returns:
            Set[Node]: A set of fully constructed Node, ModbusRTUNode, or OPCUANode instances.
        """

        created_nodes: Set[Node] = set()

        for node_record in record.nodes:
            cfg = node_record.config
            node_type = NodeType(cfg["type"])

            if node_record.protocol == Protocol.MODBUS_RTU:
                created_nodes.add(
                    ModbusRTUNode(
                        name=node_record.name,
                        type=node_type,
                        register=cfg["register"],
                        unit=cfg.get("unit"),
                        publish=cfg.get("publish", True),
                        calculated=cfg.get("calculated", False),
                        logging=cfg.get("logging", False),
                        logging_period=cfg.get("logging_period", 15),
                        min_alarm=cfg.get("min_alarm", False),
                        max_alarm=cfg.get("max_alarm", False),
                        min_alarm_value=cfg.get("min_alarm_value"),
                        max_alarm_value=cfg.get("max_alarm_value"),
                        incremental_node=cfg.get("incremental_node"),
                        positive_incremental=cfg.get("positive_incremental"),
                        calculate_increment=cfg.get("calculate_increment"),
                    )
                )

            elif node_record.protocol == Protocol.OPC_UA:
                created_nodes.add(
                    OPCUANode(
                        name=node_record.name,
                        type=node_type,
                        node_id=cfg["node_id"],
                        unit=cfg.get("unit"),
                        publish=cfg.get("publish", True),
                        calculated=cfg.get("calculated", False),
                        logging=cfg.get("logging", False),
                        logging_period=cfg.get("logging_period", 15),
                        min_alarm=cfg.get("min_alarm", False),
                        max_alarm=cfg.get("max_alarm", False),
                        min_alarm_value=cfg.get("min_alarm_value"),
                        max_alarm_value=cfg.get("max_alarm_value"),
                        incremental_node=cfg.get("incremental_node"),
                        positive_incremental=cfg.get("positive_incremental"),
                        calculate_increment=cfg.get("calculate_increment"),
                    )
                )

            else:
                created_nodes.add(
                    Node(
                        name=node_record.name,
                        type=node_type,
                        unit=cfg.get("unit"),
                        publish=cfg.get("publish", True),
                        calculated=cfg.get("calculated", False),
                        logging=cfg.get("logging", False),
                        logging_period=cfg.get("logging_period", 15),
                        min_alarm=cfg.get("min_alarm", False),
                        max_alarm=cfg.get("max_alarm", False),
                        min_alarm_value=cfg.get("min_alarm_value"),
                        max_alarm_value=cfg.get("max_alarm_value"),
                        incremental_node=cfg.get("incremental_node"),
                        positive_incremental=cfg.get("positive_incremental"),
                        calculate_increment=cfg.get("calculate_increment"),
                    )
                )

        return created_nodes
