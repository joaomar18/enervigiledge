###########EXTERNAL IMPORTS############

import asyncio
from typing import Optional, Dict, Set, Any

#######################################

#############LOCAL IMPORTS#############

from mqtt.client import MQTTMessage
from db.db import SQLiteDBClient
from controller.types import Protocol, EnergyMeterRecord, NodeConfig
from controller.registry import ProtocolRegistry
from controller.device import Device
from controller.node import Node, NodeType
from controller.meter import EnergyMeterOptions
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
        self.handler_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """
        Starts the device handling background task.
        """

        logger = LoggerManager.get_logger(__name__)

        try:

            if self.handler_task is not None:
                raise ValueError("Handler task is already instantiated")

            await self.init_devices()
            loop = asyncio.get_event_loop()
            self.handler_task = loop.create_task(self.handle_devices())

        except Exception as e:
            logger.exception(f"Failed to start device manager handler task: {e}")

    async def stop(self) -> None:
        """
        Stops and cancels the device handling background task.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.handler_task:
                self.handler_task.cancel()
                await self.handler_task
                self.handler_task = None

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Failed to stop device manager handler task: {e}")

    async def init_devices(self) -> None:
        """
        Loads all devices from the database, initializes them, and starts
        the background task that handles periodic device state publishing.
        """

        meter_records = self.devices_db.get_all_energy_meters()

        for record in meter_records:
            device = self.create_device_from_record(record)
            await self.add_device(device)

    async def add_device(self, device: Device):
        """
        Registers a new device, configures its queues, and starts it asynchronously.

        Args:
            device (Device): The device instance to add.
        """

        if device.publish_queue != self.publish_queue:
            device.publish_queue = self.publish_queue
        if device.measurements_queue != self.measurements_queue:
            device.measurements_queue = self.measurements_queue

        await device.start()
        self.devices.add(device)

    async def delete_device(self, device: Device) -> None:
        """
        Stops and removes a device from the manager asynchronously.

        Args:
            device (Device): The device instance to remove.
        """

        await device.stop()
        self.devices.discard(device)

    def get_device(self, device_id: int) -> Optional[Device]:
        """
        Retrieves a device by ID.

        Args:
            device_id (int): The unique identifier of the device.

        Returns:
            Optional[Device]: The matched device, or None if not found.
        """

        return next((device for device in self.devices if device.id == device_id), None)

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

        plugin = ProtocolRegistry.get_protocol_plugin(record.protocol)
        if not plugin:
            raise ValueError(f"Unsupported protocol: {record.protocol}")

        return plugin.meter_class(
            id=record.id,
            name=record.name,
            publish_queue=self.publish_queue,
            measurements_queue=self.measurements_queue,
            meter_type=record.type,
            meter_options=EnergyMeterOptions(**record.options),
            communication_options=plugin.options_class(**record.communication_options),
            nodes=self.create_nodes(record),
        )

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

            try:
                protocol = Protocol(node_record.protocol)
            except ValueError:
                protocol = Protocol.NONE

            plugin = ProtocolRegistry.get_protocol_plugin(protocol)

            if plugin:
                created_nodes.add(plugin.node_factory(node_record))
                continue

            created_nodes.add(ProtocolRegistry.base_node_factory(node_record))

        return created_nodes
