############### EXTERNAL IMPORTS ################

import aiosqlite
import json
from typing import List, Dict, Tuple, Set, Any, Optional

#################################################

############### LOCAL IMPORTS ###################

from util.debug import LoggerManager
from controller.registry.protocol import ProtocolRegistry
from model.controller.device import EnergyMeterRecord, DeviceHistoryStatus
from model.controller.node import NodeRecord

#################################################


class SQLiteDBClient:
    """
    Async SQLite database client for energy meter device and node configuration management.

    Provides CRUD operations for devices and their associated data nodes with WAL mode
    for concurrent access and foreign key constraints for data integrity.

    Attributes:
        db_path (str): Path to the SQLite database file.
        conn (sqlite3.Connection): Database connection object.
        cursor (sqlite3.Cursor): Database cursor for executing queries.
    """

    def __init__(self, db_path: str = "config.db"):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
        self.cursor: Optional[aiosqlite.Cursor] = None

    async def init_connection(self) -> None:
        """
        Initiates the SQLite connection.
        Should be called during application initialization.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.conn is not None or self.cursor is not None:
                raise RuntimeError("DB connection is already instantiated")
            self.conn = await aiosqlite.connect(self.db_path)
            await self.conn.execute("PRAGMA journal_mode=WAL;")  # Enable WAL mode
            await self.conn.execute("PRAGMA foreign_keys=ON;")  # Enable foreign key constraints
            await self.create_tables()
        except Exception as e:
            logger.exception(f"Failed to initiate SQLite connecion: {e}")

    async def close_connection(self) -> None:
        """
        Closes the SQLite connection.
        Should be called during application shutdown.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.conn:
                await self.conn.close()
                self.conn = None

        except Exception as e:
            logger.exception(f"Failed to close SQLite connection: {e}")

    def require_client(self) -> aiosqlite.Connection:
        """
        Return the active database connection.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.conn is None:
            raise RuntimeError(
                f"DB client is not instantiated properly. "
                f"Type of connection: {type(self.conn).__name__}, "
            )
        return self.conn

    async def create_tables(self) -> None:
        """
        Creates the required SQLite tables for storing energy meter configurations and operational status.

        Tables created:
            - devices: Stores energy meter-level configuration including protocol, meter options, and connection options.
            - nodes: Stores individual data points (nodes) associated with each device, including protocol-specific and common configuration.
            - device_status: Stores operational status information for each device including connection timestamps and status tracking.

        Notes:
            - Each node is linked to a device via a foreign key (device_id).
            - Each device_status entry is linked to a device via a foreign key (device_id).
            - Devices use an auto-incrementing primary key (id).
            - Nodes and device_status entries are automatically deleted if their parent device is removed (ON DELETE CASCADE).
            - Device status table tracks last seen time, and record timestamps.
        """

        logger = LoggerManager.get_logger(__name__)
        conn = self.require_client()

        try:

            await conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    meter_options TEXT NOT NULL,
                    communication_options TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    config TEXT NOT NULL,
                    protocol_options TEXT NOT NULL,
                    attributes TEXT NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS device_status (
                    device_id INTEGER PRIMARY KEY,
                    last_seen TEXT DEFAULT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT NULL,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                );
            """
            )
            await conn.commit()
        except Exception as e:
            logger.exception(f"Failed to create tables: {e}")
            raise

    async def insert_energy_meter(self, record: EnergyMeterRecord, conn: aiosqlite.Connection) -> int | None:
        """
        Inserts a new energy meter (device) into the database along with all associated
        nodes and an initial device status record.

        This method performs multiple INSERT operations using the provided database
        cursor but does NOT manage the transaction lifecycle. It assumes an active
        transaction context controlled by the caller. No commit or rollback is
        performed inside this method.

        Args:
            record (EnergyMeterRecord): Structured energy meter data, including device
                configuration and associated node definitions.
            conn (aiosqlite.Connection): Active SQLite database connection.

        Returns:
            int | None: The ID of the newly inserted device if successful, None if an
            error occurs during insertion.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            async with conn.execute(
                """
                INSERT INTO devices (name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    record.name,
                    record.protocol,
                    record.type,
                    json.dumps(record.options.get_meter_options()),
                    json.dumps(record.communication_options.get_communication_options()),
                ),
            ) as cursor:
                device_id = cursor.lastrowid

            for node in record.nodes:
                node.device_id = device_id
                await conn.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config, protocol_options, attributes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        device_id,
                        node.name,
                        node.protocol,
                        json.dumps(node.config.get_config()),
                        json.dumps(node.protocol_options.get_options()),
                        json.dumps(node.attributes.get_attributes()),
                    ),
                )

            # Create initial device status entry
            await conn.execute(
                """
                INSERT INTO device_status (device_id)
                VALUES (?)
                """,
                (device_id,),
            )

            return device_id

        except aiosqlite.OperationalError as e:
            logger.error(f"Operational error while trying to insert energy meter {record.name}: {e}")
            return None
        except Exception as e:
            return None

    async def update_energy_meter(self, record: EnergyMeterRecord, conn: aiosqlite.Connection) -> bool:
        """
        Updates an existing energy meter configuration by fully replacing its
        device and node definitions.

        This method performs a complete replacement of the energy meter data by
        deleting the existing device and its associated nodes, then inserting the
        updated configuration. The device status record is preserved when present
        and its timestamps are updated accordingly.

        This method executes multiple database operations using the provided
        cursor but does NOT manage the transaction lifecycle. It assumes an active
        transaction context controlled by the caller. No commit or rollback is
        performed inside this method.

        Args:
            record (EnergyMeterRecord): Updated energy meter configuration, including
                associated nodes. The record must contain a valid `id` field
                identifying the device to update.
            conn (aiosqlite.Connection): Active SQLite database connection.

        Returns:
            bool: True if the update operations complete successfully, False if an
            error occurs or the specified device does not exist.
        """

        logger = LoggerManager.get_logger(__name__)

        if record.id is None:
            logger.error("Cannot update energy meter: record ID is required")
            return False

        try:
            # Retrieve existing device status to preserve timestamps
            async with conn.execute(
                """
                SELECT last_seen, created_at 
                FROM device_status 
                WHERE device_id = ?
                """,
                (record.id,),
            ) as cursor:
                status_data = await cursor.fetchone()

            # Delete existing nodes and device
            await conn.execute("DELETE FROM nodes WHERE device_id = ?", (record.id,))
            async with conn.execute("DELETE FROM devices WHERE id = ?", (record.id,)) as cursor:
                if not cursor.rowcount:
                    logger.warning(f"No energy meter found with ID {record.id}")
                    return False


            # Insert the updated device configuration
            await conn.execute(
                """
                INSERT INTO devices (id, name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.id,
                    record.name,
                    record.protocol,
                    record.type,
                    json.dumps(record.options.get_meter_options()),
                    json.dumps(record.communication_options.get_communication_options()),
                ),
            )

            # Insert all associated nodes
            for node in record.nodes:
                node.device_id = record.id
                await conn.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config, protocol_options, attributes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.id,
                        node.name,
                        node.protocol,
                        json.dumps(node.config.get_config()),
                        json.dumps(node.protocol_options.get_options()),
                        json.dumps(node.attributes.get_attributes()),
                    ),
                )

            # Update or create device history status
            if status_data:
                await conn.execute(
                    """
                    INSERT INTO device_status (device_id, last_seen, created_at, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (record.id, status_data[0], status_data[1]),
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO device_status (device_id)
                    VALUES (?)
                    """,
                    (record.id,),
                )

            return True
        except aiosqlite.OperationalError as e:
            logger.error(f"Operational error while trying to update energy meter {record.name} with id {record.id}: {e}")
            return False
        except Exception as e:
            return False

    async def delete_device(self, device_id: int, conn: aiosqlite.Connection) -> bool:
        """
        Deletes a device and all its associated data from the database.

        This method deletes the device record identified by the given ID. All related
        records (such as nodes and device status entries) are removed automatically
        via foreign key cascade constraints.

        This method executes a DELETE operation using the provided cursor but does
        NOT manage the transaction lifecycle. It assumes an active transaction
        context controlled by the caller. No commit or rollback is performed inside
        this method.

        Args:
            device_id (int): The unique ID of the device to delete.
            conn (aiosqlite.Connection): Active SQLite database connection.

        Returns:
            bool: True if the device was successfully deleted, False if the device
            does not exist or an error occurs.

        Note:
            - Foreign key cascade constraints are responsible for removing all
            dependent records.
            - Transaction commit or rollback must be handled by the caller.
        """

        logger = LoggerManager.get_logger(__name__)

        try:

            # Delete the device (other tables will be cascade deleted due to foreign key)
            async with conn.execute("DELETE FROM devices WHERE id = ?", (device_id,)) as cursor:
                if cursor.rowcount == 0:
                    logger.warning(f"No energy meter found with ID {device_id}")
                    return False

            return True

        except Exception as e:
            return False

    async def get_all_energy_meters(self) -> List[EnergyMeterRecord]:
        """
        Retrieves all energy meters from the database, including their associated nodes.

        Returns:
            List[EnergyMeterRecord]: A list of fully populated energy meter records.
        """

        logger = LoggerManager.get_logger(__name__)

        meters: List[EnergyMeterRecord] = []
        conn = self.require_client()

        try:
            async with conn.execute(
                """
                SELECT id, name, protocol, device_type, meter_options, communication_options
                FROM devices
            """
            ) as cursor:
                device_rows = await cursor.fetchall()

            for device_row in device_rows:
                (
                    device_id,
                    name,
                    protocol,
                    device_type,
                    meter_opts_json,
                    comm_opts_json,
                ) = device_row

                async with conn.execute(
                    """
                    SELECT name, protocol, config, protocol_options, attributes FROM nodes WHERE device_id = ?
                """,
                    (device_id,),
                ) as cursor:
                    node_rows = await cursor.fetchall()

                nodes: Set[NodeRecord] = set()
                for node_name, node_protocol, config_json, protocol_options_json, attributes_json in node_rows:
                    config: Dict[str, Any] = json.loads(config_json)
                    protocol_options: Dict[str, Any] = json.loads(protocol_options_json)
                    attributes: Dict[str, Any] = json.loads(attributes_json)
                    nodes.add(
                        ProtocolRegistry.get_protocol_plugin(node_protocol).node_record_factory(
                            node_name, node_protocol, config, protocol_options, attributes
                        )
                    )

                meter_opts: Dict[str, Any] = json.loads(meter_opts_json)
                comm_opts: Dict[str, Any] = json.loads(comm_opts_json)
                meter_factory = ProtocolRegistry.get_protocol_plugin(protocol).meter_record_factory
                if meter_factory is None:
                    raise RuntimeError(f"No meter record factory registered for protocol {protocol}.")
                meters.append(meter_factory(device_id, name, protocol, device_type, meter_opts, comm_opts, nodes))

        except Exception as e:
            logger.exception(f"Failed to retrieve energy meters: {e}")

        return meters

    async def update_device_last_seen(self, device_id: int) -> bool:
        """
        Updates device last seen timestamp.

        Args:
            device_id (int): Device identifier

        Returns:
            bool: True if successful, False otherwise
        """

        logger = LoggerManager.get_logger(__name__)
        conn = self.require_client()

        try:
            await conn.execute(
                """
                INSERT INTO device_status (device_id, last_seen)
                VALUES (?, CURRENT_TIMESTAMP)
                ON CONFLICT(device_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP
                """,
                (device_id,),
            )

            await conn.commit()
            return True

        except Exception as e:
            logger.exception(f"Failed to update last seen timestamp for device {device_id}: {e}")
            return False

    async def get_device_history(self, device_id: int) -> DeviceHistoryStatus:
        """
        Retrieves the connection history and status timestamps for a device.

        Args:
            device_id (int): Device identifier

        Returns:
            DeviceHistoryStatus: Object containing last seen timestamp and record lifecycle info
        """

        logger = LoggerManager.get_logger(__name__)
        conn = self.require_client()

        try:
            async with conn.execute(
                """
                SELECT last_seen, created_at, updated_at
                FROM device_status
                WHERE device_id = ?
                """,
                (device_id,),
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                return DeviceHistoryStatus(
                    last_seen=row[0],
                    created_at=row[1],
                    updated_at=row[2],
                )
            else:
                logger.warning(f"No status record found for device ID {device_id}")
                return DeviceHistoryStatus(None, None, None)

        except Exception as e:
            logger.exception(f"Failed to retrieve device history for device {device_id}: {e}")
            return DeviceHistoryStatus(None, None, None)
