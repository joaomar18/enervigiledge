############### EXTERNAL IMPORTS ################

import sqlite3
import json
from typing import List, Tuple, Optional
from dataclasses import dataclass

#################################################

############### LOCAL IMPORTS ###################

from util.debug import LoggerManager
from model.controller.general import Protocol
from model.controller.device import EnergyMeterType, EnergyMeterRecord, DeviceHistoryStatus
from model.controller.node import NodeRecord

#################################################


class SQLiteDBClient:
    """
    SQLite database client for energy meter device and node configuration management.

    Provides CRUD operations for devices and their associated data nodes with WAL mode
    for concurrent access and foreign key constraints for data integrity.

    Attributes:
        db_path (str): Path to the SQLite database file.
        conn (sqlite3.Connection): Database connection object.
        cursor (sqlite3.Cursor): Database cursor for executing queries.
    """

    def __init__(self, db_path: str = "config.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    async def init_connection(self) -> None:
        """
        Initiates the SQLite connection.
        Should be called during application initialization.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            if self.conn is not None or self.cursor is not None:
                raise RuntimeError("DB connection or cursor are already instantiated")
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.conn.execute("PRAGMA journal_mode=WAL;")  # Enable WAL mode
            self.conn.execute("PRAGMA foreign_keys=ON;")  # Enable foreign key constraints
            self.create_tables()
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
                self.conn.close()
                self.conn = None

            if self.cursor:
                self.cursor = None

        except Exception as e:
            logger.exception(f"Failed to close SQLite connection: {e}")

    def __require_client(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """
        Return the active database connection and cursor.

        Raises:
            RuntimeError: If the client is not initialized.
        """

        if self.conn is None or self.cursor is None:
            raise RuntimeError(
                f"DB client is not instantiated properly. "
                f"Type of connection: {type(self.conn).__name__}, "
                f"Type of cursor: {type(self.cursor).__name__}"
            )
        return self.conn, self.cursor

    def create_tables(self) -> None:
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
            - Device status table tracks first connection time, last connection time, and record timestamps.
        """

        logger = LoggerManager.get_logger(__name__)
        conn, cursor = self.__require_client()

        try:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS devices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    meter_options TEXT NOT NULL,
                    communication_options TEXT NOT NULL
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    config TEXT NOT NULL,
                    protocol_options TEXT NOT NULL,
                    attributes TEXT NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                )
            """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS device_status (
                    device_id INTEGER PRIMARY KEY,
                    connection_on_datetime TEXT DEFAULT NULL,
                    connection_off_datetime TEXT DEFAULT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                )
            """
            )

            conn.commit()
        except Exception as e:
            logger.exception(f"Failed to create tables: {e}")

    def insert_energy_meter(self, record: EnergyMeterRecord) -> int | None:
        """
        Inserts a new energy meter (device) into the database along with all associated nodes and status tracking.

        This method creates a complete energy meter record including device configuration,
        all associated node configurations, and initializes the device status tracking
        with default timestamps. Uses database transactions to ensure atomicity.

        Args:
            record (EnergyMeterRecord): Structured energy meter data including nodes.

        Returns:
            int | None: The ID of the inserted device if successful, None otherwise.

        Note:
            Creates the following records:
            - Device record with configuration
            - All associated node records
            - Device status record with default timestamps (connection times set to NULL)
        """

        logger = LoggerManager.get_logger(__name__)
        conn, cursor = self.__require_client()

        try:
            cursor.execute(
                """
                INSERT INTO devices (name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.name, record.protocol, record.type, json.dumps(record.options), json.dumps(record.communication_options)),
            )
            device_id = cursor.lastrowid

            for node in record.nodes:
                node.device_id = device_id
                cursor.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config, protocol_options, attributes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (device_id, node.name, node.protocol, json.dumps(node.config), json.dumps(node.protocol_options), json.dumps(node.attributes)),
                )

            # Create initial device status entry
            cursor.execute(
                """
                INSERT INTO device_status (device_id)
                VALUES (?)
                """,
                (device_id,),
            )

            conn.commit()
            logger.info(f"Successfully added energy meter '{record.name}' with ID {device_id}")
            return device_id

        except Exception as e:
            logger.exception(f"Failed to insert energy meter '{record.name}': {e}")
            conn.rollback()
            return None

    def update_energy_meter(self, record: EnergyMeterRecord) -> bool:
        """
        Updates an existing energy meter configuration by replacing it entirely.

        This method performs a complete replacement of the energy meter configuration
        by deleting the existing record and inserting the new one. Uses database
        transactions to ensure atomicity and data consistency. The device status
        is preserved and updated with a new timestamp.

        Args:
            record (EnergyMeterRecord): New energy meter configuration including nodes.
                Must have a valid id field to identify the record to update.

        Returns:
            bool: True if update successful, False otherwise.

        Note:
            - Device status record is preserved (not deleted/recreated)
            - Device status updated_at timestamp is refreshed
            - Connection timestamps (first/last) remain unchanged
            - Creates device status if it doesn't exist
        """

        logger = LoggerManager.get_logger(__name__)

        if record.id is None:
            logger.error("Cannot update energy meter: record ID is required")
            return False

        conn, cursor = self.__require_client()

        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            # Preserve device status by temporarily storing it
            cursor.execute(
                """
                SELECT connection_on_datetime, connection_off_datetime, created_at 
                FROM device_status 
                WHERE device_id = ?
                """,
                (record.id,),
            )
            status_data = cursor.fetchone()

            # Delete existing nodes and device
            cursor.execute("DELETE FROM nodes WHERE device_id = ?", (record.id,))
            cursor.execute("DELETE FROM devices WHERE id = ?", (record.id,))

            if cursor.rowcount == 0:
                logger.warning(f"No energy meter found with ID {record.id}")
                conn.rollback()
                return False

            # Insert the updated device configuration
            cursor.execute(
                """
                INSERT INTO devices (id, name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record.id, record.name, record.protocol, record.type, json.dumps(record.options), json.dumps(record.communication_options)),
            )

            # Insert all associated nodes
            for node in record.nodes:
                node.device_id = record.id
                cursor.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config, protocol_options, attributes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (record.id, node.name, node.protocol, json.dumps(node.config), json.dumps(node.protocol_options), json.dumps(node.attributes)),
                )

            # Restore device status with preserved created_at and updated updated_at
            if status_data:
                cursor.execute(
                    """
                    INSERT INTO device_status (device_id, connection_on_datetime, connection_off_datetime, created_at, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (record.id, status_data[0], status_data[1], status_data[2]),
                )
            else:
                # Create new status if none existed
                cursor.execute(
                    """
                    INSERT INTO device_status (device_id)
                    VALUES (?)
                    """,
                    (record.id,),
                )

            # Commit transaction
            conn.commit()
            logger.info(f"Successfully updated energy meter '{record.name}' with ID {record.id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to update energy meter '{record.name}' with ID {record.id}: {e}")
            conn.rollback()
            return False

    def delete_device(self, device_id: int) -> bool:
        """
        Deletes a device and all its associated data from the database.

        Uses database transactions to ensure atomicity. The foreign key cascade
        constraints automatically remove all tables when the device is deleted.

        Args:
            device_id(int): ID of the device to delete

        Returns:
            bool: True if deletion successful, False otherwise.
        """

        logger = LoggerManager.get_logger(__name__)
        conn, cursor = self.__require_client()

        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            # Delete the device (other tables will be cascade deleted due to foreign key)
            cursor.execute("DELETE FROM devices WHERE id = ?", (device_id,))

            if cursor.rowcount == 0:
                logger.warning(f"No energy meter found with ID {device_id}")
                conn.rollback()
                return False

            # Commit transaction
            conn.commit()
            logger.info(f"Successfully deleted device with ID {device_id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to delete device with ID {device_id}: {e}")
            conn.rollback()
            return False

    def get_all_energy_meters(self) -> List[EnergyMeterRecord]:
        """
        Retrieves all energy meters from the database, including their associated nodes.

        Returns:
            List[EnergyMeterRecord]: A list of fully populated energy meter records.
        """

        logger = LoggerManager.get_logger(__name__)

        meters: List[EnergyMeterRecord] = []

        conn, cursor = self.__require_client()

        try:
            cursor.execute(
                """
                SELECT id, name, protocol, device_type, meter_options, communication_options
                FROM devices
            """
            )
            device_rows = cursor.fetchall()

            for device_row in device_rows:
                (
                    device_id,
                    name,
                    protocol,
                    device_type,
                    meter_opts_str,
                    comm_opts_str,
                ) = device_row

                cursor.execute(
                    """
                    SELECT name, protocol, config, protocol_options, attributes FROM nodes WHERE device_id = ?
                """,
                    (device_id,),
                )
                node_rows = cursor.fetchall()

                nodes = set()
                for node_name, node_protocol, config_str, protocol_options_str, attributes_str in node_rows:
                    node = NodeRecord(
                        device_id=device_id,
                        name=node_name,
                        protocol=node_protocol,
                        config=json.loads(config_str),
                        protocol_options=json.loads(protocol_options_str),
                        attributes=json.loads(attributes_str),
                    )
                    nodes.add(node)

                meter = EnergyMeterRecord(
                    id=device_id,
                    name=name,
                    protocol=Protocol(protocol),
                    type=EnergyMeterType(device_type),
                    options=json.loads(meter_opts_str),
                    communication_options=json.loads(comm_opts_str),
                    nodes=nodes,
                )

                meters.append(meter)

        except Exception as e:
            logger.exception(f"Failed to retrieve energy meters: {e}")

        return meters

    def update_device_connection_history(self, device_id: int, status: bool) -> bool:
        """
        Updates device connection timestamps.

        Args:
            device_id (int): Device identifier
            status (bool): True for connected, False for disconnected

        Returns:
            bool: True if successful, False otherwise
        """

        logger = LoggerManager.get_logger(__name__)
        conn, cursor = self.__require_client()

        try:
            conn_parameter = "on" if status else "off"

            cursor.execute(
                f"""
                UPDATE device_status 
                SET connection_{conn_parameter}_datetime = CURRENT_TIMESTAMP
                WHERE device_id = ?
                """,
                (device_id,),
            )

            conn.commit()
            return True

        except Exception as e:
            logger.exception(f"Failed to update connection status for device {device_id}: {e}")
            return False

    def get_device_history(self, device_id: int) -> DeviceHistoryStatus:
        """
        Retrieves the connection history and status timestamps for a device.

        Args:
            device_id (int): Device identifier

        Returns:
            DeviceHistoryStatus: Object containing connection timestamps and record lifecycle info
        """

        logger = LoggerManager.get_logger(__name__)
        conn, cursor = self.__require_client()

        try:
            cursor.execute(
                """
                SELECT connection_on_datetime, connection_off_datetime, created_at, updated_at
                FROM device_status
                WHERE device_id = ?
                """,
                (device_id,),
            )
            row = cursor.fetchone()

            if row:
                return DeviceHistoryStatus(
                    connection_on_datetime=row[0],
                    connection_off_datetime=row[1],
                    created_at=row[2],
                    updated_at=row[3],
                )
            else:
                logger.warning(f"No status record found for device ID {device_id}")
                return DeviceHistoryStatus(None, None, None, None)

        except Exception as e:
            logger.exception(f"Failed to retrieve device history for device {device_id}: {e}")
            return DeviceHistoryStatus(None, None, None, None)
