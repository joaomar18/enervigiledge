############### EXTERNAL IMPORTS ################

import sqlite3
import json
from typing import List, Dict, Set, Any, Optional
from dataclasses import dataclass

#################################################

############### LOCAL IMPORTS ###################

from util.debug import LoggerManager
from controller.types import Protocol, EnergyMeterType, NodeRecord, EnergyMeterRecord

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

        try:
            self.cursor.execute(
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

            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    config TEXT NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                )
            """
            )
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS device_status (
                    device_id INTEGER PRIMARY KEY,
                    connection_on_datetime TEXT,
                    connection_off_datetime TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
                )
            """
            )

            self.conn.commit()
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

        try:
            self.cursor.execute(
                """
                INSERT INTO devices (name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.name, record.protocol, record.type, json.dumps(record.options), json.dumps(record.communication_options)),
            )
            device_id = self.cursor.lastrowid

            for node in record.nodes:
                node.device_id = device_id
                self.cursor.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config)
                    VALUES (?, ?, ?, ?)
                    """,
                    (device_id, node.name, node.protocol, json.dumps(node.config)),
                )

            # Create initial device status entry
            self.cursor.execute(
                """
                INSERT INTO device_status (device_id)
                VALUES (?)
                """,
                (device_id,),
            )

            self.conn.commit()
            logger.info(f"Successfully added energy meter '{record.name}' with ID {device_id}")
            return device_id

        except Exception as e:
            logger.exception(f"Failed to insert energy meter '{record.name}': {e}")
            self.conn.rollback()
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

        try:
            # Begin transaction
            self.cursor.execute("BEGIN TRANSACTION")

            # Delete existing nodes and device
            self.cursor.execute("DELETE FROM nodes WHERE device_id = ?", (record.id,))
            self.cursor.execute("DELETE FROM devices WHERE id = ?", (record.id,))

            if self.cursor.rowcount == 0:
                logger.warning(f"No energy meter found with ID {record.id}")
                self.conn.rollback()
                return False

            # Insert the updated device configuration
            self.cursor.execute(
                """
                INSERT INTO devices (id, name, protocol, device_type, meter_options, communication_options)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record.id, record.name, record.protocol, record.type, json.dumps(record.options), json.dumps(record.communication_options)),
            )

            # Insert all associated nodes
            for node in record.nodes:
                node.device_id = record.id
                self.cursor.execute(
                    """
                    INSERT INTO nodes (device_id, name, protocol, config)
                    VALUES (?, ?, ?, ?)
                    """,
                    (record.id, node.name, node.protocol, json.dumps(node.config)),
                )

            # Ensure device status exists and update timestamp (robust approach)
            self.cursor.execute(
                """
                INSERT OR IGNORE INTO device_status (device_id) 
                VALUES (?)
                """,
                (record.id,),
            )
            self.cursor.execute(
                """
                UPDATE device_status 
                SET updated_at = CURRENT_TIMESTAMP 
                WHERE device_id = ?
                """,
                (record.id,),
            )

            # Commit transaction
            self.conn.commit()
            logger.info(f"Successfully updated energy meter '{record.name}' with ID {record.id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to update energy meter '{record.name}' with ID {record.id}: {e}")
            self.conn.rollback()
            return False

    def delete_energy_meter(self, record: EnergyMeterRecord) -> bool:
        """
        Deletes an energy meter and all its associated data from the database.

        Uses database transactions to ensure atomicity. The foreign key cascade
        constraints automatically remove all associated nodes and device status
        when the device is deleted.

        Args:
            record (EnergyMeterRecord): Energy meter record to delete.
                Must have a valid id field to identify the record.

        Returns:
            bool: True if deletion successful, False otherwise.

        Note:
            This operation cascades to delete:
            - All associated node records (nodes table)
            - Device status record (device_status table)
        """

        logger = LoggerManager.get_logger(__name__)

        if record.id is None:
            logger.error("Cannot delete energy meter: record ID is required")
            return False

        try:
            # Begin transaction
            self.cursor.execute("BEGIN TRANSACTION")

            # Delete the device (nodes will be cascade deleted due to foreign key)
            self.cursor.execute("DELETE FROM devices WHERE id = ?", (record.id,))

            if self.cursor.rowcount == 0:
                logger.warning(f"No energy meter found with ID {record.id}")
                self.conn.rollback()
                return False

            # Commit transaction
            self.conn.commit()
            logger.info(f"Successfully deleted energy meter '{record.name}' with ID {record.id}")
            return True

        except Exception as e:
            logger.exception(f"Failed to delete energy meter '{record.name}' with ID {record.id}: {e}")
            self.conn.rollback()
            return False

    def get_all_energy_meters(self) -> List[EnergyMeterRecord]:
        """
        Retrieves all energy meters from the database, including their associated nodes.

        Returns:
            List[EnergyMeterRecord]: A list of fully populated energy meter records.
        """

        logger = LoggerManager.get_logger(__name__)

        meters: List[EnergyMeterRecord] = []

        try:
            self.cursor.execute(
                """
                SELECT id, name, protocol, device_type, meter_options, communication_options
                FROM devices
            """
            )
            device_rows = self.cursor.fetchall()

            for device_row in device_rows:
                device_id, name, protocol, device_type, meter_opts_str, comm_opts_str = device_row

                self.cursor.execute(
                    """
                    SELECT name, protocol, config FROM nodes WHERE device_id = ?
                """,
                    (device_id,),
                )
                node_rows = self.cursor.fetchall()

                nodes = set()
                for node_name, node_protocol, config_str in node_rows:
                    node = NodeRecord(device_id=device_id, name=node_name, protocol=node_protocol, config=json.loads(config_str))
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

    def update_device_connection_status(self, device_id: int, status: bool) -> bool:
        """
        Updates device connection timestamps.

        Args:
            device_id (int): Device identifier
            status (bool): True for connected, False for disconnected

        Returns:
            bool: True if successful, False otherwise
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            conn_parameter = "on" if status else "off"

            self.cursor.execute(
                f"""
                UPDATE device_status 
                SET connection_{conn_parameter}_datetime = CURRENT_TIMESTAMP
                WHERE device_id = ?
                """,
                (device_id,),
            )

            self.conn.commit()
            return True

        except Exception as e:
            logger.exception(f"Failed to update connection status for device {device_id}: {e}")
            return False
