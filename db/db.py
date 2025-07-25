############### EXTERNAL IMPORTS ################

import sqlite3
import json
from typing import List, Dict, Set, Any, Optional
from dataclasses import dataclass

#################################################

############### LOCAL IMPORTS ###################

from util.debug import LoggerManager

#################################################


@dataclass
class NodeRecord:
    """
    Represents a configuration record for a data point (node) associated with an energy meter.

    This class supports nodes from various protocols such as Modbus RTU, OPC UA, or protocol-independent
    configurations (e.g., calculated or internal logic nodes). It stores all relevant configuration details.

    Attributes:
        device_id (int): Unique identifier of the parent device in the database.
        name (str): Unique name identifying the node.
        protocol (str): Communication protocol associated with the node.
            Valid values:
                - "modbus_rtu": Node is read via Modbus RTU.
                - "opcua": Node is read via OPC UA.
                - "none": Node has no communication protocol (e.g., calculated or virtual nodes).
        config (Dict[str, Any]): Node-specific configuration and metadata.

            Common fields:
                - type (str): Data type of the node (e.g., "float", "int").
                - unit (str): Unit of measurement (e.g., "V", "A").
                - enabled (bool): Whether the node is enabled for reading or calculation.
                - publish (bool): Whether the value should be published via MQTT.
                - calculated (bool): Whether the value is calculated instead of read.
                - custom (bool): Whether the value is a custom node (custom name or unit).
                - logging (bool): Whether the value should be logged.
                - logging_period (int): Logging interval in minutes.
                - min_alarm (bool): Whether to enable a minimum threshold alarm.
                - max_alarm (bool): Whether to enable a maximum threshold alarm.
                - min_alarm_value (float): Minimum value threshold for alarm triggering.
                - max_alarm_value (float): Maximum value threshold for alarm triggering.

            Protocol-specific fields:
                - For "modbus_rtu": register (int): Modbus register address.
                - For "opcua": node_id (str): OPC UA Node ID used to access the value.
    """

    device_id: Optional[int]
    name: str
    protocol: str
    config: Dict[str, Any]

    def __eq__(self, other):
        if not isinstance(other, NodeRecord):
            return False
        return (self.device_id, self.name, self.protocol) == (other.device_id, other.name, other.protocol)

    def __hash__(self):
        return hash((self.device_id, self.name, self.protocol))


@dataclass
class EnergyMeterRecord:
    """
    Represents the full configuration of an energy meter for persistence in SQLite.

    Attributes:
        id (int | None): id of the device, when inserting leave None
        name (str): Human-readable name of the energy meter (e.g., "OR-WE-516 Energy Meter").
        protocol (str): Communication protocol used by the meter (e.g., "modbus_rtu", "opcua").
        device_type (str): Type of the meter, typically based on electrical configuration (e.g., "three_phase", "single_phase").
        meter_options (Dict[str, Any]): Configuration options related to what the meter should read or expose (e.g., frequency, energy direction).
        connection_options (Dict[str, Any]): Protocol-specific connection settings (e.g., slave ID, port, URL, baudrate).
    """

    name: str
    protocol: str
    device_type: str
    meter_options: Dict[str, Any]
    connection_options: Dict[str, Any]
    nodes: Set[NodeRecord]
    id: Optional[int] = None


class SQLiteDBClient:
    """
    Client for storing device and node configurations using SQLite.

    This class provides methods to:
        - Create necessary tables for storing device and node configurations.
        - Insert or update device and node records.
        - Check database file accessibility.
        - Cleanly close the database connection.

    Intended for use as a local configuration store in industrial monitoring platforms
    like ENERVIGIL.

    Attributes:
        db_path (str): Filesystem path to the SQLite database file.
        conn (sqlite3.Connection): Connection object to the SQLite database.
        cursor (sqlite3.Cursor): Cursor object for executing SQL commands.
    """

    def __init__(self, db_path: str = "config.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """
        Creates the required SQLite tables for storing energy meter configurations.

        Tables created:
            - devices: Stores energy meter-level configuration including protocol, meter options, and connection options.
            - nodes: Stores individual data points (nodes) associated with each device, including protocol-specific and common configuration.

        Notes:
            - Each node is linked to a device via a foreign key (device_id).
            - Devices use an auto-incrementing primary key (id).
            - Nodes are automatically deleted if their parent device is removed (ON DELETE CASCADE).
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
                    connection_options TEXT NOT NULL
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
            self.conn.commit()
        except Exception as e:
            logger.exception(f"Failed to create tables: {e}")

    def insert_energy_meter(self, record: EnergyMeterRecord) -> int | None:
        """
        Inserts a new energy meter (device) into the database along with all associated nodes.

        Args:
            record (EnergyMeterRecord): Structured energy meter data including nodes.

        Returns:
            int | None: The ID of the inserted device if successful, None otherwise.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            self.cursor.execute(
                """
                INSERT INTO devices (name, protocol, device_type, meter_options, connection_options)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record.name, record.protocol, record.device_type, json.dumps(record.meter_options), json.dumps(record.connection_options)),
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

            self.conn.commit()
            return device_id

        except Exception as e:
            logger.exception(f"Failed to insert energy meter '{record.name}': {e}")
            self.conn.rollback()
            return None

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
                SELECT id, name, protocol, device_type, meter_options, connection_options
                FROM devices
            """
            )
            device_rows = self.cursor.fetchall()

            for device_row in device_rows:
                device_id, name, protocol, device_type, meter_opts_str, conn_opts_str = device_row

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
                    protocol=protocol,
                    device_type=device_type,
                    meter_options=json.loads(meter_opts_str),
                    connection_options=json.loads(conn_opts_str),
                    nodes=nodes,
                )

                meters.append(meter)

        except Exception as e:
            logger.exception(f"Failed to retrieve energy meters: {e}")

        return meters

    def close(self):
        """
        Closes the SQLite connection.
        Should be called during application shutdown.
        """

        logger = LoggerManager.get_logger(__name__)

        try:
            self.conn.close()
        except Exception as e:
            logger.exception(f"Failed to close SQLite connection: {e}")
