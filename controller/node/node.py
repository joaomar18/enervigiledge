###########EXTERNAL IMPORTS############

from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import NodeConfig, BaseNodeRecordConfig, NodeRecord
from controller.registry.node_type import TypeRegistry

#######################################


class Node:
    """
    Represents a data point or measurement within a device.

    A node encapsulates both the configuration (metadata, units, alarms) and
    the processor (value handling, statistics, formatting) for a single data point.
    Each node can represent measurements like voltage, current, power, or status indicators.

    The processor is automatically created based on the node's type
    and handles type-specific value processing, alarm checking, and data formatting.

    Args:
        configuration (NodeConfig): The node's configuration including name, type, units, and processing options.

    Attributes:
        config (NodeConfig): The node's configuration settings.
        processor (NodeProcessor): Type-specific processor for value handling (created automatically based on node type).
    """

    def __init__(self, configuration: NodeConfig):

        configuration.validate()
        self.config = configuration
        self.processor = TypeRegistry.get_type_plugin(configuration.type).node_processor_factory(configuration)

    def get_publish_format(self) -> Dict[str, Any]:

        return self.processor.create_publish_format()

    def get_additional_info(self) -> Dict[str, Any]:

        return self.processor.create_additional_info()

    def get_node_record(self) -> NodeRecord:
        """
        Converts the node instance into a serializable NodeRecord object for database storage.

        Creates a record containing all node configuration, attributes, and metadata
        that can be persisted to the database and later reconstructed.

        Returns:
            NodeRecord: A representation of the current node suitable for database operations.
        """

        base_config = BaseNodeRecordConfig(
            enabled=self.config.enabled,
            type=self.config.type,
            unit=self.config.unit,
            publish=self.config.publish,
            calculated=self.config.calculated,
            custom=self.config.custom,
            decimal_places=self.config.decimal_places,
            logging=self.config.logging,
            logging_period=self.config.logging_period,
            min_alarm=self.config.min_alarm,
            max_alarm=self.config.max_alarm,
            min_alarm_value=self.config.min_alarm_value,
            max_alarm_value=self.config.max_alarm_value,
            is_counter=self.config.is_counter,
            counter_mode=self.config.counter_mode,
        )

        configuration = base_config.get_config()
        attributes = self.config.attributes.get_attributes()
        return NodeRecord(device_id=None, name=self.config.name, protocol=self.config.protocol, config=configuration, attributes=attributes)


###########     P R O T O C O L     S P E C I F I C     N O D E S     ###########


# Modbus RTU Node
class ModbusRTUNode(Node):
    """
    Specialized node for Modbus RTU communication protocol.

    Extends the base Node class to include Modbus-specific functionality
    such as register addressing and connection state tracking.

    Args:
        configuration (NodeConfig): The node's configuration including name, type, units, and processing options.
        register (int): The Modbus register address for this data point.
    """

    def __init__(self, configuration: NodeConfig, register: int):
        super().__init__(configuration=configuration)
        self.register = register
        self.connected = False

    def set_connection_state(self, state: bool):
        """
        Updates the connection state of this Modbus node.

        Args:
            state (bool): True if the node is connected and communicating, False otherwise.
        """
        self.connected = state

    def get_additional_info(self) -> Dict[str, Any]:

        additional_info = self.processor.create_additional_info()
        additional_info["register"] = self.register
        return additional_info

    def get_node_record(self) -> NodeRecord:
        """
        Converts the Modbus RTU node instance into a serializable NodeRecord object.

        Extends the base implementation to include Modbus-specific register attribute.

        Returns:
            NodeRecord: A representation of the current node for database storage.
        """

        node_record = super().get_node_record()
        node_record.config["register"] = self.register

        return node_record


# OPC UA Node
class OPCUANode(Node):
    """
    Specialized node for OPC UA communication protocol.

    Extends the base Node class to include OPC UA-specific functionality
    such as node ID addressing and connection state tracking.

    Args:
        configuration (NodeConfig): The node's configuration including name, type, units, and processing options.
        node_id (str): The OPC UA node identifier for this data point.
    """

    def __init__(self, configuration: NodeConfig, node_id: str):
        super().__init__(configuration=configuration)
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        """
        Updates the connection state of this OPC UA node.

        Args:
            state (bool): True if the node is connected and communicating, False otherwise.
        """
        self.connected = state

    def get_additional_info(self) -> Dict[str, Any]:

        additional_info = self.processor.create_additional_info()
        additional_info["node_id"] = self.node_id
        return additional_info

    def get_node_record(self) -> NodeRecord:
        """
        Converts the OPC UA node instance into a serializable NodeRecord object.

        Extends the base implementation to include OPC UA-specific node_id attribute.

        Returns:
            NodeRecord: A representation of the current node for database storage.
        """

        node_record = super().get_node_record()
        node_record.config["node_id"] = self.node_id

        return node_record


#################################################################################
