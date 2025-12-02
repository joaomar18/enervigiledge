###########EXTERNAL IMPORTS############

from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import NodeConfig, BaseNodeRecordConfig, NodeRecord, BaseNodeProtocolOptions
from model.controller.protocol.modbus_rtu import ModbusRTUNodeOptions
from model.controller.protocol.opcua import OPCUANodeOptions
from controller.registry.node_type import TypeRegistry

#######################################


class Node:
    """
    Represents a single data point within a device.

    A node holds its configuration, protocol-specific communication options,
    and a type-specific processor responsible for handling value decoding,
    formatting, alarms, and statistics.

    The processor is created automatically based on the node's internal type.

    Args:
        configuration (NodeConfig): Node configuration including metadata,
            alarms, logging options, and internal type.
        protocol_options (BaseNodeProtocolOptions): Protocol-specific options
            required to read or compute the node's value.

    Attributes:
        config (NodeConfig): The node's configuration.
        protocol_options (BaseNodeProtocolOptions): Communication or calculation
            settings tied to the node's protocol.
        processor (NodeProcessor): Type-specific processor created from the
            internal node type.
    """

    def __init__(self, configuration: NodeConfig, protocol_options: BaseNodeProtocolOptions):

        configuration.validate()
        self.config = configuration
        self.protocol_options = protocol_options
        self.processor = TypeRegistry.get_type_plugin(configuration.type).node_processor_factory(configuration)

    def get_publish_format(self) -> Dict[str, Any]:
        """Returns the formatted value payload for publishing."""

        return self.processor.create_publish_format()

    def get_additional_info(self) -> Dict[str, Any]:
        """Returns merged processor and protocol-specific metadata."""

        return {
            **self.processor.create_additional_info(),
            **self.protocol_options.get_options(),
        }

    def get_node_record(self) -> NodeRecord:
        """
        Converts the node instance into a serializable NodeRecord object for database storage.

        Creates a record containing all node configuration, protocol options, attributes, and metadata
        that can be persisted to the database and later reconstructed.

        Returns:
            NodeRecord: A representation of the current node suitable for database operations.
        """

        base_config = BaseNodeRecordConfig(
            enabled=self.config.enabled,
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
        protocol_options = self.protocol_options.get_options()
        attributes = self.config.attributes.get_attributes()
        return NodeRecord(device_id=None, name=self.config.name, protocol=self.config.protocol, config=configuration, protocol_options=protocol_options, attributes=attributes)


###########     P R O T O C O L     S P E C I F I C     N O D E S     ###########


# Modbus RTU Node
class ModbusRTUNode(Node):
    """
    Node implementation for the Modbus RTU protocol.

    Extends the base Node with Modbus-specific communication options, such as
    register addressing, data type decoding, and connection state tracking.
    The protocol options are also exposed as `self.options` for convenient
    access to Modbus-specific fields.

    Args:
        configuration (NodeConfig): Runtime configuration for the node.
        options (ModbusRTUNodeOptions): Modbus-specific options
            including register address, data type, and endianness.
    """


    def __init__(self, configuration: NodeConfig, protocol_options: ModbusRTUNodeOptions):
        super().__init__(configuration=configuration, protocol_options=protocol_options)
        self.options = protocol_options
        self.connected = False

    def set_connection_state(self, state: bool):
        """
        Updates the connection state of this Modbus node.

        Args:
            state (bool): True if the node is connected and communicating, False otherwise.
        """
        self.connected = state


# OPC UA Node
class OPCUANode(Node):
    """
    Node implementation for the OPC UA protocol.

    Extends the base Node with OPC UA-specific communication options, such as
    NodeId addressing and connection state tracking. The protocol options are
    also exposed as `self.options` for convenient access to OPC UA–specific fields.

    Args:
        configuration (NodeConfig): Runtime configuration for the node.
        protocol_options (OPCUANodeOptions): OPC UA–specific options including
            the NodeId and expected data type.
    """

    def __init__(self, configuration: NodeConfig, protocol_options: OPCUANodeOptions):
        super().__init__(configuration=configuration, protocol_options=protocol_options)
        self.options = protocol_options
        self.connected = False

    def set_connection_state(self, state: bool):
        """
        Updates the connection state of this OPC UA node.

        Args:
            state (bool): True if the node is connected and communicating, False otherwise.
        """
        self.connected = state


#################################################################################
