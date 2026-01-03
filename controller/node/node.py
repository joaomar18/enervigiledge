###########EXTERNAL IMPORTS############

from typing import Dict, Any

#######################################

#############LOCAL IMPORTS#############

from model.controller.node import NodeConfig, BaseNodeRecordConfig, NodeRecord, BaseNodeProtocolOptions
from model.controller.protocol.modbus_rtu import ModbusRTUNodeOptions
from model.controller.protocol.opc_ua import OPCUANodeOptions
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

    def get_extended_info(self) -> Dict[str, Any]:
        """Returns merged processor and protocol-specific extended information metadata."""

        return {**self.processor.create_extended_info(), "protocol_type": self.protocol_options.get_options().get("type") or "Not found"}

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
            min_warning=self.config.min_warning,
            max_warning=self.config.max_warning,
            min_warning_value=self.config.min_warning_value,
            max_warning_value=self.config.max_warning_value,
            is_counter=self.config.is_counter,
            counter_mode=self.config.counter_mode,
        )

        return NodeRecord(
            device_id=None,
            name=self.config.name,
            protocol=self.config.protocol,
            config=base_config,
            protocol_options=self.protocol_options,
            attributes=self.config.attributes,
        )


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

    MAX_NUMBER_FAILS = 3

    def __init__(self, configuration: NodeConfig, protocol_options: ModbusRTUNodeOptions):
        super().__init__(configuration=configuration, protocol_options=protocol_options)
        self.options = protocol_options
        self.connected = False
        self.enable_batch_read = True
        self.number_fails = 0

    def set_connection_state(self, state: bool):
        """
        Update the node's connection state and adjust its failure counter based on the result.

        Args:
            state (bool): True if the node read succeeded, False if it failed.
        """

        if state:
            self.reset_fails()
        elif not state and self.enable_batch_read:
            self.increment_fails()

        self.connected = state

    def increment_fails(self) -> None:
        """Increase failure count and disable batch reading if limit exceeded."""

        self.number_fails += 1
        if self.number_fails > ModbusRTUNode.MAX_NUMBER_FAILS:
            self.enable_batch_read = False

    def reset_fails(self) -> None:
        """Reset the failure counter and re-enable batch reading."""

        self.number_fails = 0
        self.enable_batch_read = True


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

    MAX_NUMBER_FAILS = 3

    def __init__(self, configuration: NodeConfig, protocol_options: OPCUANodeOptions):
        super().__init__(configuration=configuration, protocol_options=protocol_options)
        self.options = protocol_options
        self.connected = False
        self.enable_batch_read = True
        self.number_fails = 0

    def set_connection_state(self, state: bool):
        """
        Update the node's connection state and adjust its failure counter based on the result.

        Args:
            state (bool): True if the node read succeeded, False if it failed.
        """

        if state:
            self.reset_fails()
        elif not state and self.enable_batch_read:
            self.increment_fails()

        self.connected = state

    def increment_fails(self) -> None:
        """Increase failure count and disable batch reading if limit exceeded."""

        self.number_fails += 1
        if self.number_fails > OPCUANode.MAX_NUMBER_FAILS:
            self.enable_batch_read = False

    def reset_fails(self) -> None:
        """Reset the failure counter and re-enable batch reading."""

        self.number_fails = 0
        self.enable_batch_read = True


#################################################################################
