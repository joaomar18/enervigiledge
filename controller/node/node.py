###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

from controller.types.node import NodeConfig, BaseNodeRecordConfig, NodeRecord
from controller.registry.node_type import TypeRegistry

#######################################


class Node:

    def __init__(self, configuration: NodeConfig):

        configuration.validate()
        self.config = configuration
        self.processor = TypeRegistry.get_type_plugin(configuration.type).node_processor_factory(configuration)

    def get_node_record(self) -> NodeRecord:

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
            incremental_node=self.config.incremental_node,
            positive_incremental=self.config.positive_incremental,
            calculate_increment=self.config.calculate_increment,
        )

        configuration = base_config.get_config()
        attributes = self.config.attributes.get_attributes()
        return NodeRecord(device_id=None, name=self.config.name, protocol=self.config.protocol, config=configuration, attributes=attributes)


###########     P R O T O C O L     S P E C I F I C     N O D E S     ###########


# Modbus RTU Node
class ModbusRTUNode(Node):

    def __init__(self, configuration: NodeConfig, register: int):
        super().__init__(configuration=configuration)
        self.register = register
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state

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

    def __init__(self, configuration: NodeConfig, node_id: str):
        super().__init__(configuration=configuration)
        self.node_id = node_id
        self.connected = False

    def set_connection_state(self, state: bool):
        self.connected = state

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
