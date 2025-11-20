###########EXTERNAL IMPORTS############

from typing import Dict, Any, Optional
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.processor import NodeProcessor
from model.controller.node import NodeConfig
from controller.exceptions import NotImplemeted

#######################################


class BoolNodeProcessor(NodeProcessor[bool]):
    """
    Processor for boolean-valued nodes.

    Handles boolean status indicators, switches, and digital inputs.
    Alarm checking is not implemented for boolean values.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
    """

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=bool)

    def check_alarms(self, value: bool) -> None:
        """
        Alarm checking is not implemented for boolean nodes.

        Args:
            value (bool): The boolean value (ignored).

        Raises:
            NotImplemeted: Always raised as alarms are not supported for boolean values.
        """
        raise NotImplemeted(f"check_alarms method is not implemented for bool nodes")

    def set_value(self, value: Optional[bool]) -> None:
        """
        Sets the boolean value after basic validation.

        Args:
            value (Optional[bool]): The boolean value to set, or None to clear.
        """

        if not super().prepare_set_value(value) or value is None:  # Node disabled or value is None
            return

        self.value = value

    def get_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output["value"] = self.value
        return super().create_publish_format(additional_data=output)

    def create_additional_info(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        return super().create_additional_info()

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Prepares boolean data for database logging.

        Args:
            date_time (datetime): The end time for this logging period.
            additional_data (Dict[str, Any]): Additional data to include in the log.

        Returns:
            Dict[str, Any]: Log data including the boolean value.
        """

        output = additional_data.copy()
        output["value"] = self.value
        return super().submit_log(date_time=date_time, additional_data=output)
