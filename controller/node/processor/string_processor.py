###########EXTERNAL IMPORTS############

from typing import Dict, Any, Optional
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.processor import NodeProcessor
from model.controller.node import NodeConfig

#######################################


class StringNodeProcessor(NodeProcessor[str]):
    """
    Processor for string-valued nodes.

    Handles text-based status information, device names, and other string data.
    Alarm checking is not implemented for string values.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
    """

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=str)

    def check_alarms(self, value: str) -> None:
        """
        Alarm checking is not implemented for string nodes.

        Args:
            value (str): The string value (ignored).

        Raises:
            NotImplementedError: Always raised as alarms are not supported for string values.
        """
        raise NotImplementedError(f"check_alarms method is not implemented for string nodes")

    def set_value(self, value: Optional[str]) -> None:
        """
        Sets the string value after basic validation.

        Args:
            value (Optional[str]): The string value to set, or None to clear.
        """

        if not super().prepare_set_value(value) or value is None:  # Node disabled or value is None
            return

        self.value = value

    def create_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Builds the publish payload for the node, including its current value.
        """

        output = additional_data.copy()
        output["value"] = self.value
        return super().create_publish_format(additional_data=output)

    def create_extended_info(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Extends the base node information with additional metadata.
        """

        return super().create_extended_info(additional_data=additional_data)

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Prepares string data for database logging.

        Args:
            date_time (datetime): The end time for this logging period.
            additional_data (Dict[str, Any]): Additional data to include in the log.

        Returns:
            Dict[str, Any]: Log data including the string value.
        """

        output = additional_data.copy()
        output["value"] = self.value
        return super().submit_log(date_time=date_time, additional_data=output)
