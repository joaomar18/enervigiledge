###########EXTERNAL IMPORTS############

from typing import Dict, Any, Optional
from datetime import datetime

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.processor import NodeProcessor
from controller.types.node import NodeConfig
from controller.exceptions import NotImplemeted

#######################################


class StringNodeProcessor(NodeProcessor[str]):

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=str)

    def check_alarms(self, value: str) -> None:
        raise NotImplemeted(f"check_alarms method is not implemented for string nodes")

    def set_value(self, value: Optional[str]) -> None:

        if not super().prepare_set_value(value) or value is None:  # Node disabled or value is None
            return

        self.value = value

    def get_publish_format(self, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output["value"] = self.value
        return super().get_publish_format(additional_data=output)

    def submit_log(self, date_time: datetime, additional_data: Dict[str, Any] = {}) -> Dict[str, Any]:

        output = additional_data.copy()
        output["value"] = self.value
        return super().submit_log(date_time=date_time, additional_data=output)
