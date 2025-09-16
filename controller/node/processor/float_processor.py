###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.numeric_processor import NumericNodeProcessor
from controller.types.node import NodeConfig

#######################################


class FloatNodeProcessor(NumericNodeProcessor[float]):

    @property
    def ZERO(self) -> float:
        return 0.0

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=float)
