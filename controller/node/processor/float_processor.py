###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.numeric_processor import NumericNodeProcessor
from model.controller.node import NodeConfig

#######################################


class FloatNodeProcessor(NumericNodeProcessor[float]):
    """
    Processor for floating-point valued nodes.

    Handles floating-point measurements, calculated values, and energy readings
    with all numeric processing capabilities including decimal place formatting.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
    """

    @property
    def ZERO(self) -> float:
        """
        Returns the zero value for floating-point operations.

        Returns:
            float: Zero value (0.0).
        """
        return 0.0

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=float)
