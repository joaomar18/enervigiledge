###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

from controller.node.processor.numeric_processor import NumericNodeProcessor
from model.controller.node import NodeConfig

#######################################


class IntNodeProcessor(NumericNodeProcessor[int]):
    """
    Processor for integer-valued nodes.

    Handles integer measurements, counters, and calculated values with
    all numeric processing capabilities.

    Args:
        configuration (NodeConfig): Node configuration containing settings and metadata.
    """

    @property
    def ZERO(self) -> int:
        """
        Returns the zero value for integer operations.

        Returns:
            int: Zero value (0).
        """
        return 0

    def __init__(self, configuration: NodeConfig):
        super().__init__(configuration=configuration, value_type=int)
