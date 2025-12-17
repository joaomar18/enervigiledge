###########EXTERNAL IMPORTS############

#######################################

#############LOCAL IMPORTS#############

#######################################

##########     M E T E R     E X C E P T I O N S     ##########


class MeterError(Exception):
    """Raised when there is an error on the meter configuration"""

    pass


class UnitError(Exception):
    """Raised when a unit configuration or value is invalid."""

    pass


class NodeUnknownError(Exception):
    """Raised when a requested node does not exist or is unknown."""

    pass


class NodeMissingError(Exception):
    """Raised when a required node is missing during validation or runtime."""

    pass


class NodeInvalidOptionError(Exception):
    """
    Raised when a node is configured with an invalid or unsupported option.
    """

    pass


class LoggingPeriodError(Exception):
    """Raised when at least two nodes of the same type have different logging periods."""

    pass
