###########EXTERNAL IMPORTS############

import logging
from colorama import Fore, Style
from typing import Dict, Optional

#######################################

#############LOCAL IMPORTS#############

#######################################


class ColoredFormatter(logging.Formatter):
    """
    Custom log formatter that adds color to log messages based on severity level.
    """

    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA + Style.BRIGHT,
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Applies color to the log message based on its severity level,
        then delegates formatting to the parent class.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted, colorized log message.
        """

        color = self.COLORS.get(record.levelno, "")
        reset = Style.RESET_ALL
        record.msg = f"{color}{record.msg}{reset}"
        return super().format(record)


class LoggerManager:
    """
    Centralized logger manager that provides consistent loggers across the application,
    with optional colored output for better readability in the terminal.
    """

    DEFAULT_LEVEL = logging.INFO
    FORMATTER = ColoredFormatter("[%(name)s] [%(levelname)s] %(message)s")
    loggers: Dict[str, logging.Logger] = {}

    @staticmethod
    def init():
        """
        Disables all existing loggers from third-party libraries except those explicitly created by LoggerManager.
        """

        for name, logger in logging.root.manager.loggerDict.items():
            if name not in LoggerManager.loggers and isinstance(logger, logging.Logger):
                logger.disabled = True
                logger.setLevel(logging.CRITICAL + 1)
                logger.handlers.clear()

    @staticmethod
    def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
        """
        Returns a logger with the specified name and log level.
        If the logger was already created, returns the existing instance.

        Args:
            name (str): Name of the logger (usually `__name__`).
            level (int, optional): Logging level (e.g., logging.DEBUG, logging.INFO).

        Returns:
            logging.Logger: Configured logger instance.
        """

        if name in LoggerManager.loggers:
            logger = LoggerManager.loggers[name]
            if level is not None:
                logger.setLevel(level)
            return logger

        logger = logging.getLogger(name)
        logger.setLevel(level if level is not None else LoggerManager.DEFAULT_LEVEL)
        logger.propagate = False

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(LoggerManager.FORMATTER)
            logger.addHandler(handler)

        LoggerManager.loggers[name] = logger
        return logger

    @staticmethod
    def set_level(name: str, level: int) -> None:
        """
        Updates the logging level for a specific logger.

        Args:
            name (str): Name of the logger to update.
            level (int): New logging level to apply.
        """

        logger = LoggerManager.get_logger(name)
        logger.setLevel(level)
