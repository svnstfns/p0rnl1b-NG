"""
Logging Configuration Module

Provides centralized logging configuration with support for 'origin' fields.
"""

import logging
from flask import request

class OriginFilter(logging.Filter):
    """
    Custom filter to add an 'origin' field to log records.
    """

    def __init__(self, origin):
        """
        Initialize the OriginFilter with a specific origin.

        Args:
            origin (str): The origin identifier for the logs.
        """
        super().__init__()
        self.origin = origin

    def filter(self, record):
        """
        Add the 'origin' field to the log record.

        Args:
            record (logging.LogRecord): The log record being processed.

        Returns:
            bool: True if the log record should be logged, False otherwise.
        """
        record.origin = self.origin
        return True


class CustomFormatter(logging.Formatter):
    """
    Custom formatter to include the 'origin' field in log messages.
    """

    def format(self, record):
        """
        Format the log record to include the 'origin' field.

        Args:
            record (logging.LogRecord): The log record being formatted.

        Returns:
            str: The formatted log message.
        """
        if not hasattr(record, "origin"):
            record.origin = "GENERAL"
        return super().format(record)


def configure_logging():
    """
    Configures the global logging settings.

    Returns:
        logging.Logger: The configured logger.
    """
    # Create the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Setup console handler with a custom formatter
    console_handler = logging.StreamHandler()
    formatter = CustomFormatter(
        '%(asctime)s %(origin)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Suppress unnecessary logs (e.g., Werkzeug logs)
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.ERROR)

    return logger
