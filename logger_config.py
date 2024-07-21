#!/usr/bin/env python3
import logging 

class LevelRangeFilter(logging.Filter):
    """
    A logging filter that allows messages within a specified range of logging levels to pass through.

    Attributes:
        min_level (int): The minimum logging level that the filter allows.
        max_level (int): The maximum logging level that the filter allows.
    """
    def __init__(self, min_level, max_level):
        """
        Initializes the filter with minimum and maximum logging levels.

        Args:
            min_level (int): The minimum logging level.
            max_level (int): The maximum logging level.
        """
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level

    def filter(self, record):
        """
        Determines if the specified record should be logged based on its level.

        Args:
            record (LogRecord): The log record to be checked.

        Returns:
            bool: True if the record's level is within the specified range, False otherwise.
        """
        # Filter records that are not in the specified level range
        return self.min_level <= record.levelno <= self.max_level


# Create a logger for this module
main_logger = logging.getLogger(__name__)

# Set the logging level for the logger
main_logger.setLevel(logging.DEBUG)
# Setting the logger level to DEBUG means that all log messages, regardless of their severity, will be processed and output by this logger.

# Create handlers
success_handler = logging.FileHandler('etl_success.log')
debug_handler = logging.FileHandler('etl_debug.log')
error_handler = logging.FileHandler('etl_errors.log')
console_handler = logging.StreamHandler()  # This handler will send logs to the console

# Create formatters and add them to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
success_handler.setFormatter(formatter)
debug_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Create filters to the handlers
success_filter = LevelRangeFilter(logging.INFO, logging.WARNING)
error_filter = LevelRangeFilter(logging.WARNING, logging.CRITICAL)
debug_filter = LevelRangeFilter(logging.DEBUG, logging.CRITICAL)
console_filter = LevelRangeFilter(logging.INFO, logging.CRITICAL)

# Add filters to the handlers
success_handler.addFilter(success_filter)
error_handler.addFilter(error_filter)
debug_handler.addFilter(debug_filter)
console_handler.addFilter(console_filter)

# Clear any existing handlers attached to the logger
main_logger.handlers.clear()

# check if the logger currently has any handlers attached to it.
# logger.handlers will return a list of handlers attached to logger
if not main_logger.handlers:  #If this list is empty  it means no handlers are currently attached to the logger.
    # Add handlers to the logger
    main_logger.addHandler(success_handler)
    main_logger.addHandler(debug_handler)
    main_logger.addHandler(error_handler)
    main_logger.addHandler(console_handler)

# Disable propagation to avoid double logging
main_logger.propagate = False

