# Python Logging Module

## Overview

Logging is a crucial part of any application, allowing developers to track events that happen when the software runs. 
The Python `logging` module provides a way to configure and use loggers to capture these events.

## Importing the Logging Module

First, you need to import the `logging` module:

```python
import logging
```

## Basic Configuration

To configure the logging system, you can use the logging.basicConfig method. This method allows you to set the log level, format, and other basic configurations

**Example**

```python
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

This function is suitable for quick setups, especially in smaller scripts or when you are just starting to add logging to your application.

It does not write logs to a file by default. It only sets up the basic configuration for how logging messages should be displayed to the console.


**Parameters**

- `level`: Sets the threshold for the logger. Messages which are less severe than this level will be ignored. Common levels include DEBUG, INFO, WARNING, ERROR, and CRITICAL.
- `format`: Allows you to customize the log message format. Here are some common placeholders:
  - `%(asctime)s`: The time the log message was created.
  - `%(levelname)s`: The log level of the message.
  - `%(message)s`: The actual log message.
 
## Logging Levels

Python's logging module defines the following standard levels, each corresponding to a different severity of log messages:

- `DEBUG`: Detailed information, typically of interest only when diagnosing problems.
- `INFO`: Confirmation that things are working as expected.
- `WARNING`: An indication that something unexpected happened, or indicative of some problem in the near future (e.g., 'disk space low'). The software is still working as expected.
- `ERROR`: Due to a more serious problem, the software has not been able to perform some function.
- `CRITICAL`: A very serious error, indicating that the program itself may be unable to continue running.

**Example**:

```python
logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')
```

## Loggers

### Definition

A logger is an object that is used to log messages. Loggers have a name, and they can be configured with different logging levels and handlers.

This method ensures that log messages from different modules can be distinguished easily in your logs, which is crucial for debugging and understanding the flow of your application.
It allows you to customize logging behavior for different parts of your application by configuring each logger independently.

### Creating and Using Loggers

You can create a logger using logging.getLogger(name). If no name is provided, it returns the root logger. Typically, you pass __name__ as the name so that the logger's name matches the module's name.

```python
import logging

# Create a custom logger
logger = logging.getLogger(__name__)

# Set the logging level (optional, the default is WARNING)
logger.setLevel(logging.DEBUG)

# Log messages
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')
```

## Handlers

### Definition

Handlers are responsible for sending the log messages (recorded by loggers) to the desired output destination, such as the console, a file, or a network socket.

### Types of Handlers

- `StreamHandler`: Sends log messages to the console (standard output).
- `FileHandler`: Sends log messages to a file.
- `NullHandler`: Discards all log messages (used to avoid the "No handlers could be found" warning).

### Adding Handlers to Loggers

A logger can have multiple handlers, and each handler can be configured independently. Here’s how to add handlers to a logger:

```python
import logging

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
console_handler = logging.StreamHandler()  # Handler to send log messages to the console
file_handler = logging.FileHandler('app.log')  # Handler to send log messages to a file

# Set the level for handlers (optional, defaults to NOTSET)
console_handler.setLevel(logging.WARNING)
file_handler.setLevel(logging.ERROR)

# Create formatters and add them to the handlers
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Example usage
logger.debug('This is a debug message')
logger.info('This is an info message')
logger.warning('This is a warning message')
logger.error('This is an error message')
logger.critical('This is a critical message')
```







