"""Configures and provides a logger for the application."""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
  """Configures and returns a logger instance.

  Args:
    name: The name of the logger.

  Returns:
    A configured logger instance.
  """
  logger = logging.getLogger(name)

  handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter(
      '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
      ' (%(filename)s:%(lineno)d)'
  )
  handler.setFormatter(formatter)

  logger.addHandler(handler)
  return logger


def set_logging_level(debug: bool) -> None:
  """Sets the logging level for the root logger.

  Args:
    debug: If True, sets the logging level to DEBUG, otherwise INFO.
  """
  if debug:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.INFO)
