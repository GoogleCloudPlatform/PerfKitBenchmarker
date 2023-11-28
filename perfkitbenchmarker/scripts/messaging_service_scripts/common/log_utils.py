"""Module containing util functions to set up loggers."""

import logging
from absl import flags

_LOG_FORMAT = '%(asctime)s @ %(filename)s:%(lineno)d - %(message)s'
_DEBUG = flags.DEFINE_bool(
    'debug', False, help='Logs messages to files for further debugging.'
)


def silence_log_messages_by_default():
  """Silence all logging messages, but those created with get_logger."""
  logging.basicConfig(handlers=(logging.NullHandler(),))


def get_logger(name, log_file):
  """Gets a new logger that outputs to a file."""
  if not _DEBUG.value:
    return logging.getLogger('')
  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)
  formatter = logging.Formatter(_LOG_FORMAT)
  fh = logging.FileHandler(log_file)
  fh.setFormatter(formatter)
  logger.addHandler(fh)
  return logger
