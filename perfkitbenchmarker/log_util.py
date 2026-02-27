# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utilities related to loggers and logging."""

import collections
from contextlib import contextmanager
import functools
import logging
from logging import handlers
import os
import re
import sys
import threading
import time
from absl import flags
from absl import logging as absl_logging

try:
  import colorlog
except ImportError:
  colorlog = None


DEBUG = 'debug'
INFO = 'info'
WARNING = 'warning'
ERROR = 'error'
LOG_LEVELS = {
    DEBUG: logging.DEBUG,
    INFO: logging.INFO,
    WARNING: logging.WARNING,
    ERROR: logging.ERROR,
}

SHORT_LOGGER_NAME = 'short'

# Paths for log writing and exporting.
log_local_path = None
LOG_FILE_NAME = 'pkb.log'
SHORT_LOG_FILE_NAME = 'pkb_short.log'

DEFAULT_LOG_ROTATING_INTERVAL = 1
DEFAULT_LOG_ROTATING_UNIT = 'D'
DEFAULT_LOG_ROTATING_BACKUP_COUNT = 5


flags.DEFINE_enum(
    'log_level',
    INFO,
    list(LOG_LEVELS.keys()),
    'The log level to run at.',
)
flags.DEFINE_enum(
    'file_log_level',
    DEBUG,
    list(LOG_LEVELS.keys()),
    'Anything logged at this level or higher will be written to the log file.',
)
TRUNCATE_DUPLICATE_LOGS = flags.DEFINE_boolean(
    'truncate_duplicate_logs',
    True,
    'Whether to truncate duplicated logs. If True, consecutive identical logs '
    'will be truncated instead of showing their full content.'
)


class ThreadLogContext:
  """Per-thread context for log message prefix labels."""

  def __init__(self, thread_log_context=None):
    """Constructs a ThreadLogContext by copying a previous ThreadLogContext.

    Args:
      thread_log_context: A ThreadLogContext for an existing thread whose state
        will be copied to initialize a ThreadLogContext for a new thread.
    """
    if thread_log_context:
      self._label_list = thread_log_context._label_list[:]
    else:
      self._label_list = []
    self._RecalculateLabel()

  @property
  def label(self):
    return self._label

  def _RecalculateLabel(self):
    """Recalculate the string label used to to prepend log messages.

    The label is the concatenation of all non-empty strings in the _label_list.
    """
    non_empty_string_list = [s for s in self._label_list if s]
    if len(non_empty_string_list):
      self._label = ' '.join(non_empty_string_list) + ' '
    else:
      self._label = ''

  @contextmanager
  def ExtendLabel(self, label_extension):
    """Extends the string label used to prepend log messages.

    Args:
      label_extension: A string appended to the end of the current label.
    """
    self._label_list.append(label_extension)
    self._RecalculateLabel()
    yield
    self._label_list.pop()
    self._RecalculateLabel()


class _ThreadData(threading.local):

  def __init__(self):
    self.pkb_thread_log_context = ThreadLogContext()


thread_local = _ThreadData()


def SetThreadLogContext(thread_log_context):
  """Set the current thread's ThreadLogContext object.

  Args:
    thread_log_context: A ThreadLogContext to be written to thread local
      storage.
  """
  thread_local.pkb_thread_log_context = thread_log_context


def GetThreadLogContext():
  """Get the current thread's ThreadLogContext object.

  Returns:
    The ThreadLogContext previously written via SetThreadLogContext.
  """
  return thread_local.pkb_thread_log_context


# Below patterns appear in common PKB IssueCommand messages but are not useful
# for deduplication.
_RETURN_CODE_PATTERN = re.compile(
    r'WallTime:[\d:.]+s,\s+CPU:[\d.]+s,\s+MaxMemory:\d+kb\s*?'
)
_SSH_PATTERN = re.compile(r'ssh \-A \-p.*? \-o ControlPersist=[\d]+m')
# If 1 character is 1 byte, then this is 100MB.
_MAX_LENGTH = 1024 * 1024 * 100
# Example: 0217 22:57:21
_FALLBACK_TIME_FORMAT = '%m%d %H:%M:%S'


class PkbLogRecord:
  """Logging record with some additional fields to dedupe."""

  def __init__(self, record: logging.LogRecord):
    self.record = record
    self._message = record.getMessage()
    self.duplicates = 0
    self.length = len(self._message)

  @functools.cached_property
  def message(self) -> str:
    """Returns the message of the log record."""
    if 'WallTime' in self._message:
      # Remove common timing noise.
      self._message = _RETURN_CODE_PATTERN.sub(
          '...',
          self._message,
      )
    return self._message

  @functools.cached_property
  def should_truncate(self) -> bool:
    """Returns whether the message should be truncated."""
    return (
        '\n' in self.message
        and self.length > 220
        and ('DO NOT DEDUPLICATE' not in self.message)
    )

  @property
  def is_too_long(self) -> bool:
    """Returns whether the message is too long to be stored in the queue."""
    return self.length > _MAX_LENGTH

  @functools.cached_property
  def truncated_message(self) -> str:
    """Returns the truncated message of the log record."""
    if 'ssh -A -p ' in self.message and '-o ControlPersist=' in self.message:
      # Remove common ssh noise.
      message = _SSH_PATTERN.sub(
          'ssh ...',
          self.message,
      )
    else:
      message = self.message
    if len(message) > 150:
      return message[:150] + '...'
    return message

  @functools.cached_property
  def created_time(self) -> str:
    """Returns the created time of the log record."""
    if hasattr(self.record, 'asctime'):
      return self.record.asctime
    # Convert nanoseconds to seconds
    seconds_timestamp = self.record.created
    return time.strftime(
        _FALLBACK_TIME_FORMAT, time.localtime(seconds_timestamp)
    )

  def __eq__(self, other: 'PkbLogRecord') -> bool:
    if type(self) != type(other):
      return NotImplemented
    return (
        self.message == other.message
        and self.record.pkb_label == other.record.pkb_label
    )


class PkbLogFilter(logging.Filter):
  """Filter that truncates duplicate messages & adds thread context.

  Sets the LogRecord's pkb_label attribute with the ThreadLogContext label.
  For each log, if it exactly matches a recent previous log, then the current
  log is truncated and marked as duplicated.
  """

  def __init__(self):
    super().__init__()
    self.max_length = 5
    self.last_records: collections.deque[PkbLogRecord] = collections.deque(
        maxlen=self.max_length
    )

  def filter(self, record: logging.LogRecord) -> bool:
    """Modifies the log record in-place to deduplicate and set pkb_label."""
    record.pkb_label = GetThreadLogContext().label
    pkb_record = PkbLogRecord(record)
    if not TRUNCATE_DUPLICATE_LOGS.value:
      return True
    if pkb_record.is_too_long:
      # Return early to avoid storing too large records in the queue.
      # Message will not be truncated.
      return True
    duplicate_record = None
    last_records: list[PkbLogRecord] = list(self.last_records)
    for last_record in last_records:
      if pkb_record == last_record:
        duplicate_record = last_record
        break
    if not duplicate_record:
      self.last_records.append(pkb_record)
      return True
    duplicate_record.duplicates += 1
    if duplicate_record.should_truncate:
      record.msg = (
          'Message from %s has been duplicated %s times. Truncating to:\n%s.'
      )
      record.args = (
          duplicate_record.created_time,
          duplicate_record.duplicates,
          duplicate_record.truncated_message,
      )
    return True


def ConfigureBasicLogging():
  """Initializes basic python logging before a log file is available."""
  logging.basicConfig(format='%(levelname)-8s %(message)s', level=logging.INFO)


def ConfigureLogging(
    stderr_log_level, logs_dir, run_uri, file_log_level=logging.DEBUG
):
  """Configure logging.

  Note that this will destroy existing logging configuration!

  This configures python logging to emit messages to stderr and a log file.

  Args:
    stderr_log_level: Messages at this level and above are emitted to stderr.
    logs_dir: Path to the directory where logs should be written.
    run_uri: A string containing the run_uri to be appended to the log prefix
      labels.
    file_log_level: Messages at this level and above are written to the log
      file.
  """
  # Set local log file path global variable so it can be used by PKB.
  global log_local_path
  log_local_path = os.path.join(logs_dir, LOG_FILE_NAME)

  # Build the format strings for the stderr and log file message formatters.
  stderr_format = (
      '%(asctime)s {} %(threadName)s %(pkb_label)s%(levelname)-8s %(message)s'
  ).format(run_uri)
  stderr_color_format = (
      '%(log_color)s%(asctime)s {} %(threadName)s '
      '%(pkb_label)s%(levelname)-8s%(reset)s '
      '%(message)s'
  ).format(run_uri)
  file_format = (
      '%(asctime)s {} %(threadName)s %(pkb_label)s'
      '%(filename)s:%(lineno)d %(levelname)-8s %(message)s'
  )
  file_format = file_format.format(run_uri)

  # Reset root logger settings.
  logger = logging.getLogger()
  logger.handlers = []
  logger.setLevel(logging.DEBUG)

  # Initialize the main thread's ThreadLogContext. This object must be
  # initialized to use the PkbLogFilter, and it is used to derive the
  # ThreadLogContext of other threads started through
  # background_tasks.RunThreaded.
  SetThreadLogContext(ThreadLogContext())

  # Add handler to output to stderr.
  handler = logging.StreamHandler()
  handler.addFilter(PkbLogFilter())
  handler.setLevel(stderr_log_level)
  if colorlog is not None and sys.stderr.isatty():
    formatter = colorlog.ColoredFormatter(stderr_color_format, reset=True)
    handler.setFormatter(formatter)
  else:
    handler.setFormatter(logging.Formatter(stderr_format))
  logger.addHandler(handler)

  # Add handler for output to log file.
  logging.info('Verbose logging to: %s', log_local_path)
  handler = handlers.TimedRotatingFileHandler(
      filename=log_local_path,
      when=DEFAULT_LOG_ROTATING_UNIT,
      interval=DEFAULT_LOG_ROTATING_INTERVAL,
      backupCount=DEFAULT_LOG_ROTATING_BACKUP_COUNT,
  )
  handler.addFilter(PkbLogFilter())
  handler.setLevel(file_log_level)
  handler.setFormatter(logging.Formatter(file_format))
  logger.addHandler(handler)
  logging.getLogger('requests').setLevel(logging.ERROR)


def ConfigureDedupeLogging():
  """Initializes logging with dedupe only filtering."""
  logger = logging.getLogger()
  for handler in logger.handlers:
    handler.addFilter(PkbLogFilter())


def ConfigureShortLogging(logs_dir, file_log_level=logging.DEBUG):
  """Initializes short logging."""
  short_logger = logging.getLogger(SHORT_LOGGER_NAME)
  short_logger.setLevel(file_log_level)
  # Don't propagate logs to parent loggers. Callsites will determine when to log
  # to root (pkb.log / stdout / stderr) and when to log to the short log.
  short_logger.propagate = False
  handler = handlers.TimedRotatingFileHandler(
      filename=os.path.join(logs_dir, SHORT_LOG_FILE_NAME),
      when=DEFAULT_LOG_ROTATING_UNIT,
      interval=DEFAULT_LOG_ROTATING_INTERVAL,
      backupCount=DEFAULT_LOG_ROTATING_BACKUP_COUNT,
  )
  handler.setLevel(file_log_level)
  handler.setFormatter(absl_logging.PythonFormatter())
  short_logger.addHandler(handler)


def LogToShortLog(message: str, *args, **kwargs):
  """Logs the message to the short log."""
  if 'stacklevel' in kwargs:
    kwargs['stacklevel'] += 1
  else:
    kwargs['stacklevel'] = 2
  logging.getLogger(SHORT_LOGGER_NAME).info(message, *args, **kwargs)


def LogToShortLogAndRoot(message: str, *args, **kwargs):
  """Logs the message to both the short log and root (PKB/stdout)."""
  if 'stacklevel' in kwargs:
    kwargs['stacklevel'] += 1
  else:
    kwargs['stacklevel'] = 2
  logging.info(message, *args, **kwargs)
  LogToShortLog(message, *args, **kwargs)
