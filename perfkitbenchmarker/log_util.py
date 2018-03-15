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

from contextlib import contextmanager
import logging
import sys
import threading

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
    ERROR: logging.ERROR
}


class ThreadLogContext(object):
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


class PkbLogFilter(logging.Filter):
  """Filter that injects a thread's ThreadLogContext label into log messages.

  Sets the LogRecord's pkb_label attribute with the ThreadLogContext label.
  """
  def filter(self, record):
    record.pkb_label = GetThreadLogContext().label
    return True


def ConfigureBasicLogging():
  """Initializes basic python logging before a log file is available."""
  logging.basicConfig(format='%(levelname)-8s %(message)s', level=logging.INFO)


def ConfigureLogging(stderr_log_level, log_path, run_uri,
                     file_log_level=logging.DEBUG):
  """Configure logging.

  Note that this will destroy existing logging configuration!

  This configures python logging to emit messages to stderr and a log file.

  Args:
    stderr_log_level: Messages at this level and above are emitted to stderr.
    log_path: Path to the log file.
    run_uri: A string containing the run_uri to be appended to the log prefix
      labels.
    file_log_level: Messages at this level and above are written to the log
      file.
  """
  # Build the format strings for the stderr and log file message formatters.
  stderr_format = ('%(asctime)s {} %(threadName)s %(pkb_label)s'
                   '%(levelname)-8s %(message)s').format(run_uri)
  stderr_color_format = ('%(log_color)s%(asctime)s {} %(threadName)s '
                         '%(pkb_label)s%(levelname)-8s%(reset)s '
                         '%(message)s').format(run_uri)
  file_format = ('%(asctime)s {} %(threadName)s %(pkb_label)s'
                 '%(filename)s:%(lineno)d %(levelname)-8s %(message)s')
  file_format = file_format.format(run_uri)

  # Reset root logger settings.
  logger = logging.getLogger()
  logger.handlers = []
  logger.setLevel(logging.DEBUG)

  # Initialize the main thread's ThreadLogContext. This object must be
  # initialized to use the PkbLogFilter, and it is used to derive the
  # ThreadLogContext of other threads started through vm_util.RunThreaded.
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
  logging.info('Verbose logging to: %s', log_path)
  handler = logging.FileHandler(filename=log_path)
  handler.addFilter(PkbLogFilter())
  handler.setLevel(file_log_level)
  handler.setFormatter(logging.Formatter(file_format))
  logger.addHandler(handler)
  logging.getLogger('requests').setLevel(logging.ERROR)
