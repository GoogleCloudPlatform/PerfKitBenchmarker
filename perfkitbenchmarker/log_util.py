# Copyright 2014 Google Inc. All rights reserved.
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

"""Utilities related to loggers and logging.
"""

import copy
import logging
import threading

DEBUG = 'debug'
INFO = 'info'
LOG_LEVELS = {
    DEBUG: logging.DEBUG,
    INFO: logging.INFO
}


thread_local = threading.local()


class ThreadLogInfo(object):
  """Per-thread log message prefix label configuration."""
  def __init__(self, thread_log_info=None):
    """Constructs a ThreadLogInfo by copying a previous ThreadLogInfo.

    Args:
      thread_log_info: A ThreadLogInfo for an existing thread whose state will
        be copied to initialize a ThreadLogInfo for a new thread.
    """
    if thread_log_info:
      self._label_list = copy.copy(thread_log_info._label_list)
    else:
      self._label_list = []
    self._RecalculateLabel()


  def _RecalculateLabel(self):
    """Recalculate the string label used to to prepend log messages.

    The label is the concatenation of all non-empty strings in the _label_list.
    """
    if len(self._label_list):
      self._label = ' '.join(filter(None, self._label_list)) + ' '
    else:
      self._label = ''


  def GetLabel(self):
    """Gets the current label.

    Returns:
      A string that prepends log messages.
    """
    return self._label


  def EnterLabelExtension(self, label_extension):
    """Extends the label until ExitLabelExtension is called.

    Args:
      label_extension: A string that is added to the current label, to be
        printed in each log message.
    """
    self._label_list.append(label_extension)
    self._RecalculateLabel()


  def ExitLabelExtension(self):
    """Removes the most recent label extension added by EnterLabelExtension.
    """
    self._label_list.pop()
    self._RecalculateLabel()


def SetThreadLogInfo(thread_log_info):
  """Set the current thread's ThreadLogInfo object.

  Args:
    thread_log_info: A ThreadLogInfo to be written to thread local storage.
  """
  thread_local.pkb_thread_log_info = thread_log_info


def GetThreadLogInfo():
  """Get the current thread's ThreadLogInfo object.

  Returns:
    The ThreadLogInfo previously written via SetThreadLogInfo.
  """
  return thread_local.pkb_thread_log_info


class LabelExtender(object):
  """Manages a ThreadLogInfo EnterLabelExtension/ExitLabelExtension pair.
  """
  def __init__(self, label_extension):
    GetThreadLogInfo().EnterLabelExtension(label_extension)

  def __del__(self):
    GetThreadLogInfo().ExitLabelExtension()


class PkbLogFilter(logging.Filter):
  """Filter that injects a thread's ThreadLogInfo label into log messages.
  """

  def filter(self, record):
    record.pkb_label = GetThreadLogInfo().GetLabel()
    return True


def ConfigureLogging(stderr_log_level, log_path, run_uri, log_thread_name,
                     file_log_level=logging.DEBUG):
  """Configure logging.

  Note that this will destroy existing logging configuration!

  This configures python logging to emit messages to stderr and a log file.

  Args:
    stderr_log_level: Messages at this level and above are emitted to stderr.
    log_path: Path to the log file.
    run_uri: A string containing the run_uri to be appended to the log prefix
      labels. If None, then the run_uri is not appended.
    log_thread_name: A Boolean that controls whether to append the current
      thread name to the log prefix labels.
    file_log_level: Messages at this level and above are written to the log
      file.
  """
  # Build the format strings for the stderr and log file message formatters.
  stderr_format_list = ['%(asctime)s']
  file_format_list = ['%(asctime)s']
  if run_uri:
    stderr_format_list.append(run_uri)
    file_format_list.append(run_uri)
  if log_thread_name:
    stderr_format_list.append('%(threadName)s')
    file_format_list.append('%(threadName)s')
  stderr_format_list.append('%(pkb_label)s%(levelname)-8s %(message)s')
  file_format_list.append(
      '%(pkb_label)s%(filename)s:%(lineno)d %(levelname)-8s %(message)s')

  # Reset root logger settings.
  logger = logging.getLogger()
  logger.handlers = []
  logger.setLevel(logging.DEBUG)

  # Initialize the main thread's ThreadLogInfo. This object must be initialized
  # to use the PkbLogFilter, and it is used to derive the ThreadLogInfo of
  # other threads started through vm_util.RunThreaded.
  SetThreadLogInfo(ThreadLogInfo())
  logger.addFilter(PkbLogFilter())

  # Add handler to output to stderr.
  handler = logging.StreamHandler()
  handler.setLevel(stderr_log_level)
  handler.setFormatter(logging.Formatter(' '.join(stderr_format_list)))
  logger.addHandler(handler)

  # Add handler for output to log file.
  logging.info('Verbose logging to: %s', log_path)
  handler = logging.FileHandler(filename=log_path)
  handler.setLevel(file_log_level)
  handler.setFormatter(logging.Formatter(' '.join(file_format_list)))
  logger.addHandler(handler)


def CreateBenchmarkLabelExtension(benchmark_name, sequence_number,
                                  total_benchmarks):
  """Generates the logger label extension for a benchmark.

  Args:
    benchmark_name: A string containing the name of the benchmark. If None,
      then the benchmark name is not appended.
    sequence_number: The sequence number of when the benchmark was started
      relative to the other benchmarks in the suite. If None, the sequence
      number is not appended.
    total_benchmarks: The total number of benchmarks in the suite.

  Returns:
    A string containing the label extension.
  """
  label_extension = ''
  if benchmark_name:
    label_extension += benchmark_name
  if sequence_number is not None:
    label_extension += '(' + str(sequence_number) + '/' + \
        str(total_benchmarks) + ')'
  return label_extension
