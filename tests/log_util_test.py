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

import logging
import inspect
import os
import subprocess
import sys
import threading
import unittest

from perfkitbenchmarker import log_util
from perfkitbenchmarker import vm_util

TEMP_DIR = '/tmp/perfkitbenchmarker/log_util_test'
STDERR_PATH = TEMP_DIR + '/stderr.log'
LOG_PATH = TEMP_DIR + '/pkb.log'
FILE_NAME = (
    os.path.split(inspect.getframeinfo(inspect.currentframe()).filename))[-1]


def RunCommand(cmd):
  p = subprocess.Popen(cmd)
  p.wait()


class LogUtilTestCase(unittest.TestCase):
  """Tests exercising the utilities in log_util.
  """
  @staticmethod
  def _CreateTempDir():
    RunCommand(['mkdir', '-p', TEMP_DIR])

  @staticmethod
  def _ClearLogFiles():
    RunCommand(['cp', '/dev/null', STDERR_PATH])
    RunCommand(['cp', '/dev/null', LOG_PATH])

  @staticmethod
  def _DeleteTempDir():
    RunCommand(['rm', '-rf', TEMP_DIR])

  @staticmethod
  def _RemoveLogHandlers():
    logging.getLogger().handlers = []

  @staticmethod
  def _StripTimestamp(line):
    """Removes the timestamp from a log messages.

    Args:
      line: A string containing the original log message.

    Returns:
      A string containing the log message without the leading timestamp.
    """
    first_space = line.find(' ')
    second_space = line.find(' ', first_space + 1)
    return line[second_space + 1:]

  @staticmethod
  def _ReadStrippedLogFiles():
    with open(STDERR_PATH) as f:
      stderr_contents = [LogUtilTestCase._StripTimestamp(line)
                         for line in f.readlines()]
    with open(LOG_PATH) as f:
      log_file_contents = [LogUtilTestCase._StripTimestamp(line)
                           for line in f.readlines()]
    return (stderr_contents, log_file_contents)

  def setUp(self):
    LogUtilTestCase._CreateTempDir()
    LogUtilTestCase._ClearLogFiles()
    self._stderr = sys.stderr
    sys.stderr = open(STDERR_PATH, 'w')

  def tearDown(self):
    LogUtilTestCase._RemoveLogHandlers()
    sys.stderr.close()
    sys.stderr = self._stderr
    LogUtilTestCase._DeleteTempDir()

  def testBasicLogging(self):
    """Verify that the format of logged messages is correct.
    """
    run_uri = sys._getframe().f_code.co_name[:8]
    log_util.ConfigureLogging(logging.INFO, LOG_PATH, run_uri)

    ln_d = inspect.getframeinfo(inspect.currentframe()).lineno + 1
    logging.debug('Debug msg')
    ln_i = inspect.getframeinfo(inspect.currentframe()).lineno + 1
    logging.info('Info msg')
    ln_w = inspect.getframeinfo(inspect.currentframe()).lineno + 1
    logging.warning('Warning msg')
    ln_e = inspect.getframeinfo(inspect.currentframe()).lineno + 1
    logging.error('Error msg')

    pre = '{} {}'.format(run_uri, threading.current_thread().name)
    stderr_contents, log_file_contents = LogUtilTestCase._ReadStrippedLogFiles()
    self.assertEqual(
        stderr_contents,
        ['{} INFO     Verbose logging to: {}\n'.format(pre, LOG_PATH),
         '{} INFO     Info msg\n'.format(pre),
         '{} WARNING  Warning msg\n'.format(pre),
         '{} ERROR    Error msg\n'.format(pre)])
    self.assertEqual(
        log_file_contents,
        ['{} {}:{} DEBUG    Debug msg\n'.format(pre, FILE_NAME, ln_d),
         '{} {}:{} INFO     Info msg\n'.format(pre, FILE_NAME, ln_i),
         '{} {}:{} WARNING  Warning msg\n'.format(pre, FILE_NAME, ln_w),
         '{} {}:{} ERROR    Error msg\n'.format(pre, FILE_NAME, ln_e)])

  def testSingleThreadContext(self):
    """Verify that ThreadLogContext label extension works properly.
    """
    run_uri = sys._getframe().f_code.co_name[:8]
    log_util.ConfigureLogging(logging.INFO, LOG_PATH, run_uri)

    context = log_util.GetThreadLogContext()
    ln = []
    ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
    logging.info('Msg 0')
    with log_util.ThreadLogContext.LabelExtension(context, 'L-A'):
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 1')
      with log_util.ThreadLogContext.LabelExtension(context, 'L-B'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 2')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 3')
      with log_util.ThreadLogContext.LabelExtension(context, 'L-C'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 4')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 5')
    ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
    logging.info('Msg 6')
    with log_util.ThreadLogContext.LabelExtension(context, 'L-D'):
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 7')
      with log_util.ThreadLogContext.LabelExtension(context, 'L-E'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 8')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 9')
      with log_util.ThreadLogContext.LabelExtension(context, 'L-F'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 10')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 11')
    ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
    logging.info('Msg 12')

    pre = '{} {}'.format(run_uri, threading.current_thread().name)
    stderr_contents, log_file_contents = LogUtilTestCase._ReadStrippedLogFiles()
    self.assertEqual(
        stderr_contents,
        ['{} INFO     Verbose logging to: {}\n'.format(pre, LOG_PATH),
         '{} INFO     Msg 0\n'.format(pre),
         '{} L-A INFO     Msg 1\n'.format(pre),
         '{} L-A L-B INFO     Msg 2\n'.format(pre),
         '{} L-A INFO     Msg 3\n'.format(pre),
         '{} L-A L-C INFO     Msg 4\n'.format(pre),
         '{} L-A INFO     Msg 5\n'.format(pre),
         '{} INFO     Msg 6\n'.format(pre),
         '{} L-D INFO     Msg 7\n'.format(pre),
         '{} L-D L-E INFO     Msg 8\n'.format(pre),
         '{} L-D INFO     Msg 9\n'.format(pre),
         '{} L-D L-F INFO     Msg 10\n'.format(pre),
         '{} L-D INFO     Msg 11\n'.format(pre),
         '{} INFO     Msg 12\n'.format(pre)])
    self.assertEqual(
        log_file_contents,
        ['{} {}:{} INFO     Msg 0\n'.format(pre, FILE_NAME, ln[0]),
         '{} L-A {}:{} INFO     Msg 1\n'.format(pre, FILE_NAME, ln[1]),
         '{} L-A L-B {}:{} INFO     Msg 2\n'.format(pre, FILE_NAME, ln[2]),
         '{} L-A {}:{} INFO     Msg 3\n'.format(pre, FILE_NAME, ln[3]),
         '{} L-A L-C {}:{} INFO     Msg 4\n'.format(pre, FILE_NAME, ln[4]),
         '{} L-A {}:{} INFO     Msg 5\n'.format(pre, FILE_NAME, ln[5]),
         '{} {}:{} INFO     Msg 6\n'.format(pre, FILE_NAME, ln[6]),
         '{} L-D {}:{} INFO     Msg 7\n'.format(pre, FILE_NAME, ln[7]),
         '{} L-D L-E {}:{} INFO     Msg 8\n'.format(pre, FILE_NAME, ln[8]),
         '{} L-D {}:{} INFO     Msg 9\n'.format(pre, FILE_NAME, ln[9]),
         '{} L-D L-F {}:{} INFO     Msg 10\n'.format(pre, FILE_NAME, ln[10]),
         '{} L-D {}:{} INFO     Msg 11\n'.format(pre, FILE_NAME, ln[11]),
         '{} {}:{} INFO     Msg 12\n'.format(pre, FILE_NAME, ln[12])])

  def testThreeThreadContext(self):
    """Verify that copying ThreadLogContext between threads works properly.
    """
    run_uri = sys._getframe().f_code.co_name[:8]
    log_util.ConfigureLogging(logging.INFO, LOG_PATH, run_uri)

    # This test consists of three threads to verify that RunThreaded can launch
    # multiple threads with separate contexts that were derived from the state
    # of the parent's context at time of launch.
    #
    # T0                      T1                      T2
    # ------------------------------------------------------------------------
    # Log 'Msg 0'
    # Enter L-T0
    # | Log 'Msg 1'
    # | Start T1 and T2       Starts in L-T0          Starts in L-T0
    # |                       | Log 'Msg 2'           |
    # |                       | Enter L-T1            |
    # |                       | | Log 'Msg 3'         |
    # |                       | | Signal event_t2_0   |
    # |                       | |                     | Log 'Msg 4'
    # |                       | |                     | Enter L-T2
    # |                       | |                     | | Log 'Msg 5'
    # |                       | |                     | | Signal event_t1_0
    # |                       | | Log 'Msg 6'         | |
    # |                       | Exit L-T1             | |
    # |                       | Log 'Msg 7'           | |
    # |                       | Signal event_t2_1     | |
    # |                                               | | Log 'Msg 8'
    # |                                               | Exit L-T2
    # |                                               | Log 'Msg 9'
    # | Log 'Msg 10'
    # Exit L-T0
    # Log 'Msg 11'
    # Check results

    # Structure that is shared between threads.
    shared = {}
    shared['thread_names'] = []
    shared['line_numbers'] = []
    shared['event_t1_0'] = threading.Event()
    shared['event_t2_0'] = threading.Event()
    shared['event_t2_1'] = threading.Event()

    thread_names = shared['thread_names']
    thread_names.append(threading.current_thread().name)
    ln = shared['line_numbers']
    ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
    logging.info('Msg 0')
    t0_context = log_util.GetThreadLogContext()
    with log_util.ThreadLogContext.LabelExtension(t0_context, 'L-T0'):
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 1')
      vm_util.RunThreaded(
          target=LogUtilTestCase.ThreeThreadContextHelper,
          thread_params=[((1, shared), {}), ((2, shared), {})])
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 10')
    ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
    logging.info('Msg 11')

    # Check results
    pre0 = '{} {}'.format(run_uri, thread_names[0])
    pre1 = '{} {}'.format(run_uri, thread_names[1])
    pre2 = '{} {}'.format(run_uri, thread_names[2])
    stderr_contents, log_file_contents = LogUtilTestCase._ReadStrippedLogFiles()
    self.assertEqual(
        stderr_contents,
        ['{} INFO     Verbose logging to: {}\n'.format(pre0, LOG_PATH),
         '{} INFO     Msg 0\n'.format(pre0),
         '{} L-T0 INFO     Msg 1\n'.format(pre0),
         '{} L-T0 INFO     Msg 2\n'.format(pre1),
         '{} L-T0 L-T1 INFO     Msg 3\n'.format(pre1),
         '{} L-T0 INFO     Msg 4\n'.format(pre2),
         '{} L-T0 L-T2 INFO     Msg 5\n'.format(pre2),
         '{} L-T0 L-T1 INFO     Msg 6\n'.format(pre1),
         '{} L-T0 INFO     Msg 7\n'.format(pre1),
         '{} L-T0 L-T2 INFO     Msg 8\n'.format(pre2),
         '{} L-T0 INFO     Msg 9\n'.format(pre2),
         '{} L-T0 INFO     Msg 10\n'.format(pre0),
         '{} INFO     Msg 11\n'.format(pre0)])
    self.assertEqual(
        log_file_contents,
        ['{} {}:{} INFO     Msg 0\n'.format(pre0, FILE_NAME, ln[0]),
         '{} L-T0 {}:{} INFO     Msg 1\n'.format(pre0, FILE_NAME, ln[1]),
         '{} L-T0 {}:{} INFO     Msg 2\n'.format(pre1, FILE_NAME, ln[2]),
         '{} L-T0 L-T1 {}:{} INFO     Msg 3\n'.format(pre1, FILE_NAME, ln[3]),
         '{} L-T0 {}:{} INFO     Msg 4\n'.format(pre2, FILE_NAME, ln[4]),
         '{} L-T0 L-T2 {}:{} INFO     Msg 5\n'.format(pre2, FILE_NAME, ln[5]),
         '{} L-T0 L-T1 {}:{} INFO     Msg 6\n'.format(pre1, FILE_NAME, ln[6]),
         '{} L-T0 {}:{} INFO     Msg 7\n'.format(pre1, FILE_NAME, ln[7]),
         '{} L-T0 L-T2 {}:{} INFO     Msg 8\n'.format(pre2, FILE_NAME, ln[8]),
         '{} L-T0 {}:{} INFO     Msg 9\n'.format(pre2, FILE_NAME, ln[9]),
         '{} L-T0 {}:{} INFO     Msg 10\n'.format(pre0, FILE_NAME, ln[10]),
         '{} {}:{} INFO     Msg 11\n'.format(pre0, FILE_NAME, ln[11])])

  @staticmethod
  def ThreeThreadContextHelper(thread_id, shared):
    """Helper function that is executed by threads in testThreeThreadContext.
    """
    thread_names = shared['thread_names']
    ln = shared['line_numbers']
    if thread_id is 1:
      # Thread 1.
      thread_names.append(threading.current_thread().name)
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 2')
      t1_context = log_util.GetThreadLogContext()
      with log_util.ThreadLogContext.LabelExtension(t1_context, 'L-T1'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 3')
        shared['event_t2_0'].set()
        shared['event_t1_0'].wait()
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 6')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 7')
      shared['event_t2_1'].set()
    else:
      # Thread 2.
      shared['event_t2_0'].wait()
      thread_names.append(threading.current_thread().name)
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 4')
      t2_context = log_util.GetThreadLogContext()
      with log_util.ThreadLogContext.LabelExtension(t2_context, 'L-T2'):
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 5')
        shared['event_t1_0'].set()
        shared['event_t2_1'].wait()
        ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
        logging.info('Msg 8')
      ln.append(inspect.getframeinfo(inspect.currentframe()).lineno + 1)
      logging.info('Msg 9')


if __name__ == '__main__':
  unittest.main()
