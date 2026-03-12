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

import inspect
import logging
import os
import threading
import unittest

from absl.testing import parameterized
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import log_util
from tests import pkb_common_test_case

_LONG_MESSAGE = 'Results of test:\n' + ' '.join(map(str, range(200)))


class LogUtilTestCase(pkb_common_test_case.PkbCommonTestCase):
  """Tests exercising the utilities in log_util."""

  def testThreadLogContextExtendLabel(self):
    """Verify ThreadLogContext.ExtendLabel behavior."""
    context = log_util.ThreadLogContext()
    self.assertEqual(context.label, '')
    with context.ExtendLabel('LABEL-A'):
      self.assertEqual(context.label, 'LABEL-A ')
      with context.ExtendLabel('LABEL-B'):
        self.assertEqual(context.label, 'LABEL-A LABEL-B ')
      self.assertEqual(context.label, 'LABEL-A ')
    self.assertEqual(context.label, '')

  def testThreadLogContextExtendLabelEmptyStrings(self):
    """Verify ThreadLogContext.ExtendLabel behavior with empty strings."""
    context = log_util.ThreadLogContext()
    self.assertEqual(context.label, '')
    with context.ExtendLabel(''):
      self.assertEqual(context.label, '')
      with context.ExtendLabel('LABEL-A'):
        self.assertEqual(context.label, 'LABEL-A ')
        with context.ExtendLabel(''):
          self.assertEqual(context.label, 'LABEL-A ')
          with context.ExtendLabel('LABEL-B'):
            self.assertEqual(context.label, 'LABEL-A LABEL-B ')
          self.assertEqual(context.label, 'LABEL-A ')
        self.assertEqual(context.label, 'LABEL-A ')
      self.assertEqual(context.label, '')
    self.assertEqual(context.label, '')

  def testThreadLogContextCopyConstruct(self):
    """Verify ThreadLogContext init with a reference ThreadLogContext behavior.

    The label state of the first ThreadLogContext should be copied.
    """
    original = log_util.ThreadLogContext()
    self.assertEqual(original.label, '')
    with original.ExtendLabel('LABEL-A'):
      self.assertEqual(original.label, 'LABEL-A ')
      copied = log_util.ThreadLogContext(original)
      self.assertEqual(original.label, 'LABEL-A ')
      self.assertEqual(copied.label, 'LABEL-A ')
      with original.ExtendLabel('LABEL-B'):
        self.assertEqual(original.label, 'LABEL-A LABEL-B ')
        self.assertEqual(copied.label, 'LABEL-A ')
        with copied.ExtendLabel('LABEL-C'):
          self.assertEqual(original.label, 'LABEL-A LABEL-B ')
          self.assertEqual(copied.label, 'LABEL-A LABEL-C ')
        self.assertEqual(original.label, 'LABEL-A LABEL-B ')
        self.assertEqual(copied.label, 'LABEL-A ')
      self.assertEqual(original.label, 'LABEL-A ')
      self.assertEqual(copied.label, 'LABEL-A ')
    self.assertEqual(original.label, '')
    self.assertEqual(copied.label, 'LABEL-A ')

  def testRunThreadedContextCopy(self):
    """Verify that ThreadLogContext is copied to threads by background_tasks.RunThreaded."""
    original = log_util.ThreadLogContext()
    log_util.SetThreadLogContext(original)
    t1_list = ['T1']
    t2_list = ['T2']
    self.assertEqual(original.label, '')
    with original.ExtendLabel('T0'):
      self.assertEqual(original.label, 'T0 ')
      background_tasks.RunThreaded(
          target=LogUtilTestCase.RunThreadedContextCopyHelper,
          thread_params=[t1_list, t2_list],
      )
      self.assertEqual(original.label, 'T0 ')
      self.assertEqual(t1_list, ['T1', 'T0 ', 'T0 T1 ', 'T0 '])
      self.assertEqual(t2_list, ['T2', 'T0 ', 'T0 T2 ', 'T0 '])

  @staticmethod
  def RunThreadedContextCopyHelper(my_list):
    """Helper method used by testRunThreadedContextCopy."""
    context = log_util.GetThreadLogContext()
    my_list.append(context.label)
    with context.ExtendLabel(my_list[0]):
      my_list.append(context.label)
    my_list.append(context.label)

  def testPkbLogFilter(self):
    """Verify that PkbLogFilter sets the pkb_label of LogRecords it processes."""
    logger_name = 'log_util_test.LogUtilTestCase.testPkbLogFilter'
    context = log_util.ThreadLogContext()
    log_util.SetThreadLogContext(context)
    with context.ExtendLabel('LABEL-A'):
      log_record = logging.LogRecord(
          name=logger_name,
          level=logging.INFO,
          pathname=__file__,
          lineno=inspect.getframeinfo(inspect.currentframe()).lineno + 1,
          msg='Log message.',
          args=None,
          exc_info=None,
      )
      log_util.PkbLogFilter().filter(log_record)
      self.assertEqual(log_record.pkb_label, 'LABEL-A ')

  def testPkbLogFilterNoContext(self):
    """Verify that PkbLogFilter works if no context was set."""
    self.completed = False

    def childLog():
      logger_name = 'log_util_test.LogUtilTestCase.testPkbLogFilterNoContext'
      self.log_record = logging.LogRecord(
          name=logger_name,
          level=logging.INFO,
          pathname=__file__,
          lineno=inspect.getframeinfo(inspect.currentframe()).lineno + 1,
          msg='Log message.',
          args=None,
          exc_info=None,
      )
      log_util.PkbLogFilter().filter(self.log_record)
      self.completed = True

    child = threading.Thread(target=childLog)
    child.start()
    child.join()
    self.assertTrue(self.completed)
    self.assertEqual(self.log_record.pkb_label, '')

  def testLogDeduplicated(self):
    log_dir = self.create_tempdir().full_path
    pkb_log_path = os.path.join(log_dir, log_util.LOG_FILE_NAME)

    log_util.ConfigureLogging(
        stderr_log_level=logging.INFO,
        logs_dir=log_dir,
        run_uri='test_uri',
        file_log_level=logging.INFO,
    )
    logging.info(_LONG_MESSAGE)
    logging.info(_LONG_MESSAGE)

    with open(pkb_log_path) as f:
      content = f.read()
      self.assertEqual(
          content.count(_LONG_MESSAGE),
          1,
          f'Expected exactly 1 occurrences of the long message in'
          f' source {content}.',
      )
      self.assertIn('has been duplicated 1 times', content)

  @parameterized.parameters(
      (_LONG_MESSAGE + 'DO NOT DEDUPLICATE'),
      ('message is too short to deduplicate'),
  )
  def testSomeLogsNotDeduplicated(self, message):
    log_dir = self.create_tempdir().full_path
    pkb_log_path = os.path.join(log_dir, log_util.LOG_FILE_NAME)

    log_util.ConfigureLogging(
        stderr_log_level=logging.INFO,
        logs_dir=log_dir,
        run_uri='test_uri',
        file_log_level=logging.INFO,
    )
    logging.info(message)
    logging.info(message)

    with open(pkb_log_path) as f:
      content = f.read()
      self.assertEqual(
          content.count(message),
          2,
          f'Expected exactly 2 occurrences of the message {message} in'
          f' source {content}.',
      )
      self.assertNotIn('has been duplicated 1 times', content)

  def testLogToShortLog(self):
    log_dir = self.create_tempdir().full_path
    short_log_path = os.path.join(log_dir, log_util.SHORT_LOG_FILE_NAME)

    log_util.ConfigureShortLogging(log_dir)
    log_util.LogToShortLog('test message')

    self.assertTrue(os.path.exists(short_log_path))
    with open(short_log_path) as f:
      self.assertIn('test message', f.read())

  def testLogToShortLogAndRoot(self):
    log_dir = self.create_tempdir().full_path
    short_log_path = os.path.join(log_dir, log_util.SHORT_LOG_FILE_NAME)
    pkb_log_path = os.path.join(log_dir, log_util.LOG_FILE_NAME)

    log_util.ConfigureLogging(
        stderr_log_level=logging.INFO,
        logs_dir=log_dir,
        run_uri='test_uri',
        file_log_level=logging.INFO,
    )
    log_util.ConfigureShortLogging(log_dir)
    log_util.LogToShortLogAndRoot('test message')

    for log_path in [short_log_path, pkb_log_path]:
      self.assertTrue(os.path.exists(log_path))
      with open(log_path) as f:
        self.assertIn('test message', f.read())


if __name__ == '__main__':
  unittest.main()
