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
import unittest

from perfkitbenchmarker import log_util
from perfkitbenchmarker import vm_util


class LogUtilTestCase(unittest.TestCase):
  """Tests exercising the utilities in log_util."""

  def testThreadLogContextLabelExtension(self):
    """Verify ThreadLogContext.LabelExtension behavior."""
    context = log_util.ThreadLogContext()
    self.assertEqual(context.label, '')
    with context.LabelExtension('LABEL-A'):
      self.assertEqual(context.label, 'LABEL-A ')
      with context.LabelExtension('LABEL-B'):
        self.assertEqual(context.label, 'LABEL-A LABEL-B ')
      self.assertEqual(context.label, 'LABEL-A ')
    self.assertEqual(context.label, '')

  def testThreadLogContextLabelExtensionEmptyStrings(self):
    """Verify ThreadLogContext.LabelExtension behavior with empty strings."""
    context = log_util.ThreadLogContext()
    self.assertEqual(context.label, '')
    with context.LabelExtension(''):
      self.assertEqual(context.label, '')
      with context.LabelExtension('LABEL-A'):
        self.assertEqual(context.label, 'LABEL-A ')
        with context.LabelExtension(''):
          self.assertEqual(context.label, 'LABEL-A ')
          with context.LabelExtension('LABEL-B'):
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
    with original.LabelExtension('LABEL-A'):
      self.assertEqual(original.label, 'LABEL-A ')
      copied = log_util.ThreadLogContext(original)
      self.assertEqual(copied.label, 'LABEL-A ')
      self.assertIsNot(copied.label, original.label)

  def testRunThreadedContextCopy(self):
    """Verify that ThreadLogContext is copied to threads by vm_util.RunThreaded.
    """
    original = log_util.ThreadLogContext()
    log_util.SetThreadLogContext(original)
    t1_list = ['T1']
    t2_list = ['T2']
    self.assertEqual(original.label, '')
    with original.LabelExtension('T0'):
      self.assertEqual(original.label, 'T0 ')
      vm_util.RunThreaded(
          target=LogUtilTestCase.RunThreadedContextCopyHelper,
          thread_params=[t1_list, t2_list])
      self.assertEqual(original.label, 'T0 ')
      self.assertEqual(t1_list, ['T1', 'T0 ', 'T0 T1 ', 'T0 '])
      self.assertEqual(t2_list, ['T2', 'T0 ', 'T0 T2 ', 'T0 '])

  @staticmethod
  def RunThreadedContextCopyHelper(my_list):
    """Helper method used by testRunThreadedContextCopy."""
    context = log_util.GetThreadLogContext()
    my_list.append(context.label)
    with context.LabelExtension(my_list[0]):
      my_list.append(context.label)
    my_list.append(context.label)

  def testPkbLogFilter(self):
    """Verify that PkbLogFilter sets the pkb_label of LogRecords it processes.
    """
    logger_name = 'log_util_test.LogUtilTestCase.testPkbLogFilter'
    logger = logging.getLogger(logger_name)
    context = log_util.ThreadLogContext()
    log_util.SetThreadLogContext(context)
    with context.LabelExtension('LABEL-A'):
      logger.addFilter(log_util.PkbLogFilter())
      log_record = logging.LogRecord(
          name=logger_name, level=logging.INFO, pathname=__file__,
          lineno=inspect.getframeinfo(inspect.currentframe()).lineno + 1,
          msg="Log message.", args=None, exc_info=None)
      logger.handle(log_record)
      self.assertEqual(log_record.pkb_label, 'LABEL-A ')


if __name__ == '__main__':
  unittest.main()
