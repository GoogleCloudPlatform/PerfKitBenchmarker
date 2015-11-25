# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for tests.mock_flags."""

import unittest

from perfkitbenchmarker import flags
from tests import mock_flags


FLAGS = flags.FLAGS

flags.DEFINE_integer('test_flag', 0, 'Test flag.')


class MockFlagsTestCase(unittest.TestCase):

  def setUp(self):
    super(MockFlagsTestCase, self).setUp()
    self.flags = mock_flags.MockFlags()

  def testGetUnsetFlag(self):
    self.assertIs(self.flags['test_flag'].present, False)

  def testSetAndGetFlag(self):
    self.flags.test_flag = 5
    self.assertIs(self.flags.test_flag, 5)
    self.assertIs(self.flags['test_flag'].present, True)
    self.assertIs(self.flags['test_flag'].value, 5)


class PatchFlagsTestCase(unittest.TestCase):

  def setUp(self):
    super(PatchFlagsTestCase, self).setUp()
    self.flags = mock_flags.MockFlags()

  def testGetFlag(self):
    self.flags.test_flag = 5
    with mock_flags.PatchFlags(self.flags):
      self.assertIs(FLAGS.test_flag, 5)
      self.assertIs(FLAGS['test_flag'].present, True)
      self.assertIs(FLAGS['test_flag'].value, 5)
    self.assertIs(FLAGS.test_flag, 0)
    self.assertIs(FLAGS['test_flag'].present, False)
    self.assertIs(FLAGS['test_flag'].value, 0)

  def testSetFlag(self):
    with mock_flags.PatchFlags(self.flags):
      FLAGS.test_flag = 5
      self.assertIs(FLAGS.test_flag, 5)
      self.assertIs(FLAGS['test_flag'].present, True)
      self.assertIs(FLAGS['test_flag'].value, 5)
    self.assertIs(self.flags.test_flag, 5)
    self.assertIs(self.flags['test_flag'].present, True)
    self.assertIs(self.flags['test_flag'].value, 5)
    self.assertIs(FLAGS.test_flag, 0)
    self.assertIs(self.flags['test_flag'].present, False)
    self.assertIs(self.flags['test_flag'].value, 0)


if __name__ == '__main__':
  unittest.main()
