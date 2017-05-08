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

import copy
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
    flag = self.flags['test_flag']
    self.assertFalse(flag.present)
    self.assertIsNone(flag.value)

  def testSetViaAttribute(self):
    self.flags.test_flag = 5
    self.assertEqual(self.flags.test_flag, 5)
    self.assertFalse(self.flags['test_flag'].present)
    self.assertEqual(self.flags['test_flag'].value, 5)

  def testSetViaItemAttribute(self):
    self.flags['test_flag'].value = 5
    self.assertFalse(self.flags['test_flag'].present)
    self.assertEqual(self.flags['test_flag'].value, 5)
    self.flags['test_flag'].present = True
    self.assertTrue(self.flags['test_flag'].present)
    self.assertEqual(self.flags['test_flag'].value, 5)

  def testSetViaParse(self):
    self.flags['test_flag'].parse(5)
    self.assertTrue(self.flags['test_flag'].present)
    self.assertEqual(self.flags['test_flag'].value, 5)

  def testCopy(self):
    copied_flags = copy.deepcopy(self.flags)
    copied_flags['test_flag'].parse(5)
    self.assertFalse(self.flags['test_flag'].present)
    self.assertIsNone(self.flags['test_flag'].value)
    self.assertTrue(copied_flags['test_flag'].present)
    self.assertEqual(copied_flags['test_flag'].value, 5)


class PatchFlagsTestCase(unittest.TestCase):

  def setUp(self):
    super(PatchFlagsTestCase, self).setUp()
    self.flags = mock_flags.MockFlags()

  def testGetFlag(self):
    self.flags['test_flag'].parse(5)
    with mock_flags.PatchFlags(self.flags):
      self.assertEqual(FLAGS.test_flag, 5)
      self.assertTrue(FLAGS['test_flag'].present)
      self.assertEqual(FLAGS['test_flag'].value, 5)
    self.assertEqual(FLAGS.test_flag, 0)
    self.assertFalse(FLAGS['test_flag'].present)
    self.assertEqual(FLAGS['test_flag'].value, 0)

  def testSetFlag(self):
    with mock_flags.PatchFlags(self.flags):
      FLAGS.test_flag = 5
      self.assertEqual(FLAGS.test_flag, 5)
      self.assertFalse(FLAGS['test_flag'].present)
      self.assertEqual(FLAGS['test_flag'].value, 5)
      FLAGS['test_flag'].present = True
      self.assertTrue(FLAGS['test_flag'].present)
      self.assertEqual(FLAGS['test_flag'].value, 5)
    self.assertEqual(self.flags.test_flag, 5)
    self.assertTrue(self.flags['test_flag'].present)
    self.assertEqual(self.flags['test_flag'].value, 5)
    self.assertEqual(FLAGS.test_flag, 0)
    self.assertFalse(FLAGS['test_flag'].present)
    self.assertEqual(FLAGS['test_flag'].value, 0)

  def testCopyPatchedFlags(self):
    with mock_flags.PatchFlags(self.flags):
      FLAGS.test_flag = 1
      copied_flags = copy.deepcopy(FLAGS)
      copied_flags.test_flag = 2
      self.assertEqual(FLAGS.test_flag, 1)
      self.assertEqual(self.flags.test_flag, 1)
      self.assertEqual(copied_flags.test_flag, 2)
    self.assertEqual(FLAGS.test_flag, 0)
    self.assertEqual(self.flags.test_flag, 1)
    self.assertEqual(copied_flags.test_flag, 2)


class PatchTestCaseFlagsTestCase(unittest.TestCase):

  def testGetFlag(self):
    self.assertEqual(FLAGS.test_flag, 0)
    flags = mock_flags.PatchTestCaseFlags(self)
    flags.test_flag = 5
    self.assertEqual(FLAGS.test_flag, 5)

  def testSetFlag(self):
    self.assertEqual(FLAGS.test_flag, 0)
    flags = mock_flags.PatchTestCaseFlags(self)
    FLAGS.test_flag = 5
    self.assertEqual(flags.test_flag, 5)


if __name__ == '__main__':
  unittest.main()
