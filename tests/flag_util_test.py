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

"""Tests for flag_util.py."""

import copy
import unittest

from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util


class TestIntegerList(unittest.TestCase):
  def testSimpleLength(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertEqual(len(il), 3)

  def testRangeLength(self):
    il = flag_util.IntegerList([1, (2, 5), 9])
    self.assertEqual(len(il), 6)

  def testSimpleGetItem(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertEqual(il[0], 1)
    self.assertEqual(il[1], 2)
    self.assertEqual(il[2], 3)

  def testOutOfRangeIndexError(self):
    il = flag_util.IntegerList([1, 2, 3])
    with self.assertRaises(IndexError):
      il[4]

  def testRangeGetItem(self):
    il = flag_util.IntegerList([1, (2, 5), 9])
    self.assertEqual(il[1], 2)
    self.assertEqual(il[2], 3)
    self.assertEqual(il[5], 9)

  def testIter(self):
    il = flag_util.IntegerList([1, (2, 5), 9])
    self.assertEqual(list(il), [1, 2, 3, 4, 5, 9])


class TestParseIntegerList(unittest.TestCase):
  def setUp(self):
    self.ilp = flag_util.IntegerListParser()

  def testOneInteger(self):
    self.assertEqual(list(self.ilp.Parse('3')), [3])

  def testIntegerRange(self):
    self.assertEqual(list(self.ilp.Parse('3-5')), [3, 4, 5])

  def testIntegerList(self):
    self.assertEqual(list(self.ilp.Parse('3-5,8,10-12')),
                     [3, 4, 5, 8, 10, 11, 12])

  def testNoInteger(self):
    with self.assertRaises(ValueError):
      self.ilp.Parse('a')

  def testBadRange(self):
    with self.assertRaises(ValueError):
      self.ilp.Parse('3-a')

  def testBadList(self):
    with self.assertRaises(ValueError):
      self.ilp.Parse('3-5,8a')

  def testTrailingComma(self):
    with self.assertRaises(ValueError):
      self.ilp.Parse('3-5,')

  def testNonIncreasingEntries(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.Parse('3,2,1')

  def testNonIncreasingRange(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.Parse('3-1')


class TestIntegerListSerializer(unittest.TestCase):
  def testSerialize(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([1, (2, 5), 9])

    self.assertEqual(ser.Serialize(il),
                     '1,2-5,9')


class FlagDictSubstitutionTestCase(unittest.TestCase):

  def assertFlagState(self, flag_values, value, present):
    self.assertEqual(flag_values.test_flag, value)
    self.assertEqual(flag_values['test_flag'].value, value)
    self.assertEqual(flag_values['test_flag'].present, present)

  def testReadAndWrite(self):
    flag_values = flags.FlagValues()
    flags.DEFINE_integer('test_flag', 0, 'Test flag.', flag_values=flag_values)
    flag_values_copy = copy.deepcopy(flag_values)
    flag_values_copy.test_flag = 1
    self.assertFlagState(flag_values, 0, False)
    self.assertFlagState(flag_values_copy, 1, False)
    with flag_util.FlagDictSubstitution(flag_values, flag_values_copy.FlagDict):
      self.assertFlagState(flag_values, 1, False)
      self.assertFlagState(flag_values_copy, 1, False)
      flag_values.test_flag = 2
      flag_values['test_flag'].present += 1
      self.assertFlagState(flag_values, 2, True)
      self.assertFlagState(flag_values_copy, 2, True)
    self.assertFlagState(flag_values, 0, False)
    self.assertFlagState(flag_values_copy, 2, True)
    flag_values.test_flag = 3
    self.assertFlagState(flag_values, 3, False)
    self.assertFlagState(flag_values_copy, 2, True)


if __name__ == '__main__':
  unittest.main()
