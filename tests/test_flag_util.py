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

import unittest

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
