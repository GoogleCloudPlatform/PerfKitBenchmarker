# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.linux_benchmarks.multichase_benchmark."""

import functools
import unittest

from perfkitbenchmarker import units
from perfkitbenchmarker.linux_benchmarks import multichase_benchmark


class MemorySizeParserTestCase(unittest.TestCase):

  def setUp(self):
    self._parser = multichase_benchmark._MEMORY_SIZE_PARSER

  def testParseNoUnits(self):
    with self.assertRaises(ValueError):
      self._parser.parse('10')

  def testParseInvalidUnits(self):
    with self.assertRaises(ValueError):
      self._parser.parse('20 seconds')

  def testParseExplicitMemory(self):
    q = self._parser.parse('30 GiB')
    self.assertEqual(q.magnitude, 30)
    self.assertEqual(q.units, units.Unit('gibibyte'))

  def testParsePercent(self):
    q = self._parser.parse('40%')
    self.assertEqual(q.magnitude, 40)
    self.assertEqual(q.units, units.percent)


class TranslateMemorySizeTestCase(unittest.TestCase):

  def setUp(self):
    self._func = multichase_benchmark._TranslateMemorySize

  def testExplicitSize(self):
    result = self._func(lambda: 1024, units.Quantity('1 GiB'))
    self.assertEqual(result, 1073741824)

  def testPercent(self):
    result = self._func(lambda: 1024, units.Quantity('25%'))
    self.assertEqual(result, 256)


class IterMemorySizesTestCase(unittest.TestCase):

  def setUp(self):
    self._func = functools.partial(multichase_benchmark._IterMemorySizes,
                                   lambda: 1024)

  def testHitsUpperBound(self):
    result = list(self._func(1 * units.byte, 32 * units.byte))
    self.assertEqual(result, [1, 2, 4, 8, 16, 32])

  def testSurpassesUpperBound(self):
    result = list(self._func(1 * units.byte, 20 * units.byte))
    self.assertEqual(result, [1, 2, 4, 8, 16])

  def testPercent(self):
    result = list(self._func(1 * units.percent, 10 * units.percent))
    self.assertEqual(result, [10, 20, 40, 80])

  def testEqual(self):
    result = list(self._func(32 * units.byte, 32 * units.byte))
    self.assertEqual(result, [32])

  def testMaxLessThanMin(self):
    result = list(self._func(64 * units.byte, 32 * units.byte))
    self.assertEqual(result, [])


if __name__ == '__main__':
  unittest.main()
