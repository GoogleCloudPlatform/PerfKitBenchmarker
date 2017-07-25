# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
import sys
import unittest

from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import units


class TestIntegerList(unittest.TestCase):
  def testSimpleLength(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertEqual(len(il), 3)

  def testRangeLength(self):
    il = flag_util.IntegerList([1, (2, 5), 9])
    self.assertEqual(len(il), 6)

  def testRangeLengthWithStep(self):
    il = flag_util.IntegerList([1, (2, 7, 2), 9])
    self.assertEqual(len(il), 5)

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

  def testRangeWithStepGetItem(self):
    il = flag_util.IntegerList([1, (2, 7, 2), 9])
    self.assertEqual(il[1], 2)
    self.assertEqual(il[2], 4)
    self.assertEqual(il[3], 6)
    self.assertEqual(il[4], 9)

  def testIter(self):
    il = flag_util.IntegerList([1, (2, 5), 9])
    self.assertEqual(list(il), [1, 2, 3, 4, 5, 9])

  def testIterWithStep(self):
    il = flag_util.IntegerList([1, (2, 6, 2), 9])
    self.assertEqual(list(il), [1, 2, 4, 6, 9])

  def testEqTrue(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertEqual([1, 2, 3], il)

  def testEqFalse(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertFalse([1] == il)

  def testNotEqTrue(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertNotEqual([1], il)

  def testNotEqFalse(self):
    il = flag_util.IntegerList([1, 2, 3])
    self.assertTrue([1] != il)


class TestParseIntegerList(unittest.TestCase):
  def setUp(self):
    self.ilp = flag_util.IntegerListParser()

  def testOneInteger(self):
    self.assertEqual(list(self.ilp.parse('3')), [3])

  def testIntegerRange(self):
    self.assertEqual(list(self.ilp.parse('3-5')), [3, 4, 5])

  def testIntegerRangeWithStep(self):
    self.assertEqual(list(self.ilp.parse('2-7-2')), [2, 4, 6])

  def testIntegerList(self):
    self.assertEqual(list(self.ilp.parse('3-5,8,10-12')),
                     [3, 4, 5, 8, 10, 11, 12])

  def testIntegerListWithRangeAndStep(self):
    self.assertEqual(list(self.ilp.parse('3-5,8,10-15-2')),
                     [3, 4, 5, 8, 10, 12, 14])

  def testNoInteger(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('a')

  def testBadRange(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-a')

  def testBadList(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-5,8a')

  def testTrailingComma(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-5,')

  def testNonIncreasingEntries(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.parse('3,2,1')

  def testNonIncreasingRange(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.parse('3-1')

  def testNonIncreasingRangeWithStep(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.parse('3-1-2')


class TestIntegerListSerializer(unittest.TestCase):
  def testSerialize(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([1, (2, 5), 9])

    self.assertEqual(ser.serialize(il),
                     '1,2-5,9')

  def testSerializeWithStep(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([1, (2, 5, 2), 9])

    self.assertEqual(ser.serialize(il),
                     '1,2-5-2,9')


class FlagDictSubstitutionTestCase(unittest.TestCase):

  def assertFlagState(self, flag_values, value, present):
    self.assertEqual(flag_values.test_flag, value)
    self.assertEqual(flag_values['test_flag'].value, value)
    self.assertEqual(flag_values['test_flag'].present, present)

  def testReadAndWrite(self):
    flag_values = flags.FlagValues()
    flags.DEFINE_integer('test_flag', 0, 'Test flag.', flag_values=flag_values)
    flag_values([sys.argv[0]])
    flag_values_copy = copy.deepcopy(flag_values)
    flag_values_copy.test_flag = 1
    self.assertFlagState(flag_values, 0, False)
    self.assertFlagState(flag_values_copy, 1, False)
    if hasattr(flag_values_copy, '_flags'):
      flag_dict_func = flag_values_copy._flags
    else:
      flag_dict_func = flag_values_copy.FlagDict
    with flag_util.FlagDictSubstitution(flag_values, flag_dict_func):
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


class TestUnitsParser(unittest.TestCase):

  def setUp(self):
    self.up = flag_util.UnitsParser('byte')

  def testParser(self):
    self.assertEqual(self.up.parse('10KiB'), 10 * 1024 * units.byte)

  def testQuantity(self):
    quantity = 1.0 * units.byte
    self.assertEqual(self.up.parse(quantity), quantity)

  def testBadExpression(self):
    with self.assertRaises(ValueError):
      self.up.parse('asdf')

  def testUnitlessExpression(self):
    with self.assertRaises(ValueError):
      self.up.parse('10')

  def testBytes(self):
    q = self.up.parse('1B')
    self.assertEqual(q.magnitude, 1.0)
    self.assertEqual(q.units, {'byte': 1.0})

  def testBytesWithPrefix(self):
    q = self.up.parse('2KB').to(units.byte)
    self.assertEqual(q.magnitude, 2000.0)
    self.assertEqual(q.units, {'byte': 1.0})

  def testWrongUnit(self):
    with self.assertRaises(ValueError):
      self.up.parse('1m')

  def testConvertibleToUnit(self):
    up = flag_util.UnitsParser(convertible_to=units.byte)
    self.assertEqual(up.parse('10KiB'), 10 * 1024 * units.byte)

  def testConvertibleToSeries(self):
    up = flag_util.UnitsParser(convertible_to=(units.byte, 'second'))
    self.assertEqual(up.parse('10 MB'), 10 * units.Unit('megabyte'))
    self.assertEqual(up.parse('10 minutes'), 10 * units.Unit('minute'))
    with self.assertRaises(ValueError):
      up.parse('1 meter')

  def testPercent(self):
    up = flag_util.UnitsParser(convertible_to=units.percent)
    self.assertEqual(up.parse('100%'), 100 * units.percent)
    with self.assertRaises(ValueError):
      up.parse('10KiB')


class TestStringToBytes(unittest.TestCase):
  def testValidString(self):
    self.assertEqual(flag_util.StringToBytes('100KB'),
                     100000)

  def testUnparseableString(self):
    with self.assertRaises(ValueError):
      flag_util.StringToBytes('asdf')

  def testBadUnits(self):
    with self.assertRaises(ValueError):
      flag_util.StringToBytes('100m')

  def testNonIntegerBytes(self):
    with self.assertRaises(ValueError):
      flag_util.StringToBytes('1.5B')

  def testNegativeBytes(self):
    with self.assertRaises(ValueError):
      flag_util.StringToBytes('-10KB')


class TestStringToRawPct(unittest.TestCase):
  def testValidPct(self):
    self.assertEquals(flag_util.StringToRawPercent('50.5%'),
                      50.5)

  def testNullString(self):
    with self.assertRaises(ValueError):
      flag_util.StringToRawPercent('')

  def testOneCharacterString(self):
    with self.assertRaises(ValueError):
      flag_util.StringToRawPercent('%')

  def testNoPercentSign(self):
    with self.assertRaises(ValueError):
      flag_util.StringToRawPercent('100')

  def testNegativePercent(self):
    with self.assertRaises(ValueError):
      flag_util.StringToRawPercent('-12%')

  def testPercentAbove100(self):
    with self.assertRaises(ValueError):
      flag_util.StringToRawPercent('112%')


class TestYAMLParser(unittest.TestCase):

  def setUp(self):
    self.parser = flag_util.YAMLParser()

  def testValidString(self):
    self.assertEqual(self.parser.parse('[1, 2, 3]'),
                     [1, 2, 3])

  def testPreParsedYAML(self):
    self.assertEqual(self.parser.parse([1, 2, 3]),
                     [1, 2, 3])

  def testBadYAML(self):
    with self.assertRaises(ValueError):
      self.parser.parse('{a')


class MockFlag():
  def __init__(self, name, value, present):
    self.name = name
    self.value = value
    self.present = present


class TestGetProvidedCommandLineFlags(unittest.TestCase):

  def setUp(self):
    flag_dict = {
        'flag1': MockFlag('flag1', '1', True),
        'flag2': MockFlag('flag2', '2', True),
        'flag3': MockFlag('flag3', '3', False)
    }
    flag_util.FLAGS = flag_dict

  def tearDown(self):
    flag_util.FLAGS = {}

  def testGetProvidedCommandLineFlags(self):
    self.assertDictEqual({
        'flag1': '1',
        'flag2': '2',
    }, flag_util.GetProvidedCommandLineFlags())


if __name__ == '__main__':
  unittest.main()
