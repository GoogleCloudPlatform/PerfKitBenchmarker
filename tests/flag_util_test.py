# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

import sys
import unittest

from absl import flags
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
    self.assertEqual(list(self.ilp.parse('-3')), [-3])

  def testMultipleIntegers(self):
    self.assertEqual(list(self.ilp.parse('1,-1,4')), [1, -1, 4])

  def testIntegerRange(self):
    self.assertEqual(list(self.ilp.parse('3-5')), [3, 4, 5])
    self.assertEqual(list(self.ilp.parse('3:5')), [3, 4, 5])
    self.assertEqual(list(self.ilp.parse('5-3')), [5, 4, 3])
    self.assertEqual(list(self.ilp.parse('5:3')), [5, 4, 3])
    self.assertEqual(list(self.ilp.parse('-3:-1')), [-3, -2, -1])
    self.assertEqual(list(self.ilp.parse('-1:-3')), [-1, -2, -3])

  def testIntegerRangeWithStep(self):
    self.assertEqual(list(self.ilp.parse('2-7-2')), [2, 4, 6])
    self.assertEqual(list(self.ilp.parse('2:7:2')), [2, 4, 6])
    # go from -3 to 1 with step 2: -3, -3+2 = -1, -1+2 = 1,
    # finally 1 + 2 = 3 which is beyond the end of the range.
    self.assertEqual(list(self.ilp.parse('-3:1:2')), [-3, -1, 1])

  def testIntegerList(self):
    self.assertEqual(list(self.ilp.parse('3-5,8,10-12')),
                     [3, 4, 5, 8, 10, 11, 12])
    self.assertEqual(list(self.ilp.parse('3:5,8,10:12')),
                     [3, 4, 5, 8, 10, 11, 12])

  def testIntegerListWithRangeAndStep(self):
    self.assertEqual(list(self.ilp.parse('3-5,8,10-15-2')),
                     [3, 4, 5, 8, 10, 12, 14])
    self.assertEqual(list(self.ilp.parse('3:5,8,10:15:2')),
                     [3, 4, 5, 8, 10, 12, 14])

  def testIncreasingIntegerLists(self):
    self.assertEqual(list(self.ilp.parse('1-5-2,6-8')),
                     [1, 3, 5, 6, 7, 8])

  def testAnyNegativeValueRequiresNewFormat(self):
    for str_range in ('-1-5', '3--5', '3-1--1'):
      with self.assertRaises(ValueError):
        self.ilp.parse(str_range)
    # how to do those in new format
    self.assertEqual(self.ilp.parse('-1:5'), [-1, 0, 1, 2, 3, 4, 5])
    self.assertEqual(self.ilp.parse('3:-5'), [3, 2, 1, 0, -1, -2, -3, -4, -5])
    self.assertEqual(self.ilp.parse('3:1:-1'), [3, 2, 1])

  def testNoMixingOfFormats(self):
    with self.assertRaises(ValueError):
      # any negative numbers -> must use the new format
      self.ilp.parse('-1-2')
    with self.assertRaises(ValueError):
      # starts off with new format and then switches to old
      self.ilp.parse('4:2-1')
    with self.assertRaises(ValueError):
      # starts off with old format and then switches to new
      self.ilp.parse('4-2:1')

  def testMixingFormatsOkayInDifferentChunks(self):
    # different formats in each comma separated part are parsed separately so
    # mixing is okay (but should probably convert all to new format)
    self.assertEqual(list(self.ilp.parse('1-2,4,6:7')), [1, 2, 4, 6, 7])
    self.assertEqual(list(self.ilp.parse('-1:-2,3,4-5')), [-1, -2, 3, 4, 5])

  def testNoInteger(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('a')

  def testBadRange(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-a')
    with self.assertRaises(ValueError):
      self.ilp.parse('3:a')

  def testBadList(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-5,8a')
    with self.assertRaises(ValueError):
      self.ilp.parse('3:5,8a')

  def testTrailingComma(self):
    with self.assertRaises(ValueError):
      self.ilp.parse('3-5,')
    with self.assertRaises(ValueError):
      self.ilp.parse('3:5,')

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
    with self.assertRaises(ValueError):
      ilp.parse('3:1')

  def testNonIncreasingRangeWithStep(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError):
      ilp.parse('3-1-2')
    with self.assertRaises(ValueError):
      ilp.parse('3:1:2')
    with self.assertRaises(ValueError):
      ilp.parse('3:1:-2')

  def testIntegerListsWhichAreNotIncreasing(self):
    ilp = flag_util.IntegerListParser(
        on_nonincreasing=flag_util.IntegerListParser.EXCEPTION)
    with self.assertRaises(ValueError) as cm:
      ilp.parse('1-5,3-7')
    self.assertEqual('Integer list 1-5,3-7 is not increasing',
                     str(cm.exception))


class TestIntegerListSerializer(unittest.TestCase):

  def testSerialize(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([1, (2, 5), 9])

    self.assertEqual(ser.serialize(il),
                     '1,2-5,9')
    self.assertEqual(str(il), '1,2-5,9')
    # previously was <perfkitbenchmarker.flag_util.IntegerList object at ...>
    self.assertEqual(repr(il), 'IntegerList([1,2-5,9])')

  def testSerializeNegativeNumbers(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([-5, 4])
    self.assertEqual(ser.serialize(il), '-5,4')

  def testSerializeRangeNegativeNumbers(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([(-5, 3)])
    self.assertEqual(ser.serialize(il), '-5:3')
    il = flag_util.IntegerList([(4, -2)])
    self.assertEqual(ser.serialize(il), '4:-2')

  def testSerializeRangeNegativeStep(self):
    ser = flag_util.IntegerListSerializer()
    # keep this in old-style format -- however should not get this
    # tuple from the parser as the step will always have correct sign
    il = flag_util.IntegerList([(5, 2, 1)])
    self.assertEqual(ser.serialize(il), '5-2-1')
    # previously serialized as 5-2--1, move to new format
    il = flag_util.IntegerList([(5, 2, -1)])
    self.assertEqual(ser.serialize(il), '5:2:-1')
    # first or second value < 0
    il = flag_util.IntegerList([(5, -2, -1)])
    self.assertEqual(ser.serialize(il), '5:-2:-1')
    il = flag_util.IntegerList([(-5, 2, 1)])
    self.assertEqual(ser.serialize(il), '-5:2:1')

  def testSerializeWithStep(self):
    ser = flag_util.IntegerListSerializer()
    il = flag_util.IntegerList([1, (2, 5, 2), 9])

    self.assertEqual(ser.serialize(il),
                     '1,2-5-2,9')


class OverrideFlagsTestCase(unittest.TestCase):

  def assertFlagState(self, flag_values, value, present):
    self.assertEqual(flag_values.test_flag, value)
    self.assertEqual(flag_values['test_flag'].value, value)
    self.assertEqual(flag_values['test_flag'].present, present)

  def testReadAndWrite(self):
    flag_values = flags.FlagValues()
    flags.DEFINE_integer('test_flag', 0, 'Test flag.', flag_values=flag_values)
    flag_values([sys.argv[0]])
    flag_values_overrides = {}
    flag_values_overrides['test_flag'] = 1
    self.assertFlagState(flag_values, 0, False)
    self.assertEqual(flag_values_overrides['test_flag'], 1)
    with flag_util.OverrideFlags(flag_values, flag_values_overrides):
      self.assertFlagState(flag_values, 1, True)
      self.assertEqual(flag_values_overrides['test_flag'], 1)
    self.assertFlagState(flag_values, 0, False)
    self.assertEqual(flag_values_overrides['test_flag'], 1)
    flag_values.test_flag = 3
    self.assertFlagState(flag_values, 3, False)
    self.assertEqual(flag_values_overrides['test_flag'], 1)

  def testFlagChangesAreNotReflectedInConfigDict(self):
    flag_values = flags.FlagValues()
    flags.DEFINE_integer('test_flag', 0, 'Test flag.', flag_values=flag_values)
    flag_values([sys.argv[0]])
    flag_values_overrides = {}
    flag_values_overrides['test_flag'] = 1
    self.assertFlagState(flag_values, 0, False)
    self.assertEqual(flag_values_overrides['test_flag'], 1)
    with flag_util.OverrideFlags(flag_values, flag_values_overrides):
      self.assertFlagState(flag_values, 1, True)
      flag_values.test_flag = 2
      self.assertFlagState(flag_values, 2, True)
      self.assertEqual(flag_values_overrides['test_flag'], 1)


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
    with self.assertRaises(ValueError) as cm:
      flag_util.StringToBytes('asdf')
    self.assertEqual("Couldn't parse size asdf", str(cm.exception))

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
    self.assertEqual(flag_util.StringToRawPercent('50.5%'),
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
    with self.assertRaises(ValueError) as cm:
      self.parser.parse('{a')
    self.assertIn("Couldn't parse YAML string '{a': ", str(cm.exception))


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
