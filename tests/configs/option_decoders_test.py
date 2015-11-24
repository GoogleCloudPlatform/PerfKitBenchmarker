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
"""Tests for perfkitbenchmarker.configs.option_decoders."""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import option_decoders


_COMPONENT = 'test_component'
_OPTION = 'test_option'


def _ReturnFive():
  return 5


class _PassThroughDecoder(option_decoders.ConfigOptionDecoder):

  def Decode(self, value):
    return value


class ConfigOptionDecoderTestCase(unittest.TestCase):

  def testNoDefault(self):
    decoder = _PassThroughDecoder(_COMPONENT, _OPTION)
    self.assertTrue(decoder.required)
    with self.assertRaises(AssertionError) as cm:
      decoder.default
    self.assertEqual(str(cm.exception), (
        'Attempted to get the default value of test_component required config '
        'option "test_option".'))

  def testDefaultValue(self):
    decoder = _PassThroughDecoder(_COMPONENT, _OPTION, default=None)
    self.assertFalse(decoder.required)
    self.assertEqual(decoder.default, None)

  def testDefaultCallable(self):
    decoder = _PassThroughDecoder(_COMPONENT, _OPTION, default=_ReturnFive)
    self.assertFalse(decoder.required)
    self.assertEqual(decoder.default, 5)

  def testIncompleteDerivedClass(self):
    class IncompleteDerivedClass(option_decoders.ConfigOptionDecoder):
      pass
    with self.assertRaises(TypeError):
      IncompleteDerivedClass(_COMPONENT, _OPTION)


class BooleanDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.BooleanDecoder(_COMPONENT, _OPTION, default=None)
    self.assertFalse(decoder.required)
    self.assertEqual(decoder.default, None)

  def testNonBoolean(self):
    decoder = option_decoders.BooleanDecoder(_COMPONENT, _OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component "test_option" value: "5" (of type "int"). '
        'Value must be a boolean.'))

  def testValidBoolean(self):
    decoder = option_decoders.BooleanDecoder(_COMPONENT, _OPTION)
    self.assertEqual(decoder.Decode(True), True)


class IntDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.IntDecoder(_COMPONENT, _OPTION, default=5)
    self.assertFalse(decoder.required)
    self.assertEqual(decoder.default, 5)

  def testNonInt(self):
    decoder = option_decoders.IntDecoder(_COMPONENT, _OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode('5')
    self.assertEqual(str(cm.exception), (
        'Invalid test_component "test_option" value: "5" (of type "str"). '
        'Value must be an integer.'))

  def testValidInt(self):
    decoder = option_decoders.IntDecoder(_COMPONENT, _OPTION)
    self.assertEqual(decoder.Decode(5), 5)

  def testMax(self):
    decoder = option_decoders.IntDecoder(_COMPONENT, _OPTION, max=2)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component "test_option" value: "5". Value must be at '
        'most 2.'))
    self.assertEqual(decoder.Decode(2), 2)
    self.assertEqual(decoder.Decode(1), 1)

  def testMin(self):
    decoder = option_decoders.IntDecoder(_COMPONENT, _OPTION, min=10)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component "test_option" value: "5". Value must be at '
        'least 10.'))
    self.assertEqual(decoder.Decode(10), 10)
    self.assertEqual(decoder.Decode(15), 15)


class StringDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.StringDecoder(_COMPONENT, _OPTION, default=None)
    self.assertFalse(decoder.required)
    self.assertEqual(decoder.default, None)

  def testNonString(self):
    decoder = option_decoders.StringDecoder(_COMPONENT, _OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component "test_option" value: "5" (of type "int"). '
        'Value must be a string.'))

  def testValidString(self):
    decoder = option_decoders.StringDecoder(_COMPONENT, _OPTION)
    self.assertEqual(decoder.Decode('red'), 'red')


if __name__ == '__main__':
  unittest.main()
