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
_FLAGS = None
_OPTION = 'test_option'


def _ReturnFive():
  return 5


class _PassThroughDecoder(option_decoders.ConfigOptionDecoder):

  def Decode(self, value, component_path, flag_values):
    return value


class ConfigOptionDecoderTestCase(unittest.TestCase):

  def testNoDefault(self):
    decoder = _PassThroughDecoder(_OPTION)
    self.assertIs(decoder.required, True)
    with self.assertRaises(AssertionError) as cm:
      decoder.default
    self.assertEqual(str(cm.exception), (
        'Attempted to get the default value of required config option '
        '"test_option".'))

  def testDefaultValue(self):
    decoder = _PassThroughDecoder(_OPTION, default=None)
    self.assertIs(decoder.required, False)
    self.assertIsNone(decoder.default)

  def testDefaultCallable(self):
    decoder = _PassThroughDecoder(_OPTION, default=_ReturnFive)
    self.assertIs(decoder.required, False)
    self.assertIs(decoder.default, 5)

  def testIncompleteDerivedClass(self):
    class IncompleteDerivedClass(option_decoders.ConfigOptionDecoder):
      pass
    with self.assertRaises(TypeError):
      IncompleteDerivedClass(_OPTION)


class TypeVerifierTestCase(unittest.TestCase):

  def testRejectNone(self):
    decoder = option_decoders.TypeVerifier(_OPTION, (int, float), default=None)
    self.assertIs(decoder.required, False)
    self.assertIsNone(decoder.default)
    self.assertIs(decoder.Decode(5, _COMPONENT, _FLAGS), 5)
    self.assertIs(decoder.Decode(5.5, _COMPONENT, _FLAGS), 5.5)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(None, _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "None" (of type '
        '"NoneType"). Value must be one of the following types: int, float.'))
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode('red', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "red" (of type "str"). '
        'Value must be one of the following types: int, float.'))

  def testAcceptNone(self):
    decoder = option_decoders.TypeVerifier(_OPTION, (int, float), none_ok=True,
                                           default=None)
    self.assertIs(decoder.required, False)
    self.assertIsNone(decoder.default)
    self.assertIs(decoder.Decode(5, _COMPONENT, _FLAGS), 5)
    self.assertIs(decoder.Decode(5.5, _COMPONENT, _FLAGS), 5.5)
    self.assertIsNone(decoder.Decode(None, _COMPONENT, _FLAGS))
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode('red', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "red" (of type "str"). '
        'Value must be one of the following types: NoneType, int, float.'))


class BooleanDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.BooleanDecoder(_OPTION, default=None)
    self.assertIs(decoder.required, False)
    self.assertIsNone(decoder.default)

  def testNone(self):
    decoder = option_decoders.BooleanDecoder(_OPTION, none_ok=True)
    self.assertIsNone(decoder.Decode(None, _COMPONENT, _FLAGS))
    decoder = option_decoders.BooleanDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue):
      decoder.Decode(None, _COMPONENT, _FLAGS)

  def testNonBoolean(self):
    decoder = option_decoders.BooleanDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5, _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "5" (of type "int"). '
        'Value must be one of the following types: bool.'))

  def testValidBoolean(self):
    decoder = option_decoders.BooleanDecoder(_OPTION)
    self.assertIs(decoder.Decode(True, _COMPONENT, _FLAGS), True)


class IntDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.IntDecoder(_OPTION, default=5)
    self.assertIs(decoder.required, False)
    self.assertIs(decoder.default, 5)

  def testNone(self):
    decoder = option_decoders.IntDecoder(_OPTION, none_ok=True)
    self.assertIsNone(decoder.Decode(None, _COMPONENT, _FLAGS))
    decoder = option_decoders.IntDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue):
      decoder.Decode(None, _COMPONENT, _FLAGS)

  def testNonInt(self):
    decoder = option_decoders.IntDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode('5', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "5" (of type "str"). '
        'Value must be one of the following types: int.'))

  def testValidInt(self):
    decoder = option_decoders.IntDecoder(_OPTION)
    self.assertEqual(decoder.Decode(5, _COMPONENT, _FLAGS), 5)

  def testMax(self):
    decoder = option_decoders.IntDecoder(_OPTION, max=2)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5, _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "5". Value must be at '
        'most 2.'))
    self.assertIs(decoder.Decode(2, _COMPONENT, _FLAGS), 2)
    self.assertIs(decoder.Decode(1, _COMPONENT, _FLAGS), 1)

  def testMin(self):
    decoder = option_decoders.IntDecoder(_OPTION, min=10)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5, _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "5". Value must be at '
        'least 10.'))
    self.assertIs(decoder.Decode(10, _COMPONENT, _FLAGS), 10)
    self.assertIs(decoder.Decode(15, _COMPONENT, _FLAGS), 15)


class StringDecoderTestCase(unittest.TestCase):

  def testDefault(self):
    decoder = option_decoders.StringDecoder(_OPTION, default=None)
    self.assertFalse(decoder.required)
    self.assertIsNone(decoder.default)

  def testNone(self):
    decoder = option_decoders.IntDecoder(_OPTION, none_ok=True)
    self.assertIsNone(decoder.Decode(None, _COMPONENT, _FLAGS))
    decoder = option_decoders.IntDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue):
      decoder.Decode(None, _COMPONENT, _FLAGS)

  def testNonString(self):
    decoder = option_decoders.StringDecoder(_OPTION)
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      decoder.Decode(5, _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.test_option value: "5" (of type "int"). '
        'Value must be one of the following types: basestring.'))

  def testValidString(self):
    decoder = option_decoders.StringDecoder(_OPTION)
    self.assertEqual(decoder.Decode('red', _COMPONENT, _FLAGS), 'red')


if __name__ == '__main__':
  unittest.main()
