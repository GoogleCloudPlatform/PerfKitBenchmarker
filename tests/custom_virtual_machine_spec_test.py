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

"""Tests for perfkitbenchmarker.custom_virtual_machine_spec."""

import unittest

from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import errors


_COMPONENT = 'test_component'
_FLAGS = None


class MemoryDecoderTestCase(unittest.TestCase):

  def setUp(self):
    super(MemoryDecoderTestCase, self).setUp()
    self.decoder = custom_virtual_machine_spec.MemoryDecoder(option='memory')

  def testValidStrings(self):
    self.assertEqual(self.decoder.Decode('1280MiB', _COMPONENT, _FLAGS), 1280)
    self.assertEqual(self.decoder.Decode('7.5GiB', _COMPONENT, _FLAGS), 7680)

  def testImproperPattern(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self.decoder.Decode('1280', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.memory value: "1280". Examples of valid '
        'values: "1280MiB", "7.5GiB".'))

  def testInvalidFloat(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self.decoder.Decode('1280.9.8MiB', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.memory value: "1280.9.8MiB". "1280.9.8" is not '
        'a valid float.'))

  def testNonIntegerMiB(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self.decoder.Decode('7.6GiB', _COMPONENT, _FLAGS)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.memory value: "7.6GiB". The specified size '
        'must be an integer number of MiB.'))


class CustomMachineTypeSpecTestCase(unittest.TestCase):

  def testValid(self):
    result = custom_virtual_machine_spec.CustomMachineTypeSpec(
        _COMPONENT, cpus=1, memory='7.5GiB')
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)

  def testMissingCpus(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      custom_virtual_machine_spec.CustomMachineTypeSpec(
          _COMPONENT, memory='7.5GiB')
    self.assertEqual(str(cm.exception), (
        'Required options were missing from test_component: cpus.'))

  def testMissingMemory(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      custom_virtual_machine_spec.CustomMachineTypeSpec(_COMPONENT, cpus=1)
    self.assertEqual(str(cm.exception), (
        'Required options were missing from test_component: memory.'))

  def testExtraOptions(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      custom_virtual_machine_spec.CustomMachineTypeSpec(
          _COMPONENT, cpus=1, memory='7.5GiB', extra1='one', extra2=2)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component: extra1, extra2.'))

  def testInvalidCpus(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      custom_virtual_machine_spec.CustomMachineTypeSpec(_COMPONENT, cpus=0,
                                                        memory='7.5GiB')
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.cpus value: "0". Value must be at least 1.'))

  def testInvalidMemory(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      custom_virtual_machine_spec.CustomMachineTypeSpec(
          _COMPONENT, cpus=1, memory=None)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.memory value: "None" (of type "NoneType"). '
        'Value must be one of the following types: basestring.'))


class MachineTypeDecoderTestCase(unittest.TestCase):

  def setUp(self):
    super(MachineTypeDecoderTestCase, self).setUp()
    self.decoder = custom_virtual_machine_spec.MachineTypeDecoder(
        option='machine_type')

  def testDecodeString(self):
    result = self.decoder.Decode('n1-standard-8', _COMPONENT, {})
    self.assertEqual(result, 'n1-standard-8')

  def testDecodeCustomVm(self):
    result = self.decoder.Decode({'cpus': 1, 'memory': '7.5GiB'}, _COMPONENT,
                                 {})
    self.assertIsInstance(result,
                          custom_virtual_machine_spec.CustomMachineTypeSpec)
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)

  def testDecodeInvalidType(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self.decoder.Decode(None, _COMPONENT, {})
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.machine_type value: "None" (of type '
        '"NoneType"). Value must be one of the following types: basestring, '
        'dict.'))

  def testDecodeInvalidValue(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self.decoder.Decode({'cpus': 0, 'memory': '7.5GiB'}, _COMPONENT, {})
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.machine_type.cpus value: "0". Value must be at '
        'least 1.'))


if __name__ == '__main__':
  unittest.main()
