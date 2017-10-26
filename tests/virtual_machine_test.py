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
"""Tests for perfkitbenchmarker.virtual_machine."""

import unittest
import mock_flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders


_COMPONENT = 'test_component'


class TestVmSpec(virtual_machine.BaseVmSpec):

  CLOUD = 'test_cloud'

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(TestVmSpec, cls)._GetOptionDecoderConstructions()
    result['required_string'] = (option_decoders.StringDecoder, {})
    result['required_int'] = (option_decoders.IntDecoder, {})
    return result


class BaseVmSpecTestCase(unittest.TestCase):

  def testDefaults(self):
    spec = virtual_machine.BaseVmSpec(_COMPONENT)
    self.assertEqual(spec.image, None)
    self.assertEqual(spec.install_packages, True)
    self.assertEqual(spec.machine_type, None)
    self.assertEqual(spec.zone, None)

  def testProvidedValid(self):
    spec = virtual_machine.BaseVmSpec(
        _COMPONENT, image='test_image', install_packages=False,
        machine_type='test_machine_type', zone='test_zone')
    self.assertEqual(spec.image, 'test_image')
    self.assertEqual(spec.install_packages, False)
    self.assertEqual(spec.machine_type, 'test_machine_type')
    self.assertEqual(spec.zone, 'test_zone')

  def testUnrecognizedOptions(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      virtual_machine.BaseVmSpec(_COMPONENT, color='red', flavor='cherry',
                                 texture=None)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component: color, flavor, '
        'texture.'))

  def testMissingOptions(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      TestVmSpec(_COMPONENT)
    self.assertEqual(str(cm.exception), (
        'Required options were missing from test_component: required_int, '
        'required_string.'))

  def testInvalidImage(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(_COMPONENT, image=0)

  def testInvalidInstallPackages(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(_COMPONENT, install_packages='yes')

  def testInvalidMachineType(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(_COMPONENT, machine_type=True)

  def testInvalidZone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(_COMPONENT, zone=0)

  def testGpus(self):
    gpu_count = 2
    gpu_type = 'k80'
    result = virtual_machine.BaseVmSpec(_COMPONENT,
                                        gpu_count=gpu_count,
                                        gpu_type=gpu_type)
    self.assertEqual(result.gpu_type, 'k80')
    self.assertEqual(result.gpu_count, 2)

  def testMissingGpuCount(self):
    flags = mock_flags.MockFlags()
    with self.assertRaises(errors.Config.MissingOption) as cm:
      virtual_machine.BaseVmSpec(_COMPONENT,
                                 flag_values=flags,
                                 gpu_type='k80')
    self.assertEqual(str(cm.exception), (
        'gpu_count must be specified if gpu_type is set'))

  def testMissingGpuType(self):
    flags = mock_flags.MockFlags()
    with self.assertRaises(errors.Config.MissingOption) as cm:
      virtual_machine.BaseVmSpec(_COMPONENT,
                                 flag_values=flags,
                                 gpu_count=1)

    self.assertEqual(str(cm.exception), (
        'gpu_type must be specified if gpu_count is set'))

  def testInvalidGpuType(self):
    flags = mock_flags.MockFlags()
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      virtual_machine.BaseVmSpec(_COMPONENT,
                                 flag_values=flags,
                                 gpu_count=1,
                                 gpu_type='bad_type')

    self.assertIn((
        'Invalid test_component.gpu_type value: "bad_type". '
        'Value must be one of the following:'), str(cm.exception))

    self.assertIn('k80', str(cm.exception))
    self.assertIn('p100', str(cm.exception))

  def testInvalidGpuCount(self):
    flags = mock_flags.MockFlags()
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      virtual_machine.BaseVmSpec(_COMPONENT,
                                 flag_values=flags,
                                 gpu_count=0,
                                 gpu_type='k80')

    self.assertEqual(str(cm.exception), (
        'Invalid test_component.gpu_count value: "0". '
        'Value must be at least 1.'))


if __name__ == '__main__':
  unittest.main()
