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
"""Tests for perfkitbenchmarker.virtual_machine."""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders


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
    spec = virtual_machine.BaseVmSpec()
    self.assertEqual(spec.image, None)
    self.assertEqual(spec.install_packages, True)
    self.assertEqual(spec.machine_type, None)
    self.assertEqual(spec.zone, None)

  def testProvidedValid(self):
    spec = virtual_machine.BaseVmSpec(
        image='test_image', install_packages=False,
        machine_type='test_machine_type', zone='test_zone')
    self.assertEqual(spec.image, 'test_image')
    self.assertEqual(spec.install_packages, False)
    self.assertEqual(spec.machine_type, 'test_machine_type')
    self.assertEqual(spec.zone, 'test_zone')

  def testUnrecognizedOptions(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      virtual_machine.BaseVmSpec(color='red', flavor='cherry', texture=None)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in a VM config: color, flavor, '
        'texture.'))

  def testMissingOptions(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      TestVmSpec()
    self.assertEqual(str(cm.exception), (
        'Required options were missing from a test_cloud VM config: '
        'required_int, required_string.'))

  def testInvalidImage(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(image=0)

  def testInvalidInstallPackages(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(install_packages='yes')

  def testInvalidMachineType(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(machine_type=True)

  def testInvalidZone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      virtual_machine.BaseVmSpec(zone=0)


if __name__ == '__main__':
  unittest.main()
