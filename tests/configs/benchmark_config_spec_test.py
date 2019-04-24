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
"""Tests for perfkitbenchmarker.configs.benchmark_config_spec."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest

import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from tests import pkb_common_test_case
from six.moves import range

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'
_OPTION = 'test_option'
_GCP_ONLY_VM_CONFIG = {'GCP': {'machine_type': 'n1-standard-1'}}
_GCP_AWS_VM_CONFIG = {'GCP': {'machine_type': 'n1-standard-1'},
                      'AWS': {'machine_type': 'm4.large'}}
_GCP_AWS_DISK_CONFIG = {'GCP': {}, 'AWS': {}}


def _GetFlagDict(flag_values):
  return {name: flag_values[name] for name in flag_values}


class PerCloudConfigSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(PerCloudConfigSpecTestCase, self).setUp()
    self._spec_class = option_decoders._PerCloudConfigSpec

  def testDefaults(self):
    spec = self._spec_class(_COMPONENT)
    for cloud in providers.VALID_CLOUDS:
      self.assertIsNone(getattr(spec, cloud))

  def testDict(self):
    spec = self._spec_class(_COMPONENT, GCP={})
    self.assertEqual(spec.GCP, {})
    for cloud in frozenset(providers.VALID_CLOUDS).difference([providers.GCP]):
      self.assertIsNone(getattr(spec, cloud))

  def testNonDict(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, GCP=[])
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.GCP value: "[]" (of type "list"). Value must '
        'be one of the following types: dict.'))

  def testUnrecognizedCloud(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      self._spec_class(_COMPONENT, fake_provider={})
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component: fake_provider.'))


class PerCloudConfigDecoderTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(PerCloudConfigDecoderTestCase, self).setUp()
    self._decoder = option_decoders.PerCloudConfigDecoder(option=_OPTION)

  def testRejectNone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._decoder.Decode(None, _COMPONENT, {})

  def testAcceptNone(self):
    decoder = option_decoders.PerCloudConfigDecoder(none_ok=True,
                                                    option=_OPTION)
    self.assertIsNone(decoder.Decode(None, _COMPONENT, {}))

  def testEmptyDict(self):
    result = self._decoder.Decode({}, _COMPONENT, {})
    self.assertIsInstance(result, option_decoders._PerCloudConfigSpec)
    self.assertEqual(result.__dict__, {
        cloud: None for cloud in providers.VALID_CLOUDS})

  def testNonEmptyDict(self):
    result = self._decoder.Decode(_GCP_ONLY_VM_CONFIG, _COMPONENT, {})
    self.assertIsInstance(result, option_decoders._PerCloudConfigSpec)
    expected_attributes = {cloud: None for cloud in providers.VALID_CLOUDS}
    expected_attributes['GCP'] = {'machine_type': 'n1-standard-1'}
    self.assertEqual(result.__dict__, expected_attributes)


class StaticVmDecoderTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(StaticVmDecoderTestCase, self).setUp()
    self._decoder = benchmark_config_spec._StaticVmDecoder()

  def testNone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._decoder.Decode(None, _COMPONENT, {})

  def testValidInput(self):
    result = self._decoder.Decode({'ssh_port': 111}, _COMPONENT, {})
    self.assertIsInstance(result, static_virtual_machine.StaticVmSpec)
    self.assertEqual(result.ssh_port, 111)

  def testVmSpecFlag(self):
    FLAGS.install_packages = False
    FLAGS['install_packages'].present = True
    result = self._decoder.Decode({}, _COMPONENT, FLAGS)
    self.assertFalse(result.install_packages)

  def testDiskSpecFlag(self):
    FLAGS.scratch_dir = '/path/from/flag'
    FLAGS['scratch_dir'].present = True
    result = self._decoder.Decode({
        'disk_specs': [{
            'mount_point': '/path/from/spec'
        }]
    }, _COMPONENT, FLAGS)
    self.assertEqual(result.disk_specs[0].mount_point, '/path/from/flag')


class StaticVmListDecoderTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(StaticVmListDecoderTestCase, self).setUp()
    self._decoder = benchmark_config_spec._StaticVmListDecoder()

  def testNone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._decoder.Decode(None, _COMPONENT, {})

  def testValidList(self):
    input_list = [{'ssh_port': i} for i in range(3)]
    result = self._decoder.Decode(input_list, _COMPONENT, {})
    self.assertIsInstance(result, list)
    self.assertEqual([vm_spec.ssh_port for vm_spec in result], list(range(3)))

  def testInvalidList(self):
    input_list = [{'ssh_port': 0}, {'ssh_port': 1}, {'ssh_pory': 2}]
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      self._decoder.Decode(input_list, _COMPONENT, {})
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component[2]: ssh_pory.'))


class VmGroupSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(VmGroupSpecTestCase, self).setUp()
    self._spec_class = benchmark_config_spec._VmGroupSpec
    self._kwargs = {'cloud': providers.GCP, 'os_type': os_types.DEBIAN,
                    'vm_spec': _GCP_AWS_VM_CONFIG}

  def testMissingValues(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      self._spec_class(_COMPONENT)
    self.assertEqual(str(cm.exception), (
        'Required options were missing from test_component: cloud, os_type, '
        'vm_spec.'))

  def testDefaults(self):
    result = self._spec_class(_COMPONENT, **self._kwargs)
    self.assertIsInstance(result, benchmark_config_spec._VmGroupSpec)
    self.assertEqual(result.cloud, 'GCP')
    self.assertEqual(result.disk_count, 1)
    self.assertIsNone(result.disk_spec)
    self.assertEqual(result.os_type, 'debian')
    self.assertEqual(result.static_vms, [])
    self.assertEqual(result.vm_count, 1)
    self.assertIsInstance(result.vm_spec, gce_virtual_machine.GceVmSpec)

  def testInvalidCloud(self):
    self._kwargs['cloud'] = 'fake_provider'
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.cloud value: "fake_provider". Value must be '
        'one of the following: {0}.'.format(', '.join(providers.VALID_CLOUDS))))

  def testInvalidDiskCount(self):
    self._kwargs['disk_count'] = -1
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.disk_count value: "-1". '
        'Value must be at least 0.'))

  def testInvalidDiskSpec(self):
    self._kwargs['disk_spec'] = {'GCP': None}
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.disk_spec.GCP value: "None" (of type '
        '"NoneType"). Value must be one of the following types: dict.'))

  def testInvalidOsType(self):
    self._kwargs['os_type'] = 'fake_os_type'
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.os_type value: "fake_os_type". Value must be '
        'one of the following: {0}.'.format(', '.join(os_types.ALL))))

  def testInvalidStaticVms(self):
    self._kwargs['static_vms'] = [{'fake_option': None}]
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component.static_vms[0]: '
        'fake_option.'))

  def testInvalidVmCount(self):
    self._kwargs['vm_count'] = None
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.vm_count value: "None" (of type "NoneType"). '
        'Value must be one of the following types: int.'))
    self._kwargs['vm_count'] = -1
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.vm_count value: "-1". '
        'Value must be at least 0.'))

  def testInvalidVmSpec(self):
    self._kwargs['vm_spec'] = {'GCP': None}
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.vm_spec.GCP value: "None" (of type '
        '"NoneType"). Value must be one of the following types: dict.'))

  def testValidInput(self):
    result = self._spec_class(
        _COMPONENT, cloud=providers.AWS, disk_count=0,
        disk_spec=_GCP_AWS_DISK_CONFIG, os_type=os_types.WINDOWS,
        static_vms=[{}], vm_count=0, vm_spec=_GCP_AWS_VM_CONFIG)
    self.assertIsInstance(result, benchmark_config_spec._VmGroupSpec)
    self.assertEqual(result.cloud, 'AWS')
    self.assertEqual(result.disk_count, 0)
    self.assertIsInstance(result.disk_spec, aws_disk.AwsDiskSpec)
    self.assertEqual(result.os_type, 'windows')
    self.assertIsInstance(result.static_vms, list)
    self.assertEqual(len(result.static_vms), 1)
    self.assertIsInstance(result.static_vms[0],
                          static_virtual_machine.StaticVmSpec)
    self.assertEqual(result.vm_count, 0)
    self.assertIsInstance(result.vm_spec, virtual_machine.BaseVmSpec)

  def testMissingCloudDiskConfig(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      self._spec_class(_COMPONENT, cloud=providers.GCP, os_type=os_types.DEBIAN,
                       disk_spec={}, vm_spec=_GCP_AWS_VM_CONFIG)
    self.assertEqual(str(cm.exception), (
        'test_component.cloud is "GCP", but test_component.disk_spec does not '
        'contain a configuration for "GCP".'))

  def testMissingCloudVmConfig(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      self._spec_class(_COMPONENT, cloud=providers.GCP, os_type=os_types.DEBIAN,
                       vm_spec={})
    self.assertEqual(str(cm.exception), (
        'test_component.cloud is "GCP", but test_component.vm_spec does not '
        'contain a configuration for "GCP".'))

  def createNonPresentFlags(self):
    FLAGS.cloud = providers.AWS
    FLAGS.num_vms = 3
    FLAGS.os_type = os_types.WINDOWS

  def createPresentFlags(self):
    self.createNonPresentFlags()
    FLAGS['cloud'].present = True
    FLAGS['num_vms'].present = True
    FLAGS['os_type'].present = True

  def testPresentFlagsAndPresentConfigValues(self):
    self.createPresentFlags()
    result = self._spec_class(
        _COMPONENT, flag_values=FLAGS, vm_count=2, **self._kwargs)
    self.assertEqual(result.cloud, 'AWS')
    self.assertEqual(result.os_type, 'windows')
    self.assertEqual(result.vm_count, 2)

  def testPresentFlagsAndNonPresentConfigValues(self):
    self.createPresentFlags()
    result = self._spec_class(
        _COMPONENT, flag_values=FLAGS, vm_spec=_GCP_AWS_VM_CONFIG)
    self.assertEqual(result.cloud, 'AWS')
    self.assertEqual(result.os_type, 'windows')
    self.assertEqual(result.vm_count, 1)

  def testNonPresentFlagsAndPresentConfigValues(self):
    self.createNonPresentFlags()
    result = self._spec_class(
        _COMPONENT, flag_values=self.createNonPresentFlags(), vm_count=2,
        **self._kwargs)
    self.assertEqual(result.cloud, 'GCP')
    self.assertEqual(result.os_type, 'debian')
    self.assertEqual(result.vm_count, 2)

  def testVmCountNone(self):
    self.createNonPresentFlags()
    result = self._spec_class(
        _COMPONENT, vm_count=None, flag_values=FLAGS, **self._kwargs)
    self.assertEqual(result.vm_count, 3)

  def testCallsLoadProviderAndChecksRequirements(self):
    self.createNonPresentFlags()
    FLAGS.ignore_package_requirements = False
    with mock.patch(providers.__name__ + '.LoadProvider'):
      self._spec_class(_COMPONENT, flag_values=FLAGS, **self._kwargs)
      providers.LoadProvider.assert_called_once_with('GCP', False)

  def testCallsLoadProviderAndIgnoresRequirements(self):
    self.createNonPresentFlags()
    FLAGS.ignore_package_requirements = True
    with mock.patch(providers.__name__ + '.LoadProvider'):
      self._spec_class(_COMPONENT, flag_values=FLAGS, **self._kwargs)
      providers.LoadProvider.assert_called_once_with('GCP', True)


class VmGroupsDecoderTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(VmGroupsDecoderTestCase, self).setUp()
    self._decoder = benchmark_config_spec._VmGroupsDecoder()

  def testNone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._decoder.Decode(None, _COMPONENT, {})

  def testValidInput(self):
    result = self._decoder.Decode({
        'default': {'cloud': providers.GCP, 'os_type': os_types.DEBIAN,
                    'vm_spec': _GCP_AWS_VM_CONFIG}}, _COMPONENT, {})
    self.assertIsInstance(result, dict)
    self.assertEqual(len(result), 1)
    self.assertIsInstance(result['default'], benchmark_config_spec._VmGroupSpec)
    self.assertEqual(result['default'].cloud, 'GCP')
    self.assertEqual(result['default'].os_type, 'debian')
    self.assertIsInstance(result['default'].vm_spec,
                          gce_virtual_machine.GceVmSpec)

  def testInvalidInput(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      self._decoder.Decode(
          {'default': {'cloud': providers.GCP, 'os_type': os_types.DEBIAN,
                       'static_vms': [{}, {'fake_option': 1.2}],
                       'vm_spec': _GCP_AWS_VM_CONFIG}},
          _COMPONENT, {})
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in '
        'test_component.default.static_vms[1]: fake_option.'))


class CloudRedisDecoderTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(CloudRedisDecoderTestCase, self).setUp()
    self._decoder = benchmark_config_spec._CloudRedisDecoder()
    FLAGS.cloud = providers.GCP
    FLAGS.run_uri = 'test'

  def testNone(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._decoder.Decode(None, _COMPONENT, {})

  def testValidInput(self):
    result = self._decoder.Decode({
        'redis_version': 'redis_3_2'
    }, _COMPONENT, FLAGS)
    self.assertIsInstance(result, benchmark_config_spec._CloudRedisSpec)
    self.assertEqual(result.redis_version, 'redis_3_2')

  def testInvalidInput(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      self._decoder.Decode({'foo': 'bar'}, _COMPONENT, FLAGS)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in '
        'test_component: foo.'))


class CloudRedisSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(CloudRedisSpecTestCase, self).setUp()
    self._spec_class = benchmark_config_spec._CloudRedisSpec

  def testMissingValues(self):
    with self.assertRaises(errors.Config.MissingOption) as cm:
      self._spec_class(_COMPONENT)
    self.assertEqual(str(cm.exception), (
        'Required options were missing from test_component: cloud.'))

  def testDefaults(self):
    result = self._spec_class(_COMPONENT, flag_values=FLAGS)
    self.assertIsInstance(result, benchmark_config_spec._CloudRedisSpec)
    self.assertEqual(result.redis_version, 'redis_3_2')


class BenchmarkConfigSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(BenchmarkConfigSpecTestCase, self).setUp()

    self._spec_class = benchmark_config_spec.BenchmarkConfigSpec
    self._description = 'Test description.'
    self._vm_groups = {'default': {'cloud': providers.GCP,
                                   'os_type': os_types.DEBIAN,
                                   'vm_spec': _GCP_AWS_VM_CONFIG}}
    self._kwargs = {'description': self._description,
                    'vm_groups': self._vm_groups}

  def testValidInput(self):
    result = self._spec_class(_COMPONENT, flag_values=FLAGS, **self._kwargs)
    self.assertIsInstance(result, benchmark_config_spec.BenchmarkConfigSpec)
    self.assertEqual(result.description, 'Test description.')
    self.assertIsNot(result.flags, _GetFlagDict(flags.FLAGS))
    self.assertIsInstance(result.vm_groups, dict)
    self.assertEqual(len(result.vm_groups), 1)
    self.assertIsInstance(result.vm_groups['default'],
                          benchmark_config_spec._VmGroupSpec)
    self.assertEqual(result.vm_groups['default'].cloud, 'GCP')
    self.assertEqual(result.vm_groups['default'].os_type, 'debian')
    self.assertIsInstance(result.vm_groups['default'].vm_spec,
                          gce_virtual_machine.GceVmSpec)

  def testInvalidVmGroups(self):
    self._kwargs['vm_groups']['default']['static_vms'] = [{'disk_specs': [{
        'disk_size': 0.5}]}]
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(_COMPONENT, flag_values=FLAGS, **self._kwargs)
    self.assertEqual(str(cm.exception), (
        'Invalid test_component.vm_groups.default.static_vms[0].disk_specs[0]'
        '.disk_size value: "0.5" (of type "float"). Value must be one of the '
        'following types: NoneType, int.'))

  def testMismatchedOsTypes(self):
    self._kwargs['vm_groups'] = {
        os_type + '_group': {'os_type': os_type, 'vm_spec': _GCP_AWS_VM_CONFIG}
        for os_type in (os_types.DEBIAN, os_types.RHEL, os_types.WINDOWS)}
    expected_os_types = os_types.JUJU, os_types.WINDOWS
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._spec_class(
          _COMPONENT,
          expected_os_types=expected_os_types,
          flag_values=FLAGS,
          **self._kwargs)
    self.assertEqual(str(cm.exception), (
        "VM groups in test_component may only have the following OS types: "
        "'juju', 'windows'. The following VM group options are invalid:{sep}"
        "test_component.vm_groups['debian_group'].os_type: 'debian'{sep}"
        "test_component.vm_groups['rhel_group'].os_type: 'rhel'".format(
            sep=os.linesep)))

  def testFlagOverridesPropagate(self):
    self._kwargs['flags'] = {'cloud': providers.AWS,
                             'ignore_package_requirements': True}
    result = self._spec_class(_COMPONENT, flag_values=FLAGS, **self._kwargs)
    self.assertIsInstance(result, benchmark_config_spec.BenchmarkConfigSpec)
    self.assertEqual(result.description, 'Test description.')
    self.assertIsInstance(result.flags, dict)
    self.assertIsNot(result.flags, _GetFlagDict(flags.FLAGS))
    self.assertEqual(result.flags['cloud'], 'AWS')
    self.assertEqual(FLAGS['cloud'].value, 'GCP')
    self.assertIsInstance(result.vm_groups, dict)
    self.assertEqual(len(result.vm_groups), 1)
    self.assertIsInstance(result.vm_groups['default'],
                          benchmark_config_spec._VmGroupSpec)
    self.assertEqual(result.vm_groups['default'].cloud, 'AWS')
    self.assertEqual(result.vm_groups['default'].os_type, 'debian')
    self.assertIsInstance(result.vm_groups['default'].vm_spec,
                          virtual_machine.BaseVmSpec)


if __name__ == '__main__':
  unittest.main()
