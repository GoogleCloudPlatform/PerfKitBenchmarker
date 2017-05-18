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

"""Tests for perfkitbenchmarker.providers.gcp.gcp_managed_relational_db"""

import contextlib
import mock
import re
import unittest

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gcp_managed_relational_db
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import mock_flags


_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None


#class MemoryDecoderTestCase(unittest.TestCase):
#
#  def setUp(self):
#    super(MemoryDecoderTestCase, self).setUp()
#    self.decoder = gce_virtual_machine.MemoryDecoder(option='memory')
#
#  def testValidStrings(self):
#    self.assertEqual(self.decoder.Decode('1280MiB', _COMPONENT, _FLAGS), 1280)
#    self.assertEqual(self.decoder.Decode('7.5GiB', _COMPONENT, _FLAGS), 7680)
#
#  def testImproperPattern(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      self.decoder.Decode('1280', _COMPONENT, _FLAGS)
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.memory value: "1280". Examples of valid '
#        'values: "1280MiB", "7.5GiB".'))
#
#  def testInvalidFloat(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      self.decoder.Decode('1280.9.8MiB', _COMPONENT, _FLAGS)
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.memory value: "1280.9.8MiB". "1280.9.8" is not '
#        'a valid float.'))
#
#  def testNonIntegerMiB(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      self.decoder.Decode('7.6GiB', _COMPONENT, _FLAGS)
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.memory value: "7.6GiB". The specified size '
#        'must be an integer number of MiB.'))


#class CustomMachineTypeSpecTestCase(unittest.TestCase):
#
#  def testValid(self):
#    result = gce_virtual_machine.CustomMachineTypeSpec(_COMPONENT, cpus=1,
#                                                       memory='7.5GiB')
#    self.assertEqual(result.cpus, 1)
#    self.assertEqual(result.memory, 7680)
#
#  def testMissingCpus(self):
#    with self.assertRaises(errors.Config.MissingOption) as cm:
#      gce_virtual_machine.CustomMachineTypeSpec(_COMPONENT, memory='7.5GiB')
#    self.assertEqual(str(cm.exception), (
#        'Required options were missing from test_component: cpus.'))
#
#  def testMissingMemory(self):
#    with self.assertRaises(errors.Config.MissingOption) as cm:
#      gce_virtual_machine.CustomMachineTypeSpec(_COMPONENT, cpus=1)
#    self.assertEqual(str(cm.exception), (
#        'Required options were missing from test_component: memory.'))
#
#  def testExtraOptions(self):
#    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
#      gce_virtual_machine.CustomMachineTypeSpec(
#          _COMPONENT, cpus=1, memory='7.5GiB', extra1='one', extra2=2)
#    self.assertEqual(str(cm.exception), (
#        'Unrecognized options were found in test_component: extra1, extra2.'))
#
#  def testInvalidCpus(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      gce_virtual_machine.CustomMachineTypeSpec(_COMPONENT, cpus=0,
#                                                memory='7.5GiB')
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.cpus value: "0". Value must be at least 1.'))
#
#  def testInvalidMemory(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      gce_virtual_machine.CustomMachineTypeSpec(_COMPONENT, cpus=1, memory=None)
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.memory value: "None" (of type "NoneType"). '
#        'Value must be one of the following types: basestring.'))


#class MachineTypeDecoderTestCase(unittest.TestCase):
#
#  def setUp(self):
#    super(MachineTypeDecoderTestCase, self).setUp()
#    self.decoder = gce_virtual_machine.MachineTypeDecoder(option='machine_type')
#
#  def testDecodeString(self):
#    result = self.decoder.Decode('n1-standard-8', _COMPONENT, {})
#    self.assertEqual(result, 'n1-standard-8')
#
#  def testDecodeCustomVm(self):
#    result = self.decoder.Decode({'cpus': 1, 'memory': '7.5GiB'}, _COMPONENT,
#                                 {})
#    self.assertIsInstance(result, gce_virtual_machine.CustomMachineTypeSpec)
#    self.assertEqual(result.cpus, 1)
#    self.assertEqual(result.memory, 7680)
#
#  def testDecodeInvalidType(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      self.decoder.Decode(None, _COMPONENT, {})
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.machine_type value: "None" (of type '
#        '"NoneType"). Value must be one of the following types: basestring, '
#        'dict.'))
#
#  def testDecodeInvalidValue(self):
#    with self.assertRaises(errors.Config.InvalidValue) as cm:
#      self.decoder.Decode({'cpus': 0, 'memory': '7.5GiB'}, _COMPONENT, {})
#    self.assertEqual(str(cm.exception), (
#        'Invalid test_component.machine_type.cpus value: "0". Value must be at '
#        'least 1.'))

def _mergeDicts(dict1, dict2):
  result = dict1.copy()
  result.update(dict2)
  return result

class GcpManagedRelationalDbSpecTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.minimal_spec = {
      'cloud': 'GCP',
      'database': 'mysql',
      'vm_spec': {
        'GCP': {
            'machine_type': 'n1-standard-1'
        }
      }
    }

  def testMinimalConfig(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.minimal_spec)
    self.assertEqual(result.database, 'mysql')
    self.assertEqual(result.cloud, 'GCP')
    self.assertIsInstance(result.vm_spec, gce_virtual_machine.GceVmSpec)

  def testDefaultDatabaseName(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.minimal_spec)
    self.assertEqual(result.database_name, 'pkb-db-123')

  def testCustomDatabaseName(self):
    spec = _mergeDicts(self.minimal_spec, {
        'database_name': 'fakename'
    })
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **spec)
    self.assertEqual(result.database_name, 'fakename')

  def testDefaultDatabaseVersion(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.minimal_spec)
    self.assertEqual(result.database_version, '5.6')

  def testCustomDatabaseVersion(self):
    spec = _mergeDicts(self.minimal_spec, {
        'database_version': '6.6'
    })
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **spec)
    self.assertEqual(result.database_version, '6.6')

  def testDefaultDatabasePassword(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.minimal_spec)
    self.assertIsInstance(result.database_password, str)
    self.assertTrue(len(result.database_password) == 10)

  def testRandomDatabasePassword(self):
    spec = _mergeDicts(self.minimal_spec, {
        'database_password': 'fakepassword'
    })
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **spec)
    self.assertEqual(result.database_password, 'fakepassword')


class GceManagedRelationalDbFlagsTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.full_spec = {
      'cloud': 'GCP',
      'database': 'mysql',
      'database_name': 'fake_name',
      'database_password': 'fake_password',
      'vm_spec': {
        'GCP': {
            'machine_type': 'n1-standard-1'
        }
      }
    }

  # Not testing this yet, because it requires the implementation
  # of a managed_relational_db provider for the specified
  # cloud. We could mock it perhaps.
  # Also, this test file is supposed to be specific to GCP.
  # We may want to move the bulk of these tests to a base
  # test file.
  def testCloudFlag(self):
    pass

  def testDatabaseFlag(self):
    self.flags['database'].parse('postgres')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.full_spec)
    self.assertEqual(result.database, 'postgres')

  def testDatabaseNameFlag(self):
    self.flags['database_name'].parse('fakedbname')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.full_spec)
    self.assertEqual(result.database_name, 'fakedbname')

  def testDatabasePasswordFlag(self):
    self.flags['database_password'].parse('fakepassword')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT,
        flag_values = self.flags,
        **self.full_spec)
    self.assertEqual(result.database_password, 'fakepassword')

#class GceVmSpecTestCase(unittest.TestCase):
#
#  def testStringMachineType(self):
#    result = gce_virtual_machine.GceVmSpec(_COMPONENT,
#                                           machine_type='n1-standard-8')
#    self.assertEqual(result.machine_type, 'n1-standard-8')
#    self.assertEqual(result.cpus, None)
#    self.assertEqual(result.memory, None)
#
#  def testCustomMachineType(self):
#    result = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
#        'cpus': 1, 'memory': '7.5GiB'})
#    self.assertEqual(result.machine_type, None)
#    self.assertEqual(result.cpus, 1)
#    self.assertEqual(result.memory, 7680)
#
#  def testStringMachineTypeFlagOverride(self):
#    flags = mock_flags.MockFlags()
#    flags['machine_type'].parse('n1-standard-8')
#    result = gce_virtual_machine.GceVmSpec(
#        _COMPONENT, flag_values=flags,
#        machine_type={'cpus': 1, 'memory': '7.5GiB'})
#    self.assertEqual(result.machine_type, 'n1-standard-8')
#    self.assertEqual(result.cpus, None)
#    self.assertEqual(result.memory, None)
#
#  def testCustomMachineTypeFlagOverride(self):
#    flags = mock_flags.MockFlags()
#    flags['machine_type'].parse('{cpus: 1, memory: 7.5GiB}')
#    result = gce_virtual_machine.GceVmSpec(
#        _COMPONENT, flag_values=flags, machine_type='n1-standard-8')
#    self.assertEqual(result.machine_type, None)
#    self.assertEqual(result.cpus, 1)
#    self.assertEqual(result.memory, 7680)


#class GceVirtualMachineTestCase(unittest.TestCase):
#
#  def setUp(self):
#    p = mock.patch(gce_virtual_machine.__name__ +
#                   '.gce_network.GceNetwork.GetNetwork')
#    self.mock_get_network = p.start()
#    self.addCleanup(p.stop)
#    p = mock.patch(gce_virtual_machine.__name__ +
#                   '.gce_network.GceFirewall.GetFirewall')
#    self.mock_get_firewall = p.start()
#    self.addCleanup(p.stop)
#
#  def testVmWithMachineTypeNonPreemptible(self):
#    spec = gce_virtual_machine.GceVmSpec(
#        _COMPONENT, machine_type='test_machine_type', project='p')
#    vm = gce_virtual_machine.GceVirtualMachine(spec)
#    self.assertEqual(vm.GetMachineTypeDict(), {
#        'dedicated_host': False, 'machine_type': 'test_machine_type',
#        'project': 'p'})
#
#  def testVmWithMachineTypePreemptible(self):
#    spec = gce_virtual_machine.GceVmSpec(
#        _COMPONENT, machine_type='test_machine_type', preemptible=True,
#        project='p')
#    vm = gce_virtual_machine.GceVirtualMachine(spec)
#    self.assertEqual(vm.GetMachineTypeDict(), {
#        'dedicated_host': False, 'machine_type': 'test_machine_type',
#        'preemptible': True, 'project': 'p'})
#
#  def testCustomVmNonPreemptible(self):
#    spec = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
#        'cpus': 1, 'memory': '1.0GiB'}, project='p')
#    vm = gce_virtual_machine.GceVirtualMachine(spec)
#    self.assertEqual(vm.GetMachineTypeDict(),
#                     {'cpus': 1, 'memory_mib': 1024, 'project': 'p',
#                      'dedicated_host': False})
#
#  def testCustomVmPreemptible(self):
#    spec = gce_virtual_machine.GceVmSpec(
#        _COMPONENT, machine_type={'cpus': 1, 'memory': '1.0GiB'},
#        preemptible=True,
#        project='fakeproject')
#    vm = gce_virtual_machine.GceVirtualMachine(spec)
#    self.assertEqual(vm.GetMachineTypeDict(),
#                     {'cpus': 1, 'memory_mib': 1024, 'project': 'fakeproject',
#                      'dedicated_host': False, 'preemptible': True})
#
#
#class GCEVMFlagsTestCase(unittest.TestCase):
#
#  def setUp(self):
#    self._mocked_flags = mock_flags.PatchTestCaseFlags(self)
#    self._mocked_flags.cloud = providers.GCP
#    self._mocked_flags.gcloud_path = 'test_gcloud'
#    self._mocked_flags.os_type = os_types.DEBIAN
#    self._mocked_flags.run_uri = 'aaaaaa'
#    self._mocked_flags.gcp_instance_metadata = []
#    self._mocked_flags.gcp_instance_metadata_from_file = []
#    # Creating a VM object causes network objects to be added to the current
#    # thread's benchmark spec. Create such a benchmark spec for these tests.
#    self.addCleanup(context.SetThreadBenchmarkSpec, None)
#    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
#        _BENCHMARK_NAME, flag_values=self._mocked_flags, vm_groups={})
#    self._benchmark_spec = benchmark_spec.BenchmarkSpec(
#        mock.MagicMock(), config_spec, _BENCHMARK_UID)
#
#  @contextlib.contextmanager
#  def _PatchCriticalObjects(self):
#    """A context manager that patches a few critical objects with mocks."""
#    retval = ('', '', 0)
#    with mock.patch(vm_util.__name__ + '.IssueCommand',
#                    return_value=retval) as issue_command, \
#            mock.patch('__builtin__.open'), \
#            mock.patch(vm_util.__name__ + '.NamedTemporaryFile'), \
#            mock.patch(util.__name__ + '.GetDefaultProject'):
#      yield issue_command
#
#  def testPreemptibleVMFlag(self):
#    with self._PatchCriticalObjects() as issue_command:
#      self._mocked_flags['gce_preemptible_vms'].parse(True)
#      vm_spec = gce_virtual_machine.GceVmSpec(
#          'test_vm_spec.GCP', self._mocked_flags, image='image',
#          machine_type='test_machine_type')
#      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
#      vm._Create()
#      self.assertEquals(issue_command.call_count, 1)
#      self.assertIn('--preemptible', issue_command.call_args[0][0])
#
#  def testImageProjectFlag(self):
#    """Tests that custom image_project flag is supported."""
#    with self._PatchCriticalObjects() as issue_command:
#      self._mocked_flags['image_project'].parse('bar')
#      vm_spec = gce_virtual_machine.GceVmSpec(
#          'test_vm_spec.GCP', self._mocked_flags, image='image',
#          machine_type='test_machine_type')
#      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
#      vm._Create()
#      self.assertEquals(issue_command.call_count, 1)
#      self.assertIn('--image-project bar',
#                    ' '.join(issue_command.call_args[0][0]))
#
#  def testGcpInstanceMetadataFlag(self):
#    with self._PatchCriticalObjects() as issue_command:
#      self._mocked_flags.gcp_instance_metadata = ['k1:v1', 'k2:v2,k3:v3']
#      self._mocked_flags.owner = 'test-owner'
#      vm_spec = gce_virtual_machine.GceVmSpec(
#          'test_vm_spec.GCP', self._mocked_flags, image='image',
#          machine_type='test_machine_type')
#      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
#      vm._Create()
#      self.assertEquals(issue_command.call_count, 1)
#      actual_metadata = re.compile('--metadata\s+(.*)(\s+--)?').search(
#          ' '.join(issue_command.call_args[0][0])).group(1)
#      self.assertIn('k1=v1', actual_metadata)
#      self.assertIn('k2=v2', actual_metadata)
#      self.assertIn('k3=v3', actual_metadata)
#      # Assert that FLAGS.owner is honored and added to instance metadata.
#      self.assertIn('owner=test-owner', actual_metadata)
#
#  def testGcpInstanceMetadataFromFileFlag(self):
#    with self._PatchCriticalObjects() as issue_command:
#      self._mocked_flags.gcp_instance_metadata_from_file = [
#          'k1:p1', 'k2:p2,k3:p3']
#      vm_spec = gce_virtual_machine.GceVmSpec(
#          'test_vm_spec.GCP', self._mocked_flags, image='image',
#          machine_type='test_machine_type')
#      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
#      vm._Create()
#      self.assertEquals(issue_command.call_count, 1)
#      actual_metadata_from_file = re.compile(
#          '--metadata-from-file\s+(.*)(\s+--)?').search(
#          ' '.join(issue_command.call_args[0][0])).group(1)
#      self.assertIn('k1=p1', actual_metadata_from_file)
#      self.assertIn('k2=p2', actual_metadata_from_file)
#      self.assertIn('k3=p3', actual_metadata_from_file)


if __name__ == '__main__':
  unittest.main()
