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

"""Tests for perfkitbenchmarker.providers.gcp.gce_virtual_machine."""

import contextlib
import copy
import json
import re
import unittest

import mock
import mock_flags

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util


_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None

_FAKE_INSTANCE_METADATA = {
    'id': '123456',
    'networkInterfaces': [
        {
            'accessConfigs': [
                {
                    'natIP': '1.2.3.4'
                }
            ],
            'networkIP': '1.2.3.4'
        }
    ]
}
_FAKE_DISK_METADATA = {
    'id': '123456',
    'kind': 'compute#disk',
    'name': 'fakedisk',
    'sizeGb': '10',
    'sourceImage': ''
}


@contextlib.contextmanager
def PatchCriticalObjects(retvals=None):
  """A context manager that patches a few critical objects with mocks."""

  def ReturnVal(*unused_arg, **unused_kwargs):
    del unused_arg
    del unused_kwargs
    return ('', '', 0) if retvals is None else retvals.pop(0)

  with mock.patch(vm_util.__name__ + '.IssueCommand',
                  side_effect=ReturnVal) as issue_command, \
          mock.patch('__builtin__.open'), \
          mock.patch(vm_util.__name__ + '.NamedTemporaryFile'), \
          mock.patch(util.__name__ + '.GetDefaultProject'):
    yield issue_command


class GceVmSpecTestCase(unittest.TestCase):

  def testStringMachineType(self):
    result = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                           machine_type='n1-standard-8')
    self.assertEqual(result.machine_type, 'n1-standard-8')
    self.assertEqual(result.cpus, None)
    self.assertEqual(result.memory, None)

  def testStringMachineTypeWithGpus(self):
    gpu_count = 2
    gpu_type = 'k80'
    result = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                           machine_type='n1-standard-8',
                                           gpu_count=gpu_count,
                                           gpu_type=gpu_type)
    self.assertEqual(result.machine_type, 'n1-standard-8')
    self.assertEqual(result.gpu_type, 'k80')
    self.assertEqual(result.gpu_count, 2)

  def testCustomMachineType(self):
    result = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
        'cpus': 1, 'memory': '7.5GiB'})
    self.assertEqual(result.machine_type, None)
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)

  def testCustomMachineTypeWithGpus(self):
    gpu_count = 2
    gpu_type = 'k80'
    result = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                           machine_type={
                                               'cpus': 1,
                                               'memory': '7.5GiB'
                                           },
                                           gpu_count=gpu_count,
                                           gpu_type=gpu_type)
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)
    self.assertEqual(result.gpu_type, 'k80')
    self.assertEqual(result.gpu_count, 2)

  def testStringMachineTypeFlagOverride(self):
    flags = mock_flags.MockFlags()
    flags['machine_type'].parse('n1-standard-8')
    result = gce_virtual_machine.GceVmSpec(
        _COMPONENT, flag_values=flags,
        machine_type={'cpus': 1, 'memory': '7.5GiB'})
    self.assertEqual(result.machine_type, 'n1-standard-8')
    self.assertEqual(result.cpus, None)
    self.assertEqual(result.memory, None)

  def testCustomMachineTypeFlagOverride(self):
    flags = mock_flags.MockFlags()
    flags['machine_type'].parse('{cpus: 1, memory: 7.5GiB}')
    result = gce_virtual_machine.GceVmSpec(
        _COMPONENT, flag_values=flags, machine_type='n1-standard-8')
    self.assertEqual(result.machine_type, None)
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)


class GceVirtualMachineTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceNetwork.GetNetwork')
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceFirewall.GetFirewall')
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

  def testVmWithMachineTypeNonPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT, machine_type='test_machine_type', project='p')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    self.assertDictContainsSubset(
        {'dedicated_host': False, 'machine_type': 'test_machine_type',
         'project': 'p'},
        vm.GetResourceMetadata()
    )

  def testVmWithMachineTypePreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT, machine_type='test_machine_type', preemptible=True,
        project='p')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    self.assertDictContainsSubset(
        {'dedicated_host': False, 'machine_type': 'test_machine_type',
         'preemptible': True, 'project': 'p'},
        vm.GetResourceMetadata()
    )

  def testCustomVmNonPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
        'cpus': 1, 'memory': '1.0GiB'}, project='p')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    self.assertDictContainsSubset(
        {'cpus': 1, 'memory_mib': 1024, 'project': 'p',
         'dedicated_host': False},
        vm.GetResourceMetadata())

  def testCustomVmPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT, machine_type={'cpus': 1, 'memory': '1.0GiB'},
        preemptible=True,
        project='fakeproject')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    self.assertDictContainsSubset({
        'cpus': 1, 'memory_mib': 1024, 'project': 'fakeproject',
        'dedicated_host': False, 'preemptible': True}, vm.GetResourceMetadata())

  def testCustomVmWithGpus(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT,
        machine_type={'cpus': 1, 'memory': '1.0GiB'},
        gpu_count=2,
        gpu_type='k80',
        project='fakeproject')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    self.assertDictContainsSubset({
        'cpus': 1, 'memory_mib': 1024, 'project': 'fakeproject',
        'dedicated_host': False, 'gpu_count': 2, 'gpu_type': 'k80'
    }, vm.GetResourceMetadata())


def _CreateFakeDiskMetadata(image):
  fake_disk = copy.copy(_FAKE_DISK_METADATA)
  fake_disk['sourceImage'] = image
  return fake_disk


class GceVirtualMachineOsTypesTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceNetwork.GetNetwork')
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceFirewall.GetFirewall')
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)
    self.spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                              machine_type='fake-machine-type')
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.linux_vm.BaseLinuxMixin._GetNumCpus')
    self.mock_get_num_cpus = p.start()
    self.addCleanup(p.stop)

  def _CreateFakeReturnValues(self, fake_image=''):
    fake_rets = [('', '', 0), (json.dumps(_FAKE_INSTANCE_METADATA), '', 0)]
    if fake_image:
      fake_rets.append((json.dumps(_CreateFakeDiskMetadata(fake_image)), '', 0))
    return fake_rets

  def testCreateDebian(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.DEBIAN)
    with PatchCriticalObjects(self._CreateFakeReturnValues()) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--image ubuntu-14-04', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 2)
      self.assertDictContainsSubset({'image': 'ubuntu-14-04'},
                                    vm.GetResourceMetadata())

  def testCreateUbuntu1404(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.UBUNTU1404)
    fake_image = 'fake-ubuntu1404'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image-family ubuntu-1404-lts --image-project ubuntu-os-cloud',
          command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'ubuntu-1404-lts',
                                     'image_project': 'ubuntu-os-cloud'},
                                    vm.GetResourceMetadata())

  def testCreateUbuntu1604(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.UBUNTU1604)
    fake_image = 'fake-ubuntu1604'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image-family ubuntu-1604-lts --image-project ubuntu-os-cloud',
          command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'ubuntu-1604-lts',
                                     'image_project': 'ubuntu-os-cloud'},
                                    vm.GetResourceMetadata())

  def testCreateUbuntu1710(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.UBUNTU1710)
    fake_image = 'fake-ubuntu1704'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image-family ubuntu-1710 --image-project ubuntu-os-cloud',
          command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'ubuntu-1710',
                                     'image_project': 'ubuntu-os-cloud'},
                                    vm.GetResourceMetadata())

  def testCreateRhelCustomImage(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.RHEL)
    fake_image = 'fake-custom-rhel-image'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image)
    with PatchCriticalObjects(self._CreateFakeReturnValues()) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image ' + fake_image,
          command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 2)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image}, vm_metadata)
      self.assertNotIn('image_family', vm_metadata)
      self.assertNotIn('image_project', vm_metadata)

  def testCreateCentos7CustomImage(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.CENTOS7)
    fake_image = 'fake-custom-centos7-image'
    fake_image_project = 'fake-project'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image,
                                         image_project=fake_image_project)
    with PatchCriticalObjects(self._CreateFakeReturnValues()) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--image ' + fake_image, command_string)
      self.assertIn('--image-project ' + fake_image_project, command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 2)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_project': fake_image_project},
                                    vm_metadata)
      self.assertNotIn('image_family', vm_metadata)


class GCEVMFlagsTestCase(unittest.TestCase):

  def setUp(self):
    self._mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self._mocked_flags.cloud = providers.GCP
    self._mocked_flags.gcloud_path = 'test_gcloud'
    self._mocked_flags.os_type = os_types.DEBIAN
    self._mocked_flags.run_uri = 'aaaaaa'
    self._mocked_flags.gcp_instance_metadata = []
    self._mocked_flags.gcp_instance_metadata_from_file = []
    # Creating a VM object causes network objects to be added to the current
    # thread's benchmark spec. Create such a benchmark spec for these tests.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=self._mocked_flags, vm_groups={})
    self._benchmark_spec = benchmark_spec.BenchmarkSpec(
        mock.MagicMock(), config_spec, _BENCHMARK_UID)

  def _CreateVmCommand(self, **flag_kwargs):
    with PatchCriticalObjects() as issue_command:
      for key, value in flag_kwargs.items():
        self._mocked_flags[key].parse(value)
      vm_spec = gce_virtual_machine.GceVmSpec(
          'test_vm_spec.GCP', self._mocked_flags, image='image',
          machine_type='test_machine_type')
      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
      vm._Create()
      return ' '.join(issue_command.call_args[0][0]), issue_command.call_count

  def testPreemptibleVMFlag(self):
    cmd, call_count = self._CreateVmCommand(gce_preemptible_vms=True)
    self.assertEqual(call_count, 1)
    self.assertIn('--preemptible', cmd)

  def testMigrateOnMaintenanceFlagTrueWithGpus(self):
    with self.assertRaises(errors.Config.InvalidValue) as cm:
      self._CreateVmCommand(
          gce_migrate_on_maintenance=True, gpu_count=1, gpu_type='k80')
      self.assertEqual(str(cm.exception), (
          'Cannot set flag gce_migrate_on_maintenance on '
          'instances with GPUs, as it is not supported by GCP.'))

  def testMigrateOnMaintenanceFlagFalseWithGpus(self):
    _, call_count = self._CreateVmCommand(
        gce_migrate_on_maintenance=False, gpu_count=1, gpu_type='k80')
    self.assertEqual(call_count, 1)

  def testAcceleratorTypeOverrideFlag(self):
    cmd, call_count = self._CreateVmCommand(
        gce_accelerator_type_override='fake_type', gpu_count=1, gpu_type='k80')
    self.assertEqual(call_count, 1)
    self.assertIn('--accelerator', cmd)
    self.assertIn('type=fake_type,count=1', cmd)

  def testImageProjectFlag(self):
    """Tests that custom image_project flag is supported."""
    cmd, call_count = self._CreateVmCommand(image_project='bar')
    self.assertEqual(call_count, 1)
    self.assertIn('--image-project bar', cmd)

  def testGcpInstanceMetadataFlag(self):
    cmd, call_count = self._CreateVmCommand(
        gcp_instance_metadata=['k1:v1', 'k2:v2,k3:v3'], owner='test-owner')
    self.assertEqual(call_count, 1)
    actual_metadata = re.compile(
        r'--metadata\s+(.*)(\s+--)?').search(cmd).group(1)
    self.assertIn('k1=v1', actual_metadata)
    self.assertIn('k2=v2', actual_metadata)
    self.assertIn('k3=v3', actual_metadata)
    # Assert that FLAGS.owner is honored and added to instance metadata.
    self.assertIn('owner=test-owner', actual_metadata)

  def testGcpInstanceMetadataFromFileFlag(self):
    cmd, call_count = self._CreateVmCommand(
        gcp_instance_metadata_from_file=['k1:p1', 'k2:p2,k3:p3'])
    self.assertEqual(call_count, 1)
    actual_metadata_from_file = re.compile(
        r'--metadata-from-file\s+(.*)(\s+--)?').search(cmd).group(1)
    self.assertIn('k1=p1', actual_metadata_from_file)
    self.assertIn('k2=p2', actual_metadata_from_file)
    self.assertIn('k3=p3', actual_metadata_from_file)

  def testGceTags(self):
    self.assertIn('--tags perfkitbenchmarker ', self._CreateVmCommand()[0])
    self.assertIn('--tags perfkitbenchmarker,testtag ',
                  self._CreateVmCommand(gce_tags=['testtag'])[0])


class GCEVMCreateTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceNetwork.GetNetwork')
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceFirewall.GetFirewall')
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

  def testVmWithoutGpu(self):
    with PatchCriticalObjects() as issue_command:
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = gce_virtual_machine.GceVirtualMachine(spec)
      vm._Create()
      self.assertEqual(issue_command.call_count, 1)
      self.assertNotIn('--accelerator', issue_command.call_args[0][0])

  def testVmWithGpu(self):
    with PatchCriticalObjects() as issue_command:
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT,
          machine_type='n1-standard-8',
          gpu_count=2,
          gpu_type='k80')
      vm = gce_virtual_machine.GceVirtualMachine(spec)
      vm._Create()
      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('--accelerator', issue_command.call_args[0][0])
      self.assertIn('type=nvidia-tesla-k80,count=2',
                    issue_command.call_args[0][0])
      self.assertIn('--maintenance-policy', issue_command.call_args[0][0])
      self.assertIn('TERMINATE', issue_command.call_args[0][0])


if __name__ == '__main__':
  unittest.main()
