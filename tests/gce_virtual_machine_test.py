# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS

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

  with mock.patch(
      vm_util.__name__ + '.IssueCommand',
      side_effect=ReturnVal) as issue_command, mock.patch(
          builtins.__name__ + '.open'), mock.patch(
              vm_util.__name__ +
              '.NamedTemporaryFile'), mock.patch(util.__name__ +
                                                 '.GetDefaultProject'):
    yield issue_command


class GceVmSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

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
    FLAGS['machine_type'].parse('n1-standard-8')
    result = gce_virtual_machine.GceVmSpec(
        _COMPONENT,
        flag_values=FLAGS,
        machine_type={
            'cpus': 1,
            'memory': '7.5GiB'
        })
    self.assertEqual(result.machine_type, 'n1-standard-8')
    self.assertEqual(result.cpus, None)
    self.assertEqual(result.memory, None)

  def testCustomMachineTypeFlagOverride(self):
    FLAGS['machine_type'].parse('{cpus: 1, memory: 7.5GiB}')
    result = gce_virtual_machine.GceVmSpec(
        _COMPONENT, flag_values=FLAGS, machine_type='n1-standard-8')
    self.assertEqual(result.machine_type, None)
    self.assertEqual(result.cpus, 1)
    self.assertEqual(result.memory, 7680)


class GceVirtualMachineTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GceVirtualMachineTestCase, self).setUp()
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceNetwork.GetNetwork')
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceFirewall.GetFirewall')
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir')
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

  def testVmWithMachineTypeNonPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT, machine_type='test_machine_type', project='p')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    vm.created = True
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
    vm.created = True
    self.assertDictContainsSubset(
        {'dedicated_host': False, 'machine_type': 'test_machine_type',
         'preemptible': True, 'project': 'p'},
        vm.GetResourceMetadata()
    )

  def testCustomVmNonPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
        'cpus': 1, 'memory': '1.0GiB'}, project='p')
    vm = gce_virtual_machine.GceVirtualMachine(spec)
    vm.created = True
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
    vm.created = True
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
    vm.created = True
    self.assertDictContainsSubset({
        'cpus': 1, 'memory_mib': 1024, 'project': 'fakeproject',
        'dedicated_host': False, 'gpu_count': 2, 'gpu_type': 'k80'
    }, vm.GetResourceMetadata())


def _CreateFakeDiskMetadata(image):
  fake_disk = copy.copy(_FAKE_DISK_METADATA)
  fake_disk['sourceImage'] = image
  return fake_disk


class GceVirtualMachineOsTypesTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GceVirtualMachineOsTypesTestCase, self).setUp()
    FLAGS.gcp_instance_metadata_from_file = ''
    FLAGS.gcp_instance_metadata = ''
    FLAGS.gcloud_path = 'gcloud'

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

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir')
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

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
      vm.created = True
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
      vm.created = True
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
      vm.created = True
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

  def testCreateUbuntu1804(self):
    vm_class = virtual_machine.GetVmClass(providers.GCP, os_types.UBUNTU1804)
    fake_image = 'fake-ubuntu1804'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image-family ubuntu-1804-lts --image-project ubuntu-os-cloud',
          command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'ubuntu-1804-lts',
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
      vm.created = True
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
      vm.created = True
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


class GCEVMFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GCEVMFlagsTestCase, self).setUp()
    FLAGS.cloud = providers.GCP
    FLAGS.gcloud_path = 'test_gcloud'
    FLAGS.os_type = os_types.DEBIAN
    FLAGS.run_uri = 'aaaaaa'
    FLAGS.gcp_instance_metadata = []
    FLAGS.gcp_instance_metadata_from_file = []
    # Creating a VM object causes network objects to be added to the current
    # thread's benchmark spec. Create such a benchmark spec for these tests.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=FLAGS, vm_groups={})
    self._benchmark_spec = benchmark_spec.BenchmarkSpec(
        mock.MagicMock(), config_spec, _BENCHMARK_UID)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir')
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

  def _CreateVmCommand(self, **flag_kwargs):
    with PatchCriticalObjects() as issue_command:
      for key, value in flag_kwargs.items():
        FLAGS[key].parse(value)
      vm_spec = gce_virtual_machine.GceVmSpec(
          'test_vm_spec.GCP',
          FLAGS,
          image='image',
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

  def testNetworkTierFlagPremium(self):
    """Tests that the premium network tier flag is supported."""
    cmd, call_count = self._CreateVmCommand(gce_network_tier='premium')
    self.assertEqual(call_count, 1)
    self.assertIn('--network-tier PREMIUM', cmd)

  def testNetworkTierFlagStandard(self):
    """Tests that the standard network tier flag is supported."""
    cmd, call_count = self._CreateVmCommand(gce_network_tier='standard')
    self.assertEqual(call_count, 1)
    self.assertIn('--network-tier STANDARD', cmd)

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

  def testShieldedSecureBootFlag(self):
    """Tests that the custom shielded secure boot flag is supported."""
    cmd, call_count = self._CreateVmCommand(
        gce_shielded_secure_boot=True)
    self.assertEqual(call_count, 1)
    self.assertIn('--shielded-secure-boot', cmd)


class GCEVMCreateTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GCEVMCreateTestCase, self).setUp()
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceNetwork.GetNetwork')
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(gce_virtual_machine.__name__ +
                   '.gce_network.GceFirewall.GetFirewall')
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir')
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testCreateRateLimitedMachineCreated(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'The resource already exists', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = gce_virtual_machine.GceVirtualMachine(spec)
      vm._Create()  # No error should be thrown.
      self.assertEqual(issue_command.call_count, 5)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testCreateRateLimitedMachineCreatedFailure(self, mock_cmd):
    fake_rets = []
    for _ in range(0, 100):
      fake_rets.append(('stdout', 'Rate Limit Exceeded', 1))
    with PatchCriticalObjects(fake_rets) as issue_command:
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = gce_virtual_machine.GceVirtualMachine(spec)
      with self.assertRaises(
          errors.Benchmarks.QuotaFailure.RateLimitExceededError):
        vm._Create()
      self.assertEqual(issue_command.call_count,
                       util.RATE_LIMITED_MAX_RETRIES + 1)

  def testCreateVMAlreadyExists(self):
    fake_rets = [('stdout', 'The resource already exists', 1)]
    with PatchCriticalObjects(fake_rets):
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = gce_virtual_machine.GceVirtualMachine(spec)
      with self.assertRaises(errors.Resource.CreationError):
        vm._Create()

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


class GceFirewallRuleTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleSuccessfulAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'some warning perhaps', 0)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                       'network_name')
      fr._Create()
      self.assertEqual(issue_command.call_count, 2)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleGenericErrorAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'some random firewall error', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      with self.assertRaises(errors.VmUtil.IssueCommandError):
        fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                         'network_name')
        fr._Create()
      self.assertEqual(issue_command.call_count, 3)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleAlreadyExistsAfterRateLimited(self, mock_cmd):
    fake_rets = [('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'Rate Limit Exceeded', 1),
                 ('stdout', 'firewall already exists', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                       'network_name')
      fr._Create()
      self.assertEqual(issue_command.call_count, 3)

  @mock.patch('time.sleep', side_effect=lambda _: None)
  def testGceFirewallRuleGenericError(self, mock_cmd):
    fake_rets = [('stdout', 'some random firewall error', 1)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      with self.assertRaises(errors.VmUtil.IssueCommandError):
        fr = gce_network.GceFirewallRule('name', 'project', 'allow',
                                         'network_name')
        fr._Create()
      self.assertEqual(issue_command.call_count, 1)


class GceNetworkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GceNetworkTest, self).setUp()
    # need a benchmarkspec in the context to run
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        'cluster_boot', flag_values=FLAGS)
    benchmark_spec.BenchmarkSpec(mock.Mock(), config_spec, 'uid')

  def testGetNetwork(self):
    project = 'myproject'
    zone = 'us-east1-a'
    vm = mock.Mock(zone=zone, project=project)
    net = gce_network.GceNetwork.GetNetwork(vm)
    self.assertEqual(project, net.project)
    self.assertEqual(zone, net.zone)


if __name__ == '__main__':
  unittest.main()
