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

import builtins
import contextlib
import copy
import json
import re
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

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
    'sourceImage': '',
    'type': 'pd-standard'
}


_COMPUTE_DESCRIBE_RUNNING = r"""{
  "id": "758013403901965936",
  "networkInterfaces": [
    {
      "accessConfigs": [
        {
          "natIP": "35.227.176.232"
        }
      ],
      "networkIP": "10.138.0.113"
    }
  ],
  "status": "RUNNING"
}
"""


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
          builtins.__name__ +
          '.open'), mock.patch(vm_util.__name__ +
                               '.NamedTemporaryFile'):
    yield issue_command


class GceVmSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testStringMachineType(self):
    result = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                           machine_type='n1-standard-8')
    self.assertEqual(result.machine_type, 'n1-standard-8')
    self.assertIsNone(result.cpus)
    self.assertIsNone(result.memory)

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
    self.assertIsNone(result.machine_type)
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
    self.assertIsNone(result.cpus)
    self.assertIsNone(result.memory)

  def testCustomMachineTypeFlagOverride(self):
    FLAGS['machine_type'].parse('{cpus: 1, memory: 7.5GiB}')
    result = gce_virtual_machine.GceVmSpec(
        _COMPONENT, flag_values=FLAGS, machine_type='n1-standard-8')
    self.assertIsNone(result.machine_type)
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
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
    vm.created = True
    self.assertDictContainsSubset(
        {'dedicated_host': False, 'machine_type': 'test_machine_type',
         'preemptible': True, 'project': 'p'},
        vm.GetResourceMetadata()
    )

  def testCustomVmNonPreemptible(self):
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT, machine_type={
        'cpus': 1, 'memory': '1.0GiB'}, project='p')
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
    vm.created = True
    self.assertDictContainsSubset({
        'cpus': 1, 'memory_mib': 1024, 'project': 'fakeproject',
        'dedicated_host': False, 'gpu_count': 2, 'gpu_type': 'k80'
    }, vm.GetResourceMetadata())

  @parameterized.named_parameters(
      ('custom', {
          'cpus': 1,
          'memory': '1.0GiB'
      }, 'skylake', 'skylake'),
      ('n1', 'n1-standard-2', 'skylake', 'skylake'),
  )
  def testGenerateCreateCommand(self, machine_type, min_cpu_platform_flag,
                                min_cpu_platform_in_command):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT,
        machine_type=machine_type,
        gpu_count=2,
        gpu_type='t4',
        project='fakeproject',
        min_cpu_platform=min_cpu_platform_flag)
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
    gcloud_cmd = vm._GenerateCreateCommand('x')
    self.assertEqual(min_cpu_platform_in_command,
                     gcloud_cmd.flags.get('min-cpu-platform'))

  def testUpdateInterruptibleVmStatus(self):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT, machine_type='test_machine_type', preemptible=True,
        project='p')
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
    vm.created = True
    vm.name = 'pkb-1234-0'
    vm.zone = 'asia-northeast2-a'
    stdout = """
    [{
        "endTime": "2021-03-12T10:38:00.048-08:00",
        "id": "5583038217590279140",
        "insertTime": "2021-03-12T10:38:00.048-08:00",
        "kind": "compute#operation",
        "name": "systemevent-1815574280048-5bd5b33121158-c654ffc9-0ff0dc28",
        "operationType": "compute.instances.preempted",
        "progress": 100,
        "selfLink": "https://www.googleapis.com/compute/v1/projects/my-project/zones/asia-northeast2-a/operations/systemevent-1815574280048-5bd5b33121158-c654ffc9-0ff0dc28",
        "startTime": "2021-03-12T10:38:00.048-08:00",
        "status": "DONE",
        "statusMessage": "Instance was preempted.",
        "targetId": "3533220210345905302",
        "targetLink": "https://www.googleapis.com/compute/v1/projects/my-project/zones/asia-northeast2-a/instances/pkb-1234-0",
        "user": "system",
        "zone": "https://www.googleapis.com/compute/v1/projects/my-project/zones/asia-northeast2-a"
    }]
    """
    fake_rets = [(stdout, 'stderr', 0)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      vm._UpdateInterruptibleVmStatusThroughApi()
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertRegex(command_string, 'gcloud compute operations list --filter'
                       fr' targetLink.scope\(\):{vm.name} --format json '
                       f'--project p --quiet --zones {vm.zone}')
      self.assertTrue(vm.spot_early_termination)

  @parameterized.named_parameters(
      (
          'is_deleted',
          '',
          """ERROR: (gcloud.compute.instances.describe) Could not fetch resource:
 - The resource 'projects/p3rf-cluster-boot/zones/us-west1-a/instances/pkb-c322006c78-0' was not found""",
          1,
          False,
      ),
      ('exists', _COMPUTE_DESCRIBE_RUNNING, '', 0, True),
      (
          'is_deleted_staging_http',
          '',
          r"""{
  "error": {
    "code": 404,
    "message": "The resource 'projects/sharp-airway-384/zones/us-central1-jq1/instances/pkb-8d45d024-0' was not found",
    "errors": [
      {
        "message": "The resource 'projects/sharp-airway-384/zones/us-central1-jq1/instances/pkb-8d45d024-0' was not found",
        "domain": "global",
        "reason": "notFound",
        "debugInfo": "java.lang.Exception\n\tat com.google.cloud.control.common.publicerrors.PublicErrorProtoUtils.newErrorBuilder(PublicErrorProtoUtils.java:1919)\n\tat com.google.cloud.control.common.publicerrors.PublicErrorProtoUtils.createResourceNotFoundError(PublicErrorProtoUtils.java:184)\n\tat com.google.cloud.control.frontend.PublicResourceRepository.getEntityKeyOrThrow(PublicResourceRepository.java:229)\n\tat com.google.cloud.control.frontend.PublicResourceRepository.loadResource(PublicResourceRepository.java:174)\n\tat com.google.cloud.control.frontend.action.SimpleCustomMixerGetActionHandler.getTargetEntity(SimpleCustomMixerGetActionHandler.java:94)\n\tat com.google.cloud.control.frontend.action.SimpleCustomMixerGetActionHandler.performRead(SimpleCustomMixerGetActionHandler.java:104)\n\tat com.google.cloud.control.frontend.action.CustomMixerReadActionExecutor$InnerHandler.runAttempt(CustomMixerReadActionExecutor.java:273)\n\tat com.google.cloud.control.frontend.action.CustomMixerReadActionExecutor$InnerHandler.runAttempt(CustomMixerReadActionExecutor.java:219)\n\tat com.google.cloud.cluster.metastore.RetryingMetastoreTransactionExecutor$1.runAttempt(RetryingMetastoreTransactionExecutor.java:79)\n\tat com.google.cloud.cluster.metastore.MetastoreRetryLoop.runHandler(MetastoreRetryLoop.java:523)\n\t...Stack trace is shortened.\n"
      }
    ]
  }
}""",
          1,
          False,
      ),
  )
  def test_exists(self, stdout, stderr, return_code, expected):
    spec = gce_virtual_machine.GceVmSpec(
        _COMPONENT,
        machine_type='test_machine_type',
        preemptible=True,
        project='p',
    )
    vm = pkb_common_test_case.TestGceVirtualMachine(spec)
    fake_rets = [(stdout, stderr, return_code)]
    with PatchCriticalObjects(fake_rets):
      self.assertEqual(vm._Exists(), expected)


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

  def testCreateUbuntu1804(self):
    vm_class = virtual_machine.GetVmClass(provider_info.GCP,
                                          os_types.UBUNTU1804)
    fake_image = 'fake-ubuntu1804'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(self.spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertEqual(vm.GetDefaultImageFamily(False), 'ubuntu-1804-lts')
      self.assertEqual(vm.GetDefaultImageFamily(True), 'ubuntu-1804-lts-arm64')
      self.assertEqual(vm.GetDefaultImageProject(), 'ubuntu-os-cloud')
      self.assertTrue(vm.SupportGVNIC())
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image-family ubuntu-1804-lts --image-project ubuntu-os-cloud',
          command_string)
      self.assertNotIn('--boot-disk-size', command_string)
      self.assertNotIn('--boot-disk-type', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'ubuntu-1804-lts',
                                     'image_project': 'ubuntu-os-cloud',
                                     'boot_disk_size': '10',
                                     'boot_disk_type': 'pd-standard'},
                                    vm.GetResourceMetadata())

  def testCreateUbuntuInCustomProject(self):
    """Test simulating passing --image and --image_project."""
    vm_class = virtual_machine.GetVmClass(provider_info.GCP,
                                          os_types.UBUNTU1804)
    fake_image = 'fake-ubuntu1804'
    fake_image_project = 'fake-project'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image,
                                         image_project=fake_image_project)
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn(
          '--image fake-ubuntu1804 --image-project fake-project',
          command_string)
      self.assertNotIn('--image-family', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_project': 'fake-project'},
                                    vm_metadata)
      self.assertNotIn('image_family', vm_metadata)

  def testCreateUbuntuInCustomDisk(self):
    """Test simulating passing --image and --image_project."""
    vm_class = virtual_machine.GetVmClass(provider_info.GCP,
                                          os_types.UBUNTU1804)
    fake_image = 'fake-ubuntu1804'
    fake_image_project = 'fake-project'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image,
                                         image_project=fake_image_project,
                                         boot_disk_size=20,
                                         boot_disk_type='fake-disk-type')
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--boot-disk-size 20', command_string)
      self.assertIn('--boot-disk-type fake-disk-type', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 2)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_project': 'fake-project',
                                     'boot_disk_size': 20,
                                     'boot_disk_type': 'fake-disk-type'},
                                    vm_metadata)
      self.assertNotIn('image_family', vm_metadata)

  def testCreateRhel7CustomImage(self):
    vm_class = virtual_machine.GetVmClass(provider_info.GCP, os_types.RHEL7)
    fake_image = 'fake-custom-rhel-image'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image)
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--image ' + fake_image, command_string)
      self.assertIn('--image-project rhel-cloud', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_project': 'rhel-cloud'},
                                    vm_metadata)
      self.assertNotIn('image_family', vm_metadata)

  def testCreateCentOs7CustomImage(self):
    vm_class = virtual_machine.GetVmClass(provider_info.GCP, os_types.CENTOS7)
    fake_image = 'fake-custom-centos7-image'
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type',
                                         image=fake_image)
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--image ' + fake_image, command_string)
      self.assertIn('--image-project centos-cloud', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_project': 'centos-cloud'},
                                    vm_metadata)
      self.assertNotIn('image_family', vm_metadata)

  def testCosVm(self):
    vm_class = virtual_machine.GetVmClass(provider_info.GCP, os_types.COS)
    spec = gce_virtual_machine.GceVmSpec(_COMPONENT,
                                         machine_type='fake-machine-type')
    fake_image = 'fake_cos_image'
    with PatchCriticalObjects(
        self._CreateFakeReturnValues(fake_image)) as issue_command:
      vm = vm_class(spec)
      vm._Create()
      vm.created = True
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud compute instances create', command_string)
      self.assertIn('--image-family cos-stable', command_string)
      self.assertIn('--image-project cos-cloud', command_string)
      vm._PostCreate()
      self.assertEqual(issue_command.call_count, 3)
      vm_metadata = vm.GetResourceMetadata()
      self.assertDictContainsSubset({'image': fake_image,
                                     'image_family': 'cos-stable',
                                     'image_project': 'cos-cloud'},
                                    vm_metadata)


class GCEVMFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GCEVMFlagsTestCase, self).setUp()
    FLAGS.cloud = provider_info.GCP
    FLAGS.gcloud_path = 'test_gcloud'
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
      vm = pkb_common_test_case.TestGceVirtualMachine(vm_spec)
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
        'Cannot set flag gce_migrate_on_maintenance on instances with GPUs '
        'or network placement groups, as it is not supported by GCP.'))

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
    self.assertIn('network-tier=PREMIUM', cmd)

  def testNetworkTierFlagStandard(self):
    """Tests that the standard network tier flag is supported."""
    cmd, call_count = self._CreateVmCommand(gce_network_tier='standard')
    self.assertEqual(call_count, 1)
    self.assertIn('network-tier=STANDARD', cmd)

  def testNetworkInterfaceDefault(self):
    """Tests that gVNIC is selected as the default virtual NIC."""
    cmd, call_count = self._CreateVmCommand()
    self.assertEqual(call_count, 1)
    self.assertIn('nic-type=GVNIC', cmd)

  def testNetworkInterfaceVIRTIO(self):
    """Tests that VirtIO can be set as the virtual NIC."""
    cmd, call_count = self._CreateVmCommand(gce_nic_type='VIRTIO_NET')
    self.assertEqual(call_count, 1)
    self.assertIn('nic-type=VIRTIO_NET', cmd)

  def testEgressBandwidthTier(self):
    """Tests that egress bandwidth can be set as tier 1."""
    cmd, call_count = self._CreateVmCommand(gce_egress_bandwidth_tier='TIER_1')
    self.assertEqual(call_count, 1)
    self.assertIn('total-egress-bandwidth-tier=TIER_1', cmd)

  def testAlphaMaintenanceFlag(self):
    """Tests that egress bandwidth can be set as tier 1."""
    gcloud_init = util.GcloudCommand.__init__
    def InitAndSetAlpha(self, resource, *args):
      gcloud_init(self, resource, *args)
      self.use_alpha_gcloud = True
    with mock.patch.object(util.GcloudCommand, '__init__', InitAndSetAlpha):
      cmd, call_count = self._CreateVmCommand(
          gce_egress_bandwidth_tier='TIER_1', gpu_count=1, gpu_type='k80')
    self.assertEqual(call_count, 1)
    self.assertIn('--on-host-maintenance', cmd)

  def testGcpInstanceMetadataFlag(self):
    cmd, call_count = self._CreateVmCommand(
        gcp_instance_metadata=['k1:v1', 'k2:v2,k3:v3'], owner='test-owner')
    self.assertEqual(call_count, 1)
    actual_metadata = re.compile(
        r'--metadata\s+(.*)(\s+--)?').search(cmd).group(1)  # pytype: disable=attribute-error  # re-none
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
        r'--metadata-from-file\s+(.*)(\s+--)?').search(cmd).group(1)  # pytype: disable=attribute-error  # re-none
    self.assertIn('k1=p1', actual_metadata_from_file)
    self.assertIn('k2=p2', actual_metadata_from_file)
    self.assertIn('k3=p3', actual_metadata_from_file)

  def testGceTags(self):
    self.assertIn('--tags perfkitbenchmarker', self._CreateVmCommand()[0])
    self.assertIn('--tags perfkitbenchmarker,testtag',
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
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
      with self.assertRaises(errors.Resource.CreationError):
        vm._Create()

  def testCreateVMSubnetworkNotReady(self):
    fake_rets = [('stdout', """\
(gcloud.compute.instances.create) Could not fetch resource:
- The resource 'projects/myproject/regions/us-central1/subnetworks/mysubnet' is not ready""",
                  1)]
    with PatchCriticalObjects(fake_rets):
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
      with self.assertRaises(errors.Resource.RetryableCreationError):
        vm._Create()

  @parameterized.named_parameters(
      {
          'testcase_name':
              'unsupported_machine_type',
          'fake_stderr': (
              'ERROR: (gcloud.compute.instances.create) Could not '
              "fetch resource:\n - Invalid value for field 'resource.machineType'"
              ": 'https://compute.googleapis.com/compute/v1/projects/"
              "control-plane-tests/zones/us-west3-c/machineTypes/n2-standard-2'"
              ". Machine type with name 'n2-standard-2' does not exist in zone "
              "'us-west3-c'."),
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      }, {
          'testcase_name':
              'unsupported_cpu_platform_type',
          'fake_stderr':
              ('ERROR: (gcloud.compute.instances.create) Could not fetch '
               'resource:\n - '
               "CPU platform type with name 'icelake' does not exist in zone "
               "'europe-west8-a'."),
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      }, {
          'testcase_name':
              'unsupported_resource',
          'fake_stderr':
              ('ERROR: (gcloud.compute.instances.create) Could not fetch '
               "resource:\n - The resource 'projects/bionic-baton-343/zones/"
               "us-west4-c/acceleratorTypes/nvidia-tesla-v100' was not found"),
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      }, {
          'testcase_name':
              'unsupported_features_not_compatible',
          'fake_stderr':
              ('ERROR: (gcloud.compute.instances.create) Could not fetch '
               'resource:\n - [n1-standard-2, Absence of any accelerator] '
               'features are not compatible for creating instance.'),
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      }, {
          'testcase_name':
              'internal_error',
          'fake_stderr':
              ('ERROR: (gcloud.compute.instances.create) Could not fetch '
               'resource:\n - Internal error. Please try again or contact '
               "Google Support. (Code: '5EB8741503E10.AC27D3.3305BC26')"),
          'expected_error': errors.Resource.CreationInternalError
      }, {
          'testcase_name':
              'service_unavailable',
          'fake_stderr':
              ('ERROR: (gcloud.compute.instances.create) Could not fetch'
               'resource:\n - The service is currently unavailable.'),
          'expected_error': errors.Benchmarks.KnownIntermittentError
      })
  def testCreateVMErrorCases(self, fake_stderr, expected_error):
    fake_rets = [('stdout', fake_stderr, 1)]
    with PatchCriticalObjects(fake_rets):
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
      with self.assertRaises(expected_error):
        vm._Create()

  @parameterized.named_parameters(
      {'testcase_name': 'old_message', 'fake_stderr': '''\
"The zone 'projects/fake-project/zones/fake-zone' does not have enough \
resources available to fulfill the request. Try a different zone, or try again \
later."
'''},
      {'testcase_name': 'new_message', 'fake_stderr': '''\
code: ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS
errorDetails:
- help:
    links:
    - description: Troubleshooting documentation
      url: https://cloud.google.com/compute/docs/resource-error
- localizedMessage:
    locale: en-US
    message: A a2-megagpu-16g VM instance with 16 NVIDIA_TESLA_A100 accelerator(s)
      is currently unavailable in the us-central1-b zone. Consider trying your request
      in the us-central1-f, us-central1-a, us-central1-c zone(s), which currently
      has capacity to accommodate your request. Alternatively, you can try your request
      again with a different VM hardware configuration or at a later time. For more
      information, see the troubleshooting documentation.
- errorInfo:
    domain: compute.googleapis.com
    metadatas:
      attachment: NVIDIA_TESLA_A100:16
      vmType: a2-megagpu-16g
      zone: us-central1-b
      zonesAvailable: us-central1-f,us-central1-a,us-central1-c
    reason: stockout
message: The zone 'projects/artemis-prod/zones/us-central1-b' does not have enough
  resources available to fulfill the request.  '(resource type:compute)'.'''})
  def testInsufficientCapacityCloudFailure(self, fake_stderr):
    fake_rets = [('stdout', fake_stderr, 1)]
    with PatchCriticalObjects(fake_rets):
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
      with self.assertRaises(
          errors.Benchmarks.InsufficientCapacityCloudFailure):
        vm._Create()

  def testVmWithoutGpu(self):
    with PatchCriticalObjects() as issue_command:
      spec = gce_virtual_machine.GceVmSpec(
          _COMPONENT, machine_type={
              'cpus': 1,
              'memory': '1.0GiB',
          })
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
      vm = pkb_common_test_case.TestGceVirtualMachine(spec)
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
    vm = mock.Mock(zone=zone, project=project, cidr=None)
    net = gce_network.GceNetwork.GetNetwork(vm)
    self.assertEqual(project, net.project)
    self.assertEqual(zone, net.zone)


_IP_LINK_TEXT = """
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state ...
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
2: ens4: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1460 qdisc mq ...
    link/ether 42:01:c0:a8:15:66 brd ff:ff:ff:ff:ff:ff
"""

# real example of ethtool -i ens4 output
_ETHTOOL_TEXT = """
driver: gve
version: 1.0.0
firmware-version:
expansion-rom-version:
bus-info: 0000:00:04.0
supports-statistics: yes
supports-test: no
supports-eeprom-access: no
supports-register-dump: no
supports-priv-flags: no
"""


class GvnicTest(GceVirtualMachineTestCase):
  """Tests specific to detecting gVNIC version."""

  def setUp(self):
    super(GvnicTest, self).setUp()
    vm_spec = gce_virtual_machine.GceVmSpec('test_component', project='test')
    self.vm = gce_virtual_machine.Ubuntu1804BasedGceVirtualMachine(vm_spec)
    self.vm.HasPackage = mock.Mock(return_value=False)
    self.mock_cmd = mock.Mock()
    self.vm.RemoteCommand = self.mock_cmd

  def testGetNetworkDeviceNames(self):
    self.mock_cmd.return_value = (_IP_LINK_TEXT, '')
    names = self.vm._get_network_device_mtus()
    self.assertEqual({'ens4': '1460'}, names)
    self.mock_cmd.assert_called_with('PATH="${PATH}":/usr/sbin ip link show up')

  def testGetNetworkDeviceProperties(self):
    self.mock_cmd.return_value = (_ETHTOOL_TEXT, '')
    props = self.vm._GetNetworkDeviceProperties('ens4')
    expected = {
        'bus-info': '0000:00:04.0',
        'driver': 'gve',
        'expansion-rom-version': '',
        'firmware-version': '',
        'supports-eeprom-access': 'no',
        'supports-priv-flags': 'no',
        'supports-register-dump': 'no',
        'supports-statistics': 'yes',
        'supports-test': 'no',
        'version': '1.0.0'
    }
    self.assertEqual(expected, props)
    self.mock_cmd.assert_called_with(
        'PATH="${PATH}":/usr/sbin ethtool -i ens4')

  @flagsaver.flagsaver(gce_nic_record_version=True)
  def testOnStartupSetGvnicVersion(self):
    self.mock_cmd.side_effect = [(_IP_LINK_TEXT, ''), (_ETHTOOL_TEXT, ''),
                                 (_IP_LINK_TEXT, '')]
    self.vm.OnStartup()
    self.assertEqual('1.0.0', self.vm._gvnic_version)
    self.assertEqual('1.0.0', self.vm.GetResourceMetadata()['gvnic_version'])
    self.assertEqual('1460',
                     list(self.vm._get_network_device_mtus().values())[0])

  @flagsaver.flagsaver(gce_nic_record_version=True)
  def testMissingVersionInProperties(self):
    self.mock_cmd.side_effect = [(_IP_LINK_TEXT, ''), ('driver: gve', '')]
    with self.assertRaises(ValueError):
      self.vm.OnStartup()


if __name__ == '__main__':
  unittest.main()
