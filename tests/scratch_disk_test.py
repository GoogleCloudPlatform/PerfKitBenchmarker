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

"""Tests for PerfKitBenchmarker' scratchdisks."""

import abc
import unittest
from absl.testing import flagsaver
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util as aws_util
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case  # pylint:disable=unused-import

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


class ScratchDiskTestMixin(object):
  """Sets up and tears down some of the mocks needed to test scratch disks."""

  @abc.abstractmethod
  def _PatchCloudSpecific(self):
    """Adds any cloud specific patches to self.patches."""
    pass

  @abc.abstractmethod
  def _CreateVm(self):
    """Creates and returns a VM object of the correct type for the cloud."""
    pass

  @abc.abstractmethod
  def _GetDiskClass(self):
    """Returns the disk class for the given cloud."""
    pass

  def setUp(self):
    self.saved_flag_values = flagsaver.save_flag_values()
    self.patches = []

    vm_prefix = linux_virtual_machine.__name__ + '.BaseLinuxMixin'
    self.patches.append(
        mock.patch(vm_prefix + '.FormatDisk'))
    self.patches.append(
        mock.patch(vm_prefix + '.MountDisk'))
    self.patches.append(
        mock.patch(util.__name__ + '.GetDefaultProject'))

    # Patch subprocess.Popen to make sure we don't issue any commands to spin up
    # resources.
    self.patches.append(mock.patch('subprocess.Popen'))
    self.patches.append(
        mock.patch(vm_util.__name__ + '.GetTempDir', return_value='/tmp/dir'))

    self._PatchCloudSpecific()

    for p in self.patches:
      p.start()
      self.addCleanup(p.stop)

    # We need the disk class mocks to return new mocks each time they are
    # called. Otherwise all "disks" instantiated will be the same object.
    self._GetDiskClass().side_effect = (
        lambda *args, **kwargs: mock.MagicMock(is_striped=False))

    # VM Creation depends on there being a BenchmarkSpec.
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=FLAGS, vm_groups={})
    self.spec = benchmark_spec.BenchmarkSpec(mock.MagicMock(), config_spec,
                                             _BENCHMARK_UID)
    self.addCleanup(context.SetThreadBenchmarkSpec, None)
    self.addCleanup(flagsaver.restore_flag_values, self.saved_flag_values)

  def testScratchDisks(self):
    """Test for creating and deleting scratch disks.

    This test creates two scratch disks on a vm and deletes them, ensuring
    that the proper calls to create, format, mount, and delete are made.
    """

    vm = self._CreateVm()

    disk_spec = disk.BaseDiskSpec(_COMPONENT, mount_point='/mountpoint0')
    vm.CreateScratchDisk(disk_spec)

    assert len(vm.scratch_disks) == 1, 'Disk not added to scratch disks.'

    scratch_disk = vm.scratch_disks[0]

    scratch_disk.Create.assert_called_once_with()
    vm.FormatDisk.assert_called_once_with(scratch_disk.GetDevicePath(), None)
    vm.MountDisk.assert_called_once_with(
        scratch_disk.GetDevicePath(), '/mountpoint0',
        None, scratch_disk.mount_options, scratch_disk.fstab_options)

    disk_spec = disk.BaseDiskSpec(_COMPONENT, mount_point='/mountpoint1')
    vm.CreateScratchDisk(disk_spec)

    assert len(vm.scratch_disks) == 2, 'Disk not added to scratch disks.'

    # Check that these execute without exceptions. The return value
    # is a MagicMock, not a string, so we can't compare to expected results.
    vm.GetScratchDir()
    vm.GetScratchDir(0)
    vm.GetScratchDir(1)
    with self.assertRaises(errors.Error):
      vm.GetScratchDir(2)

    scratch_disk = vm.scratch_disks[1]

    scratch_disk.Create.assert_called_once_with()
    vm.FormatDisk.assert_called_with(scratch_disk.GetDevicePath(), None)
    vm.MountDisk.assert_called_with(
        scratch_disk.GetDevicePath(), '/mountpoint1',
        None, scratch_disk.mount_options, scratch_disk.fstab_options)

    vm.DeleteScratchDisks()

    vm.scratch_disks[0].Delete.assert_called_once_with()
    vm.scratch_disks[1].Delete.assert_called_once_with()


class AzureScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(azure_disk.__name__ + '.AzureDisk'))

  def _CreateVm(self):
    vm_spec = azure_virtual_machine.AzureVmSpec(
        'test_vm_spec.Azure', zone='eastus2', machine_type='test_machine_type')
    return azure_virtual_machine.DebianBasedAzureVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return azure_disk.AzureDisk


class GceScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(gce_disk.__name__ + '.GceDisk'))

  def _CreateVm(self):
    vm_spec = gce_virtual_machine.GceVmSpec('test_vm_spec.GCP',
                                            machine_type='test_machine_type')
    return gce_virtual_machine.DebianBasedGceVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return gce_disk.GceDisk


class AwsScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(aws_disk.__name__ + '.AwsDisk'))
    self.patches.append(mock.patch(aws_util.__name__ + '.AddDefaultTags'))
    # In Python3 the mocking of subprocess.Popen in setup() is problematic for
    # platform.system(). It is called by RemoteCommand() in
    # _GetNvmeBootIndex() so we'll mock that instead.
    self.patches.append(mock.patch(
        aws_virtual_machine.__name__ + '.AwsVirtualMachine._GetNvmeBootIndex'))

  def _CreateVm(self):
    vm_spec = aws_virtual_machine.AwsVmSpec('test_vm_spec.AWS',
                                            zone='us-east-1a',
                                            machine_type='test_machine_type')
    return aws_virtual_machine.DebianBasedAwsVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return aws_disk.AwsDisk


class GceDeviceIdTest(unittest.TestCase):

  def testDeviceId(self):
    with mock.patch(disk.__name__ + '.FLAGS') as disk_flags:
      disk_flags.os_type = 'windows'
      disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_number=1, disk_size=2,
                                    disk_type=gce_disk.PD_STANDARD)
      disk_obj = gce_disk.GceDisk(disk_spec, 'name', 'zone', 'project')
      self.assertEqual(disk_obj.GetDeviceId(), r'\\.\PHYSICALDRIVE1')


if __name__ == '__main__':
  unittest.main()
