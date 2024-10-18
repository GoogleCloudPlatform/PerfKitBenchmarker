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
from absl import flags
from absl.testing import flagsaver
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_disk_strategies
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util as aws_util
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_windows_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case  # pylint:disable=unused-import

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


class ScratchDiskTestMixin:
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
    self.patches.append(mock.patch(vm_prefix + '.FormatDisk'))
    self.patches.append(mock.patch(vm_prefix + '.MountDisk'))
    self.patches.append(
        mock.patch(
            util.__name__ + '.GetDefaultProject', side_effect='test_project'
        )
    )

    # Patch subprocess.Popen to make sure we don't issue any commands to spin up
    # resources.
    self.patches.append(mock.patch('subprocess.Popen'))
    self.patches.append(
        mock.patch(vm_util.__name__ + '.GetTempDir', return_value='/tmp/dir')
    )

    self._PatchCloudSpecific()

    for p in self.patches:
      p.start()
      self.addCleanup(p.stop)

    # We need the disk class mocks to return new mocks each time they are
    # called. Otherwise all "disks" instantiated will be the same object.
    self._GetDiskClass().side_effect = lambda *args, **kwargs: mock.MagicMock(
        is_striped=False
    )

    # VM Creation depends on there being a BenchmarkSpec.
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=FLAGS, vm_groups={}
    )
    self.spec = benchmark_spec.BenchmarkSpec(
        mock.MagicMock(), config_spec, _BENCHMARK_UID
    )
    self.addCleanup(context.SetThreadBenchmarkSpec, None)
    self.addCleanup(flagsaver.restore_flag_values, self.saved_flag_values)

  @flagsaver.flagsaver(azure_version_log=False)
  def testScratchDisks(self):
    """Test for creating and deleting scratch disks.

    This test creates two scratch disks on a vm and deletes them, ensuring
    that the proper calls to create, format, mount, and delete are made.
    """

    vm = self._CreateVm()
    disk_spec = self.GetDiskSpec(mount_point='/mountpoint')
    vm.SetDiskSpec(disk_spec, 2)
    vm.create_disk_strategy.GetSetupDiskStrategy().WaitForDisksToVisibleFromVm = mock.MagicMock(
        return_value=12
    )
    vm.RemoteHostCommand = mock.MagicMock(return_value=('0', ''))
    vm.SetupAllScratchDisks()
    assert len(vm.scratch_disks) == 2, 'Disk not added to scratch disks.'

    scratch_disk1 = vm.scratch_disks[0]
    scratch_disk2 = vm.scratch_disks[1]
    scratch_disk1.Create.assert_called_once_with()
    scratch_disk2.Create.assert_called_once_with()
    format_disk_callls = [
        mock.call(scratch_disk1.GetDevicePath(), disk_spec.disk_type),
        mock.call(scratch_disk2.GetDevicePath(), disk_spec.disk_type),
    ]

    vm.FormatDisk.assert_has_calls(format_disk_callls, None)

    mount_disk_callls = [
        mock.call(
            scratch_disk1.GetDevicePath(),
            '/mountpoint0',
            disk_spec.disk_type,
            scratch_disk1.mount_options,
            scratch_disk1.fstab_options,
        ),
        mock.call(
            scratch_disk2.GetDevicePath(),
            '/mountpoint1',
            disk_spec.disk_type,
            scratch_disk2.mount_options,
            scratch_disk2.fstab_options,
        ),
    ]
    vm.MountDisk.assert_has_calls(mount_disk_callls, None)

    # Check that these execute without exceptions. The return value
    # is a MagicMock, not a string, so we can't compare to expected results.
    vm.GetScratchDir()
    vm.GetScratchDir(0)
    vm.GetScratchDir(1)
    with self.assertRaises(errors.Error):
      vm.GetScratchDir(2)

    vm.DeleteScratchDisks()

    vm.scratch_disks[0].Delete.assert_called_once_with()
    vm.scratch_disks[1].Delete.assert_called_once_with()

  def GetDiskSpec(self, mount_point):
    return disk.BaseDiskSpec(_COMPONENT, mount_point=mount_point)


class AzureScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(azure_disk.__name__ + '.AzureDisk'))

  def _CreateVm(self):
    vm_spec = azure_virtual_machine.AzureVmSpec(
        'test_vm_spec.Azure', zone='eastus', machine_type='test_machine_type_v5'
    )
    return azure_virtual_machine.Ubuntu2004BasedAzureVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return azure_disk.AzureDisk

  def GetDiskSpec(self, mount_point):
    test_disk = disk.BaseDiskSpec(_COMPONENT, mount_point=mount_point)
    test_disk.disk_type = azure_disk.STANDARD_DISK
    test_disk.disk_size = 10
    return test_disk


class GceScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(gce_disk.__name__ + '.GceDisk'))

  def _CreateVm(self):
    vm_spec = gce_virtual_machine.GceVmSpec(
        'test_vm_spec.GCP',
        machine_type='test_machine_type_v5',
        zone='us-central1-a',
    )
    vm = gce_virtual_machine.Ubuntu2004BasedGceVirtualMachine(vm_spec)
    vm.GetNVMEDeviceInfo = mock.Mock()
    # This is modeled after N2 with Local SSDs.
    # TODO(user): Add tests for Confidential/M3's and C3-lssd.
    vm.GetNVMEDeviceInfo.return_value = [
        {
            'ModelNumber': 'nvme_card',
            'DevicePath': '/dev/nvme0n1',
        },
        {
            'ModelNumber': 'nvme_card',
            'DevicePath': '/dev/nvme0n2',
        },
    ]
    return vm

  def _GetDiskClass(self):
    return gce_disk.GceDisk

  def GetDiskSpec(self, mount_point):
    return gce_disk.GceDiskSpec(
        _COMPONENT,
        mount_point=mount_point,
        create_with_vm=False,
        disk_type=gce_disk.PD_STANDARD,
    )


class GceMultiWriterDiskTest(GceScratchDiskTest, unittest.TestCase):
  def _CreateVm(self):
    vm_spec = gce_virtual_machine.GceVmSpec(
        'test_vm_spec.GCP',
        machine_type='test_machine_type',
        zone='us-central1-a',
        image_family=os_types.WINDOWS2022_SQLSERVER_2022_STANDARD
    )
    vm = gce_windows_virtual_machine.WindowsGceVirtualMachine(vm_spec)
    vm.GetNVMEDeviceInfo = mock.Mock()
    # This is modeled after N2 with Local SSDs.
    # TODO(user): Add tests for Confidential/M3's and C3-lssd.
    vm.GetNVMEDeviceInfo.return_value = [
        {
            'ModelNumber': 'nvme_card',
            'DevicePath': '/dev/nvme0n1',
        },
        {
            'ModelNumber': 'nvme_card',
            'DevicePath': '/dev/nvme0n2',
        },
    ]
    return vm

  def _PatchCloudSpecific(self):
    # Mock GceDisk Create and Attach methods
    self.patches.append(mock.patch(gce_disk.__name__ + '.GceDisk.Create'))
    self.patches.append(mock.patch(gce_disk.__name__ + '.GceDisk.Attach'))
    self.patches.append(
        mock.patch(
            disk_strategies.__name__
            + '.PrepareScratchDiskStrategy.PrepareScratchDisk'
        )
    )

  def testScratchDisks(self):
    # Unit test method from ScratchDiskTestMixin (not applicable)
    pass

  def _GetScratchDiskName(
      self, vm_group_name: str, multi_writer_group_name: str
  ) -> str:
    """Get the name of the scratch disk.

    Args:
      vm_group_name: name of the vm_group
      multi_writer_group_name: name of the multiwriter group name (optional)

    Returns:
      scratch disk name

    """
    # In this test case, GceDisk will not be mocked. Instead, only the methods
    # Create and Attach for GceDisk need to be mocked.
    self._GetDiskClass().side_effect = None
    vm = self._CreateVm()
    # initialize vm (vm_group and zone)
    vm.vm_group = vm_group_name
    vm.zone = 'test-zone-1'

    # Create disk_spec
    disk_spec = self.GetDiskSpec(mount_point='/mountpoint')
    disk_spec.multi_writer_mode = True
    if multi_writer_group_name:
      disk_spec.multi_writer_group_name = multi_writer_group_name

    # Create scratch disks
    vm.SetDiskSpec(disk_spec, 1)
    vm.create_disk_strategy.GetSetupDiskStrategy().WaitForDisksToVisibleFromVm = mock.MagicMock(
        return_value=12
    )
    vm.SetupAllScratchDisks()

    scratch_disk_name = vm.scratch_disks[0].name if vm.scratch_disks else None
    return scratch_disk_name

  def testMultiWriterDiskName(self):
    """Test Case: Multiwriter Disk Name.

    Scenario:
      Verify that the multiwriter disk name adheres to the following convention:
      'pkb-xxxx-multiwriter-{groupName}-{xx}'.
      If 'disk_spec.multi_writer_group_name' is provided, ensure 'groupName' in
      the disk name is set to this specified value.
      If 'disk_spec.multi_writer_group_name' is not provided (or is None/empty),
      the 'groupName' in the disk name is set to 'vm.vm_group'.
    """
    # Test multiwriter disk name without setting multi_writer_group_name
    disk_name = self._GetScratchDiskName(
        vm_group_name='testSrvGroup',
        multi_writer_group_name=None,
    )
    validate_result = (
        True if disk_name and 'multiwriter-testSrvGroup' in disk_name else False
    )
    self.assertEqual(validate_result, True)

    # Test multiwriter disk name with setting multi_writer_group_name
    disk_name = self._GetScratchDiskName(
        vm_group_name='testSrvGroup',
        multi_writer_group_name='sqlSrvGroup',
    )
    validate_result = (
        True if disk_name and 'multiwriter-sqlSrvGroup' in disk_name else False
    )
    self.assertEqual(validate_result, True)


class AwsScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(aws_disk.__name__ + '.AwsDisk'))
    self.patches.append(mock.patch(aws_util.__name__ + '.AddDefaultTags'))
    # In Python3 the mocking of subprocess.Popen in setup() is problematic for
    # platform.system(). It is called by RemoteCommand() in
    # _GetNvmeBootIndex() so we'll mock that instead.
    self.patches.append(
        mock.patch(
            aws_disk_strategies.__name__
            + '.SetUpLocalDiskStrategy._GetNvmeBootIndex'
        )
    )
    self.patches.append(
        mock.patch(
            aws_disk_strategies.__name__
            + '.AWSSetupDiskStrategy.GetVolumeIdByDevice'
        )
    )
    self.patches.append(
        mock.patch(
            aws_disk_strategies.__name__
            + '.AWSSetupDiskStrategy.GetPathByDevice'
        )
    )

  def _CreateVm(self):
    vm_spec = aws_virtual_machine.AwsVmSpec(
        'test_vm_spec.AWS', zone='us-east-1a', machine_type='test_machine_type'
    )
    vm = aws_virtual_machine.Ubuntu2004BasedAwsVirtualMachine(vm_spec)

    vm.LogDeviceByDiskSpecId('0_0', 'foobar_1')
    vm.LogDeviceByName('foobar_1', 'vol67890', None)
    vm.GetNVMEDeviceInfo = mock.Mock()
    vm.GetNVMEDeviceInfo.return_value = [{
        'DevicePath': '/dev/nvme1n2',
        'SerialNumber': 'vol67890',
        'ModelNumber': 'Amazon Elastic Block Store',
    }]
    return vm

  def _GetDiskClass(self):
    return aws_disk.AwsDisk

  def GetDiskSpec(self, mount_point):
    return aws_disk.AwsDiskSpec(
        _COMPONENT,
        mount_point=mount_point,
        create_with_vm=False,
        disk_type=aws_disk.STANDARD,
    )


class GceDeviceIdTest(unittest.TestCase):

  def testDeviceId(self):
    with mock.patch(disk.__name__ + '.FLAGS') as disk_flags:
      disk_flags.os_type = 'windows'
      disk_spec = gce_disk.GceDiskSpec(
          _COMPONENT, disk_number=1, disk_size=2, disk_type=gce_disk.PD_STANDARD
      )
      disk_obj = gce_disk.GceDisk(disk_spec, 'name', 'zone', 'project')
      self.assertEqual(disk_obj.GetDeviceId(), r'\\.\PHYSICALDRIVE1')


if __name__ == '__main__':
  unittest.main()
