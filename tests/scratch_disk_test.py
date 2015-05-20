# Copyright 2014 Google Inc. All rights reserved.
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

import mock

from perfkitbenchmarker import pkb  # NOQA
from perfkitbenchmarker import disk
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.aws import aws_disk
from perfkitbenchmarker.aws import aws_virtual_machine
from perfkitbenchmarker.aws import util as aws_util
from perfkitbenchmarker.azure import azure_disk
from perfkitbenchmarker.azure import azure_network
from perfkitbenchmarker.azure import azure_virtual_machine
from perfkitbenchmarker.gcp import gce_disk
from perfkitbenchmarker.gcp import gce_virtual_machine


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
    self.patches = []

    vm_prefix = virtual_machine.__name__ + '.BaseVirtualMachine'
    self.patches.append(
        mock.patch(vm_prefix + '.FormatDisk'))
    self.patches.append(
        mock.patch(vm_prefix + '.MountDisk'))

    # Patch subprocess.Popen to make sure we don't issue any commands to spin up
    # resources.
    self.patches.append(mock.patch('subprocess.Popen'))

    self._PatchCloudSpecific()

    for p in self.patches:
      p.start()
      self.addCleanup(p.stop)

    # We need the disk class mocks to return new mocks each time they are
    # called. Otherwise all "disks" instantiated will be the same object.
    self._GetDiskClass().side_effect = (
        lambda *args, **kwargs: mock.MagicMock(is_striped=False))

  def testScratchDisks(self):
    """Test for creating and deleting scratch disks.

    This test creates two scratch disks on a vm and deletes them, ensuring
    that the proper calls to create, format, mount, and delete are made.
    """

    vm = self._CreateVm()

    disk_spec = disk.BaseDiskSpec(None, None, '/mountpoint0')
    vm.CreateScratchDisk(disk_spec)

    assert len(vm.scratch_disks) == 1, 'Disk not added to scratch disks.'

    scratch_disk = vm.scratch_disks[0]

    scratch_disk.Create.assert_called_once_with()
    vm.FormatDisk.assert_called_once_with(scratch_disk.GetDevicePath())
    vm.MountDisk.assert_called_once_with(
        scratch_disk.GetDevicePath(), '/mountpoint0')

    disk_spec = disk.BaseDiskSpec(None, None, '/mountpoint1')
    vm.CreateScratchDisk(disk_spec)

    assert len(vm.scratch_disks) == 2, 'Disk not added to scratch disks.'

    scratch_disk = vm.scratch_disks[1]

    scratch_disk.Create.assert_called_once_with()
    vm.FormatDisk.assert_called_with(scratch_disk.GetDevicePath())
    vm.MountDisk.assert_called_with(
        scratch_disk.GetDevicePath(), '/mountpoint1')

    vm.DeleteScratchDisks()

    vm.scratch_disks[0].Delete.assert_called_once_with()
    vm.scratch_disks[1].Delete.assert_called_once_with()


class AzureScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(azure_disk.__name__ + '.AzureDisk'))

  def _CreateVm(self):
    network = azure_network.AzureNetwork(None)
    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        None, None, None, None, network)
    return azure_virtual_machine.AzureVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return azure_disk.AzureDisk


class GceScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(gce_disk.__name__ + '.GceDisk'))

  def _CreateVm(self):
    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        None, None, None, None, None)
    return gce_virtual_machine.GceVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return gce_disk.GceDisk


class AwsScratchDiskTest(ScratchDiskTestMixin, unittest.TestCase):

  def _PatchCloudSpecific(self):
    self.patches.append(mock.patch(aws_disk.__name__ + '.AwsDisk'))
    self.patches.append(mock.patch(aws_util.__name__ + '.AddDefaultTags'))

  def _CreateVm(self):
    vm_spec = virtual_machine.BaseVirtualMachineSpec(
        None, 'zone', None, 'image', None)
    return aws_virtual_machine.AwsVirtualMachine(vm_spec)

  def _GetDiskClass(self):
    return aws_disk.AwsDisk

if __name__ == '__main__':
  unittest.main()
