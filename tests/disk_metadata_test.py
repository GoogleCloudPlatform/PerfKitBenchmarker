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

"""Test the annotation of disk objects with metadata."""

import unittest
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_disk
from tests import mock_flags


_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


class _DiskMetadataTestCase(unittest.TestCase):

  def setUp(self):
    self.addCleanup(context.SetThreadBenchmarkSpec, None)
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=mock_flags.MockFlags(), vm_groups={})
    self.benchmark_spec = benchmark_spec.BenchmarkSpec(
        config_spec, _BENCHMARK_NAME, _BENCHMARK_UID)


class GcpDiskMetadataTest(_DiskMetadataTestCase):
  def testPDStandard(self):
    disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_size=2,
                                  disk_type=gce_disk.PD_STANDARD)
    disk_obj = gce_disk.GceDisk(disk_spec, 'name', 'zone', 'project')
    self.assertEquals(disk_obj.metadata,
                      {disk.MEDIA: disk.HDD,
                       disk.REPLICATION: disk.ZONE,
                       disk.LEGACY_DISK_TYPE: disk.STANDARD})


class AwsDiskMetadataTest(_DiskMetadataTestCase):
  def doAwsDiskTest(self, disk_type, machine_type,
                    goal_media, goal_replication, goal_legacy_disk_type):
    disk_spec = aws_disk.AwsDiskSpec(_COMPONENT, disk_size=2,
                                     disk_type=disk_type)

    vm_spec = virtual_machine.BaseVmSpec(
        'test_vm_spec.AWS', zone='us-east-1a', machine_type=machine_type)
    vm = aws_virtual_machine.DebianBasedAwsVirtualMachine(
        vm_spec)

    vm.CreateScratchDisk(disk_spec)

    self.assertEqual(vm.scratch_disks[0].metadata,
                     {disk.MEDIA: goal_media,
                      disk.REPLICATION: goal_replication,
                      disk.LEGACY_DISK_TYPE: goal_legacy_disk_type})

  def testLocalSSD(self):
    self.doAwsDiskTest(
        disk.LOCAL,
        'c3.2xlarge',
        disk.SSD,
        disk.NONE,
        disk.LOCAL)

  def testLocalHDD(self):
    self.doAwsDiskTest(
        disk.LOCAL,
        'd2.2xlarge',
        disk.HDD,
        disk.NONE,
        disk.LOCAL)


class AzureDiskMetadataTest(_DiskMetadataTestCase):
  def doAzureDiskTest(self, storage_type, disk_type, machine_type,
                      goal_media, goal_replication, goal_legacy_disk_type):
    with mock.patch(azure_disk.__name__ + '.FLAGS') as disk_flags:
      disk_flags.azure_storage_type = storage_type
      disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_size=2,
                                    disk_type=disk_type)

      vm_spec = virtual_machine.BaseVmSpec(
          'test_vm_spec.AZURE', zone='East US 2', machine_type=machine_type)
      vm = azure_virtual_machine.DebianBasedAzureVirtualMachine(
          vm_spec)

      azure_disk.AzureDisk.Create = mock.Mock()
      azure_disk.AzureDisk.Attach = mock.Mock()
      vm.CreateScratchDisk(disk_spec)

      self.assertEqual(vm.scratch_disks[0].metadata,
                       {disk.MEDIA: goal_media,
                        disk.REPLICATION: goal_replication,
                        disk.LEGACY_DISK_TYPE: goal_legacy_disk_type})

  def testPremiumStorage(self):
    self.doAzureDiskTest(azure_flags.PLRS,
                         azure_disk.PREMIUM_STORAGE,
                         'Standard_D1',
                         disk.SSD,
                         disk.ZONE,
                         disk.REMOTE_SSD)

  def testStandardDisk(self):
    self.doAzureDiskTest(azure_flags.ZRS,
                         azure_disk.STANDARD_DISK,
                         'Standard_D1',
                         disk.HDD,
                         disk.REGION,
                         disk.STANDARD)

  def testLocalHDD(self):
    self.doAzureDiskTest(azure_flags.LRS,
                         disk.LOCAL,
                         'Standard_A1',
                         disk.HDD,
                         disk.NONE,
                         disk.LOCAL)

  def testLocalSSD(self):
    self.doAzureDiskTest(azure_flags.LRS,
                         disk.LOCAL,
                         'Standard_DS2',
                         disk.SSD,
                         disk.NONE,
                         disk.LOCAL)


if __name__ == '__main__':
  unittest.main()
