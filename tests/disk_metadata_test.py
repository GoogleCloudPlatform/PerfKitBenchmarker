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
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.azure import flags as azure_flags
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
        mock.MagicMock(), config_spec, _BENCHMARK_UID)


class GcpDiskMetadataTest(_DiskMetadataTestCase):

  def testPDStandard(self):
    disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_size=2,
                                  disk_type=gce_disk.PD_STANDARD)
    disk_obj = gce_disk.GceDisk(disk_spec, 'name', 'zone', 'project')
    self.assertDictContainsSubset(
        {disk.MEDIA: disk.HDD, disk.REPLICATION: disk.ZONE},
        disk_obj.metadata
    )


class AwsDiskMetadataTest(_DiskMetadataTestCase):

  def DoAwsDiskTest(self, disk_type, machine_type,
                    goal_media, goal_replication):
    disk_spec = aws_disk.AwsDiskSpec(_COMPONENT, disk_size=2,
                                     disk_type=disk_type)

    vm_spec = aws_virtual_machine.AwsVmSpec(
        'test_vm_spec.AWS', zone='us-east-1a', machine_type=machine_type)
    vm = aws_virtual_machine.DebianBasedAwsVirtualMachine(
        vm_spec)

    vm.CreateScratchDisk(disk_spec)

    self.assertDictContainsSubset(
        {disk.MEDIA: goal_media, disk.REPLICATION: goal_replication},
        vm.scratch_disks[0].metadata
    )

  def testLocalSSD(self):
    self.DoAwsDiskTest(
        disk.LOCAL,
        'c3.2xlarge',
        disk.SSD,
        disk.NONE)

  def testLocalHDD(self):
    self.DoAwsDiskTest(
        disk.LOCAL,
        'd2.2xlarge',
        disk.HDD,
        disk.NONE)


class AzureDiskMetadataTest(_DiskMetadataTestCase):

  def DoAzureDiskTest(self, storage_type, disk_type, machine_type,
                      goal_media, goal_replication,
                      goal_host_caching, disk_size=2,
                      goal_size=2, goal_stripes=1):
    with mock.patch(azure_disk.__name__ + '.FLAGS') as disk_flags:
      disk_flags.azure_storage_type = storage_type
      disk_flags.azure_host_caching = goal_host_caching
      disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_size=disk_size,
                                    disk_type=disk_type,
                                    num_striped_disks=goal_stripes)

      vm_spec = virtual_machine.BaseVmSpec(
          'test_vm_spec.AZURE', zone='East US 2', machine_type=machine_type)
      vm = azure_virtual_machine.DebianBasedAzureVirtualMachine(
          vm_spec)

      azure_disk.AzureDisk.Create = mock.Mock()
      azure_disk.AzureDisk.Attach = mock.Mock()
      vm.StripeDisks = mock.Mock()
      vm.CreateScratchDisk(disk_spec)

      expected = {disk.MEDIA: goal_media,
                  disk.REPLICATION: goal_replication,
                  'num_stripes': goal_stripes,
                  'size': goal_size}
      if goal_host_caching:
        expected[azure_disk.HOST_CACHING] = goal_host_caching
      self.assertDictContainsSubset(expected, vm.scratch_disks[0].metadata)

  def testPremiumStorage(self):
    self.DoAzureDiskTest(azure_flags.PLRS,
                         azure_disk.PREMIUM_STORAGE,
                         'Standard_D1',
                         disk.SSD,
                         disk.ZONE,
                         azure_flags.READ_ONLY)

  def testStandardDisk(self):
    self.DoAzureDiskTest(azure_flags.ZRS,
                         azure_disk.STANDARD_DISK,
                         'Standard_D1',
                         disk.HDD,
                         disk.REGION,
                         azure_flags.NONE)

  def testLocalHDD(self):
    self.DoAzureDiskTest(azure_flags.LRS,
                         disk.LOCAL,
                         'Standard_A1',
                         disk.HDD,
                         disk.NONE,
                         None)

  def testLocalSSD(self):
    self.DoAzureDiskTest(azure_flags.LRS,
                         disk.LOCAL,
                         'Standard_DS2',
                         disk.SSD,
                         disk.NONE,
                         None)

  def testStripedDisk(self):
    self.DoAzureDiskTest(azure_flags.LRS,
                         azure_disk.STANDARD_DISK,
                         'Standard_D1',
                         disk.HDD,
                         disk.ZONE,
                         azure_flags.READ_ONLY,
                         disk_size=5,
                         goal_size=10,
                         goal_stripes=2)


if __name__ == '__main__':
  unittest.main()
