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

"""Tests for perfkitbenchmarker.disk_iops_to_capacity."""

import unittest
from perfkitbenchmarker import disk_iops_to_capacity


class DiskIOPSToCapacityTest(unittest.TestCase):

  def testSetCPUCountAWS(self):
    AWSconfig = disk_iops_to_capacity.DiskIOPSToCapacity(300, 'AWS')
    self.assertEquals(AWSconfig.GetCPUCount(), 2)

  def testSetCPUCountGCP(self):
    GCPconfig = disk_iops_to_capacity.DiskIOPSToCapacity(300, 'GCP')
    self.assertEquals(GCPconfig.GetCPUCount(), 1)

  def testSetNumberDisksAWS(self):
    AWSconfig1 = disk_iops_to_capacity.DiskIOPSToCapacity(300, 'AWS')
    self.assertEqual(AWSconfig1.GetNumberDisks(), 1)
    AWSconfig2 = disk_iops_to_capacity.DiskIOPSToCapacity(25000, 'AWS')
    self.assertEqual(AWSconfig2.GetNumberDisks(), 3)
    AWSconfig3 = disk_iops_to_capacity.DiskIOPSToCapacity(20000, 'AWS')
    self.assertEqual(AWSconfig3.GetNumberDisks(), 2)

  def testSetNumberDisksGCP(self):
    GCPconfig1 = disk_iops_to_capacity.DiskIOPSToCapacity(50, 'GCP')
    self.assertEqual(GCPconfig1.GetNumberDisks(), 1)

  def testSetStorageSizeAWS(self):
    AWSconfig1 = disk_iops_to_capacity.DiskIOPSToCapacity(50, 'AWS')
    self.assertEqual(AWSconfig1.GetSize(), 2)
    AWSconfig2 = disk_iops_to_capacity.DiskIOPSToCapacity(100, 'AWS')
    self.assertEqual(AWSconfig2.GetSize(), 2)
    AWSconfig3 = disk_iops_to_capacity.DiskIOPSToCapacity(300, 'AWS')
    self.assertEqual(AWSconfig3.GetSize(), 300 * 3)
    AWSconfig4 = disk_iops_to_capacity.DiskIOPSToCapacity(9999, 'AWS')
    self.assertEqual(AWSconfig4.GetSize(), 9999 * 3)
    AWSconfig4 = disk_iops_to_capacity.DiskIOPSToCapacity(10000, 'AWS')
    self.assertEqual(AWSconfig4.GetSize(), 3580)

  def testSetStorageSizeGCP(self):
    GCPconfig1 = disk_iops_to_capacity.DiskIOPSToCapacity(1, 'GCP')
    self.assertEqual(GCPconfig1.GetSize(), 1)
    GCPconfig1 = disk_iops_to_capacity.DiskIOPSToCapacity(300, 'GCP')
    self.assertEqual(GCPconfig1.GetSize(), 10)
    GCPconfig2 = disk_iops_to_capacity.DiskIOPSToCapacity(30000, 'GCP')
    self.assertEqual(GCPconfig2.GetSize(), 1000)

  def testValidateProvider(self):
    self.assertRaises(disk_iops_to_capacity.InvalidProviderError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 300,
                      'NONPROVIDER')

  def testValidateIOPS(self):
    self.assertRaises(disk_iops_to_capacity.InvalidIOPSError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 0, 'AWS')
    self.assertRaises(disk_iops_to_capacity.InvalidIOPSError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 50000, 'GCP')
    self.assertRaises(disk_iops_to_capacity.InvalidIOPSError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 90000, 'AWS')

  def testValidateStorageType(self):
    self.assertRaises(disk_iops_to_capacity.InvalidStorageTypeError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 100, 'AWS',
                      'ebs-piops')
    self.assertRaises(disk_iops_to_capacity.InvalidStorageTypeError,
                      disk_iops_to_capacity.DiskIOPSToCapacity, 100, 'GCP',
                      'pd-hhd')


if __name__ == '__main__':
  unittest.main()
