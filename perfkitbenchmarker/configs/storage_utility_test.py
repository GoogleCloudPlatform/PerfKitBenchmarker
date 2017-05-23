#!/usr/bin/env python

# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


import unittest
import storage_utility


class StorageUtilityTest(unittest.TestCase):

  def testSetCPUCountAWS(self):
    AWSconfig = storage_utility.StorageConfig(300, 'AWS')
    self.assertEquals(AWSconfig.GetCPUCount(), 1)

  def testSetCPUCountGCP(self):
    GCPconfig = storage_utility.StorageConfig(300, 'GCP')
    self.assertEquals(GCPconfig.GetCPUCount(), 1)

  def testSetNumberDisksAWS(self):
    AWSconfig1 = storage_utility.StorageConfig(300, 'AWS')
    self.assertEqual(AWSconfig1.GetNumberDisks(), 1)

  def testSetNumberDisksGCP(self):
    GCPconfig1 = storage_utility.StorageConfig(50, 'GCP')
    self.assertEqual(GCPconfig1.GetNumberDisks(), 1)
    GCPconfig2 = storage_utility.StorageConfig(30000, 'GCP')
    self.assertEqual(GCPconfig2.GetNumberDisks(), 1000)

  def testSetStorageSizeAWS(self):

    AWSconfig1 = storage_utility.StorageConfig(50, 'AWS')
    self.assertEqual(AWSconfig1.GetStorageSize(), 2)
    AWSconfig2 = storage_utility.StorageConfig(100, 'AWS')
    self.assertEqual(AWSconfig2.GetStorageSize(), 2)
    AWSconfig3 = storage_utility.StorageConfig(300, 'AWS')
    self.assertEqual(AWSconfig3.GetStorageSize(), 300 * 3)
    AWSconfig4 = storage_utility.StorageConfig(9999, 'AWS')
    self.assertEqual(AWSconfig4.GetStorageSize(), 9999 * 3)
    AWSconfig4 = storage_utility.StorageConfig(10000, 'AWS')
    self.assertEqual(AWSconfig4.GetStorageSize(), 3580)

  def testSetStorageSizeGCP(self):
    GCPconfig1 = storage_utility.StorageConfig(300, 'GCP')
    self.assertEqual(GCPconfig1.GetStorageSize(), 10)
    GCPconfig2 = storage_utility.StorageConfig(30000, 'GCP')
    self.assertEqual(GCPconfig2.GetStorageSize(), 1000)

  def testValidateProvider(self):
    self.assertRaises(storage_utility.SQLProviderError,
                      storage_utility.StorageConfig, 300, 'NONPROVIDER')

  def testValidateIOPS(self):
    self.assertRaises(storage_utility.IOPSError,
                      storage_utility.StorageConfig, 0, 'AWS')


if __name__ == '__main__':
  unittest.main()
