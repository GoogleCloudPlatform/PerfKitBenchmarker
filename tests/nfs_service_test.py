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
"""Tests for NFS service."""

import unittest

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import nfs_service
from tests import mock_flags

_DEFAULT_NFS_TIER = 'foo'


class _DemoNfsService(nfs_service.BaseNfsService):
  CLOUD = 'mock'
  NFS_TIERS = (_DEFAULT_NFS_TIER,)

  def __init__(self, disk_spec, zone):
    super(_DemoNfsService, self).__init__(disk_spec, zone)
    self.is_ready_called = False

  def _IsReady(self):
    return True

  def GetRemoteAddress(self):
    return 'remote1'

  def _Create(self):
    pass

  def _Delete(self):
    pass


class _DemoNfsServiceWithDefaultNfsVersion(_DemoNfsService):
  CLOUD = 'mock2'
  DEFAULT_NFS_VERSION = '4.1'


class NfsServiceTest(unittest.TestCase):

  def _SetFlags(self, nfs_tier=None):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags['default_timeout'].parse(10)
    mocked_flags['nfs_tier'].parse(nfs_tier)

  def _NewNfsResource(self, nfs_tier=None):
    self._SetFlags(nfs_tier=nfs_tier)
    return _DemoNfsService(disk.BaseDiskSpec('test_component'), 'us-west1-a')

  def testNewNfsResource(self):
    nfs = self._NewNfsResource(_DEFAULT_NFS_TIER)
    self.assertEqual(_DEFAULT_NFS_TIER, nfs.nfs_tier)
    self.assertIsNone(nfs.DEFAULT_NFS_VERSION)

  def testNewNfsResourceBadNfsTier(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._NewNfsResource('NonExistentNfsTier')

  def testNewNfsResourceNfsTierNotSet(self):
    nfs = self._NewNfsResource()
    self.assertIsNone(nfs.nfs_tier)

  def testRegistry(self):
    nfs_class = nfs_service.GetNfsServiceClass(_DemoNfsService.CLOUD)
    self.assertEqual(_DemoNfsService, nfs_class)

  def testCreateNfsDisk(self):
    nfs = self._NewNfsResource()
    nfs_disk = nfs.CreateNfsDisk()
    self.assertEqual('remote1:/', nfs_disk.device_path)
    self.assertIsNone(nfs_disk.nfs_version)

  def testDefaultNfsVersion(self):
    self._SetFlags()
    nfs = _DemoNfsServiceWithDefaultNfsVersion(
        disk.BaseDiskSpec('test_component'), 'us-west1-a')
    nfs_disk = nfs.CreateNfsDisk()
    self.assertEqual('4.1', nfs_disk.nfs_version)


if __name__ == '__main__':
  unittest.main()
