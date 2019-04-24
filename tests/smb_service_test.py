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
"""Tests for SMB service."""

import unittest

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import smb_service
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class _FakeSmbService(smb_service.BaseSmbService):
  CLOUD = 'mock'

  def __init__(self, disk_spec, zone):
    super(_FakeSmbService, self).__init__(disk_spec, zone)
    self.is_ready_called = False

  def _IsReady(self):
    return True

  def GetRemoteAddress(self):
    return '//remote1'

  def GetStorageAccountAndKey(self):
    return {'user': 'hello', 'pw': 'world'}

  def _Create(self):
    pass

  def _Delete(self):
    pass


class _FakeSmbServiceWithDefaultSmbVersion(_FakeSmbService):
  CLOUD = 'mock2'
  DEFAULT_SMB_VERSION = '3.0'


class SmbServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def _SetFlags(self):
    FLAGS['default_timeout'].parse(10)

  def _NewSmbResource(self):
    return _FakeSmbService(disk.BaseDiskSpec('test_component'), 'us-west1-a')

  def testNewSmbResource(self):
    smb = self._NewSmbResource()
    self.assertIsNone(smb.DEFAULT_SMB_VERSION)

  def testRegistry(self):
    smb_class = smb_service.GetSmbServiceClass(_FakeSmbService.CLOUD)
    self.assertEqual(_FakeSmbService, smb_class)

  def testCreateSmbDisk(self):
    smb = self._NewSmbResource()
    smb_disk = smb.CreateSmbDisk()
    self.assertEqual('//remote1', smb_disk.device_path)
    self.assertEqual({'user': 'hello', 'pw': 'world'},
                     smb_disk.storage_account_and_key)
    self.assertEqual('3.0', smb_disk.smb_version)

  def testDefaultSmbVersion(self):
    self._SetFlags()
    smb = _FakeSmbServiceWithDefaultSmbVersion(
        disk.BaseDiskSpec('test_component'), 'us-west1-a')
    smb_disk = smb.CreateSmbDisk()
    self.assertEqual('3.0', smb_disk.smb_version)


if __name__ == '__main__':
  unittest.main()
