# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for NetApp service."""

import unittest

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import netapp_service
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class _DemoNetAppService(netapp_service.BaseNetAppService):
  CLOUD = 'mock_netapp'

  def _IsReady(self):
    return True

  def _GetRemoteAddress(self):
    return '10.0.0.1'

  def _Create(self):
    pass

  def _Delete(self):
    pass


class NetAppServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def _NewNetAppResource(self):
    FLAGS['default_timeout'].parse(10)
    return _DemoNetAppService(
        disk.BaseNetAppDiskSpec('test_component'), 'us-central1-a'
    )

  def testNewNetAppResource(self):
    netapp = self._NewNetAppResource()
    self.assertEqual('4.1', netapp.DEFAULT_NFS_VERSION)

  def testRegistry(self):
    netapp_class = netapp_service.GetNetAppServiceClass(
        _DemoNetAppService.CLOUD
    )
    self.assertEqual(_DemoNetAppService, netapp_class)

  def testCreateNetAppDisk(self):
    netapp = self._NewNetAppResource()
    netapp_disk = netapp.CreateNetAppDisk()
    self.assertEqual('10.0.0.1:/', netapp_disk.device_path)
    self.assertEqual('4.1', netapp_disk.nfs_version)


if __name__ == '__main__':
  unittest.main()
