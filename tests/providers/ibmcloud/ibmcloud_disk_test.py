# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.tests.providers.ibmcloud.ibmcloud_disk."""


import unittest
from absl import flags
import mock

from perfkitbenchmarker.providers.ibmcloud import ibmcloud_disk
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

DEVICE_PATH = '/dev/vde'


class IbmcloudDiskGetDevicePathTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmcloudDiskGetDevicePathTest, self).setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(ibmcloud_disk.IbmCloudDisk, '__init__',
                           lambda self: None):
      self.disk = ibmcloud_disk.IbmCloudDisk()

  def run_cmd(self, cmd, should_log=True):
    return (
        'Disk /dev/vde: 8589.9 GB, 8589934592000 bytes, 16777216000 '
        'sectors\nUnits = sectors of 1 * 512 = 512 bytes\n',
        None)

  def testGetDeviceFromVDisk(self):
    vm = mock.Mock()
    vm.device_paths_detected = set()
    self.disk.attached_vm = mock.Mock()
    self.disk.attached_vm.RemoteCommand.side_effect = self.run_cmd
    self.disk.disk_size = 8000
    self.disk._GetDeviceFromVDisk(vm)
    self.assertEqual(DEVICE_PATH, self.disk.device_path)


if __name__ == '__main__':
  unittest.main()
