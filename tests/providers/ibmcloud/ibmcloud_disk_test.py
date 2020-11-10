"""Tests for perfkitbenchmarker.tests.providers.ibmcloud.ibmcloud_disk."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from absl import flags
import mock

from perfkitbenchmarker.providers.ibmcloud import ibmcloud_disk
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class IbmcloudDiskGetDevicePathTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(IbmcloudDiskGetDevicePathTest, self).setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(ibmcloud_disk.IbmCloudDisk, '__init__', lambda self: None):
      self.disk = ibmcloud_disk.IbmCloudDisk()

  def run_cmd(self, cmd, should_log=True):
    print('Running fdisk.')
    response_mock = mock.Mock()
    response_mock.return_value = '\
      Disk /dev/vde: 8589.9 GB, 8589934592000 bytes, 16777216000 sectors\n\
      Units = sectors of 1 * 512 = 512 bytes\n\
      '
    print('Returning\n%s' % response_mock.return_value)
    return response_mock.return_value, None

  def testGetDeviceFromVDisk(self):
    FLAGS.data_disk_size = 8000
    vm = mock.Mock()
    vm.device_paths_detected = set()
    self.disk.attached_vm = mock.Mock()
    self.disk.attached_vm.RemoteCommand.side_effect = self.run_cmd
    self.disk._GetDeviceFromVDisk(vm)
    self.assertEqual('/dev/vde', self.disk.device_path)


if __name__ == '__main__':
  unittest.main()
