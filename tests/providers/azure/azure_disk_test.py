"""Tests for perfkitbenchmarker.tests.providers.azure_disk."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker.providers.azure import azure_disk
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AzureDiskGetDevicePathTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(azure_disk.AzureDisk, '__init__', lambda self: None):
      self.disk = azure_disk.AzureDisk()
      self.disk.disk_type = 'NOT_LOCAL'
      self.disk.machine_type = 'fake_v5'
      self.disk.vm = mock.Mock()
      self.disk.vm.SupportsNVMe = mock.Mock()
      self.disk.vm.SupportsNVMe.return_value = False

  def test_device_path_used_to_start_at_c(self):
    self.disk.machine_type = 'fake_v3'
    self.disk.lun = 0
    self.assertEqual('/dev/sdc', self.disk.GetDevicePath())

  def test_get_device_path_eq_z(self):
    self.disk.lun = 24
    self.assertEqual('/dev/sdz', self.disk.GetDevicePath())

  def test_get_device_path_eq_aa(self):
    self.disk.lun = 25
    self.assertEqual('/dev/sdaa', self.disk.GetDevicePath())

  def test_get_device_path_eq_ba(self):
    self.disk.lun = 51
    self.assertEqual('/dev/sdba', self.disk.GetDevicePath())

  def test_get_device_path_greatest_allowable_index(self):
    self.disk.lun = 700
    self.assertEqual('/dev/sdzz', self.disk.GetDevicePath())

  def test_get_device_path_index_too_large(self):
    self.disk.lun = 701
    with self.assertRaises(azure_disk.TooManyAzureDisksError):
      self.disk.GetDevicePath()


if __name__ == '__main__':
  unittest.main()
