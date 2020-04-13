"""Tests for perfkitbenchmarker.tests.providers.azure_disk."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from absl import flags
import mock

from perfkitbenchmarker.providers.azure import azure_disk
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AzureDiskGetDevicePathTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzureDiskGetDevicePathTest, self).setUp()
    # Patch the __init__ method for simplicity.
    with mock.patch.object(azure_disk.AzureDisk, '__init__', lambda self: None):
      self.disk = azure_disk.AzureDisk()
      self.disk.disk_type = 'NOT_LOCAL'
      self.disk.machine_type = 'fake'

  def test_get_device_path_starts_at_c(self):
    self.disk.lun = 0
    self.assertEqual('/dev/sdc', self.disk.GetDevicePath())

  def test_get_device_path_eq_z(self):
    self.disk.lun = 23
    self.assertEqual('/dev/sdz', self.disk.GetDevicePath())

  def test_get_device_path_eq_aa(self):
    self.disk.lun = 24
    self.assertEqual('/dev/sdaa', self.disk.GetDevicePath())

  def test_get_device_path_eq_ba(self):
    self.disk.lun = 50
    self.assertEqual('/dev/sdba', self.disk.GetDevicePath())

  def test_get_device_path_greatest_allowable_index(self):
    self.disk.lun = 699
    self.assertEqual('/dev/sdzz', self.disk.GetDevicePath())

  def test_get_device_path_index_too_large(self):
    self.disk.lun = 700
    with self.assertRaises(azure_disk.TooManyAzureDisksError):
      self.disk.GetDevicePath()


if __name__ == '__main__':
  unittest.main()
