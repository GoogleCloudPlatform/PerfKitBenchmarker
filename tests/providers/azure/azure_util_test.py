"""Tests for perfkitbenchmarker.providers.azure.util."""


import unittest

from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case


class AzureUtilTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzureUtilTest, self).setUp()
    self.expected_region = 'eastus2'
    self.expected_availability_zone = '1'

  def test_get_region_from_zone_valid_region(self):
    valid_region = 'eastus2'
    self.assertEqual(self.expected_region,
                     util.GetRegionFromZone(valid_region))

  def test_get_region_from_zone_valid_zone(self):
    valid_zone = 'eastus2-1'
    self.assertEqual(self.expected_region,
                     util.GetRegionFromZone(valid_zone))

  def test_get_region_from_zone_invalid_region(self):
    valid_region = 'us-east2'
    with self.assertRaises(ValueError):
      util.GetRegionFromZone(valid_region)

  def test_get_region_from_zone_invalid_zone(self):
    valid_region = 'eastus2-1a'
    with self.assertRaises(ValueError):
      util.GetRegionFromZone(valid_region)

  def test_get_availability_zone_from_zone_valid_region(self):
    valid_region = 'eastus2'
    self.assertIsNone(util.GetAvailabilityZoneFromZone(valid_region))

  def test_get_availability_zone_from_zone_valid_zone(self):
    valid_zone = 'eastus2-1'
    self.assertEqual(self.expected_availability_zone,
                     util.GetAvailabilityZoneFromZone(valid_zone))

  def test_get_availability_zone_from_zone_invalid_zone(self):
    valid_region = 'eastus2-1a'
    with self.assertRaises(ValueError):
      util.GetAvailabilityZoneFromZone(valid_region)


if __name__ == '__main__':
  unittest.main()
