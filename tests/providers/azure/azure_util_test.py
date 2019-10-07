# Lint as: python3
"""Tests for perfkitbenchmarker.providers.azure.util."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case


class AzureUtilTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzureUtilTest, self).setUp()
    self.expected_location = 'eastus2'
    self.expected_availability_zone = '1'

  def test_get_location_from_zone_valid_location(self):
    valid_location = 'eastus2'
    self.assertEqual(self.expected_location,
                     util.GetLocationFromZone(valid_location))

  def test_get_location_from_zone_valid_zone(self):
    valid_zone = 'eastus2-1'
    self.assertEqual(self.expected_location,
                     util.GetLocationFromZone(valid_zone))

  def test_get_location_from_zone_invalid_location(self):
    valid_location = 'us-east2'
    with self.assertRaises(ValueError):
      util.GetLocationFromZone(valid_location)

  def test_get_location_from_zone_invalid_zone(self):
    valid_location = 'eastus2-1a'
    with self.assertRaises(ValueError):
      util.GetLocationFromZone(valid_location)

  def test_get_availability_zone_from_zone_valid_location(self):
    valid_location = 'eastus2'
    self.assertEqual(None, util.GetAvailabilityZoneFromZone(valid_location))

  def test_get_availability_zone_from_zone_valid_zone(self):
    valid_zone = 'eastus2-1'
    self.assertEqual(self.expected_availability_zone,
                     util.GetAvailabilityZoneFromZone(valid_zone))

  def test_get_availability_zone_from_zone_invalid_zone(self):
    valid_location = 'eastus2-1a'
    with self.assertRaises(ValueError):
      util.GetAvailabilityZoneFromZone(valid_location)


if __name__ == '__main__':
  unittest.main()
