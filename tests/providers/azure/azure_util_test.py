"""Tests for perfkitbenchmarker.providers.azure.util."""
import unittest
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case


def _MockIssueCommand(file_name_text_response):
  path = pkb_common_test_case.GetTestDir() / 'data' / file_name_text_response
  with open(path) as f:
    output = f.read()
  return mock.patch.object(
      vm_util, 'IssueCommand', autospec=True, return_value=[output, None, None])


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

  def test_get_zones_in_region(self):
    found_zones = util.GetZonesInRegion('westus2')

    expected_zones = {'westus2-1', 'westus2-2', 'westus2-3'}
    self.assertEqual(expected_zones, found_zones)

  def test_get_all_regions(self):
    self.enter_context(
        _MockIssueCommand('az-account-list-locations-output.json'))

    found_regions = util.GetAllRegions()

    self.assertEqual({'eastus', 'eastus2'}, found_regions)

  def test_get_all_zones(self):
    self.enter_context(
        mock.patch.object(
            util,
            'GetAllRegions',
            autospec=True,
            return_value={'eastus', 'eastus2'}))

    found_regions = util.GetAllZones()

    expected_zones = {
        'eastus-1',
        'eastus-2',
        'eastus-3',
        'eastus2-1',
        'eastus2-2',
        'eastus2-3',
    }
    self.assertEqual(expected_zones, found_regions)

  def test_get_geo_from_region(self):
    test_output = """[
      "US"
    ]
    """
    self.enter_context(mock.patch.object(vm_util, 'IssueRetryableCommand',
                                         autospec=True,
                                         return_value=[test_output, None]))

    found_geo = util.GetGeoFromRegion('test_region')

    expected_geo = 'US'
    self.assertEqual(found_geo, expected_geo)

  def test_get_region_in_geo(self):
    self.enter_context(
        _MockIssueCommand('az-account-list-locations-output.json'))

    found_regions = util.GetRegionsInGeo('test_geo')

    self.assertEqual({'eastus', 'eastus2'}, found_regions)

if __name__ == '__main__':
  unittest.main()
