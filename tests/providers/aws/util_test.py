"""Tests for util."""

import unittest

import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case


def _MockIssueCommand(file_name_text_response):
  path = pkb_common_test_case.GetTestDir() / 'data' / file_name_text_response
  with open(path) as f:
    output = f.read()
  return mock.patch.object(
      vm_util, 'IssueCommand', autospec=True, return_value=[output, None, None])


class AwsUtilTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetZonesInRegion(self):
    test_region = 'us-east-1'
    self.enter_context(
        _MockIssueCommand('aws-ec2-describe-availability-zones-output.json'))

    actual_zones = util.GetZonesInRegion(test_region)

    expected_zones = {
        'us-east-1a', 'us-east-1b', 'us-east-1c', 'us-east-1d', 'us-east-1e',
        'us-east-1f'
    }
    self.assertEqual(expected_zones, actual_zones)

  def testGetAllRegions(self):
    self.enter_context(
        _MockIssueCommand('aws-ec2-describe-regions-output.json'))

    actual_regions = util.GetAllRegions()

    expected_regions = {'af-south-1', 'us-west-2'}
    self.assertEqual(expected_regions, actual_regions)

  def testGetGeoFromRegion(self):
    test_region = 'us-west-2'

    found_geo = util.GetGeoFromRegion(test_region)

    expected_geo = 'us'
    self.assertEqual(found_geo, expected_geo)

  def testGetRegionsInGeo(self):
    self.enter_context(
        _MockIssueCommand('aws-ec2-describe-regions-output.json'))

    actual_regions = util.GetRegionsInGeo('us')

    expected_regions = {'us-west-2'}
    self.assertEqual(expected_regions, actual_regions)


if __name__ == '__main__':
  unittest.main()
