"""Tests for data_discovery_service."""

import unittest
from unittest import mock

from perfkitbenchmarker import data_discovery_service
from tests import pkb_common_test_case


class FakeDataDiscoveryService(data_discovery_service.BaseDataDiscoveryService):

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def DiscoverData(self) -> float:
    return 0


class DataDiscoveryServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.path_flag_mock = self.enter_context(
        mock.patch.object(data_discovery_service,
                          '_DATA_DISCOVERY_OBJECT_STORE_PATH'))
    self.path_flag_mock.value = 's3://foo/bar'
    self.region_flag_mock = self.enter_context(
        mock.patch.object(data_discovery_service, '_DATA_DISCOVERY_REGION'))
    self.region_flag_mock.value = 'us-east-1'
    self.service = FakeDataDiscoveryService()

  def testInitialization(self):
    self.assertFalse(self.service.user_managed)
    self.assertEqual(self.service.data_discovery_path, 's3://foo/bar')
    self.assertEqual(self.service.region, 'us-east-1')

  def testGetMetadata(self):
    self.assertEqual(self.service.GetMetadata(), {
        'cloud': 'abstract',
        'data_discovery_region': 'us-east-1'
    })


if __name__ == '__main__':
  unittest.main()
