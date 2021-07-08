"""Tests for messaging_service_util.py."""
import unittest

import mock
from perfkitbenchmarker import messaging_service_util
from tests import pkb_common_test_case


class MessagingServiceUtilTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch('perfkitbenchmarker.providers.gcp.gcp_pubsub.GCPCloudPubSub')
  def testGetInstanceGCP(self, gcp_instance):
    mock_client = 'mock_client'
    messaging_service_util.get_instance(mock_client, 'GCP')
    gcp_instance.assert_called_with(mock_client)

  def testGetInstanceAWS(self):
    self.assertRaises(NotImplementedError, messaging_service_util.get_instance,
                      'mock_client', 'AWS')


if __name__ == '__main__':
  unittest.main()
