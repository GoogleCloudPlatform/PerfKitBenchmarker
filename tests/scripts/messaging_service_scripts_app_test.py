"""Tests for scripts/messaging_service_scripts/common/factories.py."""
import sys
import unittest
from unittest import mock

AZURE_MOCK = mock.Mock()
sys.modules['azure'] = AZURE_MOCK

from perfkitbenchmarker.scripts.messaging_service_scripts.aws import aws_sqs_client
from perfkitbenchmarker.scripts.messaging_service_scripts.azure import azure_service_bus_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.gcp import gcp_pubsub_client
from tests import pkb_common_test_case


@mock.patch.object(app, '_MESSAGE_SIZE')
@mock.patch.object(app, '_NUMBER_OF_MESSAGES')
@mock.patch.object(app, '_BENCHMARK_SCENARIO')
class MessagingServiceScriptsAppTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.gcp_client_mock = self.enter_context(
        mock.patch.object(gcp_pubsub_client, 'GCPPubSubClient'))
    self.aws_client_mock = self.enter_context(
        mock.patch.object(aws_sqs_client, 'AwsSqsClient'))
    self.azure_client_mock = self.enter_context(
        mock.patch.object(azure_service_bus_client, 'AzureServiceBusClient'))

  def testRegisterClient(self, *_):
    my_app = app.App()
    with self.assertRaises(Exception):
      my_app.get_client_class()
    my_client = mock.Mock()
    my_app.register_client(my_client)
    self.assertEqual(my_app.get_client_class(), my_client)

  def testGetClient(self, *_):
    my_app = app.App.for_client(gcp_pubsub_client.GCPPubSubClient)
    my_app.get_client()
    self.gcp_client_mock.from_flags.assert_called_once()


if __name__ == '__main__':
  unittest.main()
