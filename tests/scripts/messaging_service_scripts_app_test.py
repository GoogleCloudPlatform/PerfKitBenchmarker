"""Tests for scripts/messaging_service_scripts/common/factories.py."""
import sys
import unittest
from unittest import mock

AZURE_MOCK = mock.Mock()
sys.modules['azure'] = AZURE_MOCK

from perfkitbenchmarker.scripts.messaging_service_scripts.aws import aws_sqs_client
from perfkitbenchmarker.scripts.messaging_service_scripts.azure import azure_service_bus_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.common import runners
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
    self.pull_runner_mock = self.enter_context(
        mock.patch.object(runners, 'PullLatencyRunner'))
    self.publish_runner_mock = self.enter_context(
        mock.patch.object(runners, 'PublishLatencyRunner'))

  def testRegisterRunner(self, benchmark_scenario_flag, *_):
    foo_runner = mock.Mock()
    bar_runner = mock.Mock()
    my_app = app.App()
    my_app._register_runner('foo', foo_runner)
    my_app._register_runner('bar', bar_runner)
    benchmark_scenario_flag.value = 'foo'
    self.assertEqual(my_app.get_runner_class(), foo_runner)
    benchmark_scenario_flag.value = 'bar'
    self.assertEqual(my_app.get_runner_class(), bar_runner)
    benchmark_scenario_flag.value = 'baz'
    with self.assertRaises(Exception):
      my_app.get_runner_class()

  def testRegisterClient(self, *_):
    my_app = app.App()
    with self.assertRaises(Exception):
      my_app.get_client_class()
    my_client = mock.Mock()
    my_app.register_client(my_client)
    self.assertEqual(my_app.get_client_class(), my_client)

  def testGetRunner(self, benchmark_scenario_flag, *_):
    my_app = app.App.for_client(gcp_pubsub_client.GCPPubSubClient)
    my_app._register_runners()

    benchmark_scenario_flag.value = 'pull_latency'
    runner = my_app.get_runner()
    self.gcp_client_mock.from_flags.assert_called_once()
    self.pull_runner_mock.run_class_startup.assert_called_once()
    self.pull_runner_mock.assert_called_once_with(
        self.gcp_client_mock.from_flags.return_value)
    self.assertEqual(runner, self.pull_runner_mock.return_value)

  @mock.patch.object(app.App, '_register_runners')
  @mock.patch.object(app.App, 'get_runner')
  def testCall(self, get_runner_mock, register_runners_mock, _,
               number_of_messages_mock, message_size_mock):
    my_app = app.App.get_instance()
    parent_mock = mock.Mock()
    parent_mock.attach_mock(register_runners_mock, 'register_runners')
    parent_mock.attach_mock(get_runner_mock, 'get_runner')
    my_app(None)
    parent_mock.assert_has_calls([
        mock.call.register_runners(),
        mock.call.get_runner(),
        mock.call.get_runner().run_phase(number_of_messages_mock.value,
                                         message_size_mock.value),
        mock.call.get_runner().close(),
    ])


if __name__ == '__main__':
  unittest.main()
