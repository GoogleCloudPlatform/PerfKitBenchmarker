"""Tests for azure_service_bus."""
import unittest

from absl import flags
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_service_bus as asb
from tests import pkb_common_test_case

_REGION = 'eastus'
BENCHMARK_SCENARIO = 'pull_latency'
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10
MESSAGING_SERVICE_DATA_DIR = 'messaging_service_scripts'

FLAGS = flags.FLAGS


class AzureServiceBusTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(azure_network, 'GetResourceGroup')
  def setUp(self, resource_group_mock):
    super().setUp()
    FLAGS.run_uri = 'uri'
    resource_group_mock.return_value.args = ['mocked_args']
    self.client = mock.Mock()
    self.client.zone = _REGION
    self.servicebus = asb.AzureServiceBus()
    self.servicebus.client_vm = self.client

  def _MockIssueCommand(self, return_value):
    return self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=return_value))

  def testCreateTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._CreateTopic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic create --name ' + self.servicebus.topic_name +
        ' --namespace-name ' + self.servicebus.namespace_name, cmd)

  def testTopicExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    topic = self.servicebus._TopicExists()
    self.assertTrue(topic)

  def testNotFoundTopic(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    topic = self.servicebus._TopicExists()
    self.assertFalse(topic)

  def testDeleteTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._DeleteTopic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic delete --name ' + self.servicebus.topic_name +
        ' --namespace-name ' + self.servicebus.namespace_name, cmd)

  def testCreateSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._CreateSubscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic subscription create --name ' +
        self.servicebus.subscription_name + ' --topic-name ' +
        self.servicebus.topic_name + ' --namespace-name ' +
        self.servicebus.namespace_name, cmd)

  def testSubscriptionExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    subscription = self.servicebus._SubscriptionExists()
    self.assertTrue(subscription)

  def testNotFoundSubscription(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    subscription = self.servicebus._SubscriptionExists()
    self.assertFalse(subscription)

  def testDeleteSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._DeleteSubscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic subscription delete --name ' +
        self.servicebus.subscription_name + ' --topic-name ' +
        self.servicebus.topic_name + ' --namespace-name ' +
        self.servicebus.namespace_name, cmd)

  def testCreateNamespace(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._CreateNamespace()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace create --name ' + self.servicebus.namespace_name +
        ' --location ' + self.servicebus.location, cmd)

  def testNamespaceExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    namespace = self.servicebus._NamespaceExists()
    self.assertTrue(namespace)

  def testNamespaceDoesntExist(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    namespace = self.servicebus._NamespaceExists()
    self.assertFalse(namespace)

  def testDeleteNamespace(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._DeleteNamespace()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace delete --name ' + self.servicebus.namespace_name,
        cmd)

  def testGetConnectionString(self):
    # Don't actually issue a command.
    return_value = ['', None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._GetPrimaryConnectionString()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace authorization-rule keys list ' +
        '--name=RootManageSharedAccessKey --namespace-name ' +
        self.servicebus.namespace_name +
        ' --query=primaryConnectionString -o=tsv', cmd)

  @mock.patch.object(asb.AzureServiceBus, '_CreateNamespace')
  @mock.patch.object(asb.AzureServiceBus, '_CreateSubscription')
  @mock.patch.object(asb.AzureServiceBus, '_CreateTopic')
  def testCreate(self, create_topic_mock, create_subscription_mock,
                 create_namespace_mock):
    self.servicebus._Create()
    self.assertEqual(create_namespace_mock.call_count, 1)
    self.assertEqual(create_subscription_mock.call_count, 1)
    self.assertEqual(create_topic_mock.call_count, 1)

  def testPrepareClientVm(self):
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    self.servicebus.PrepareClientVm()
    self.client.assert_has_calls([
        mock.call.RemoteCommand(
            'sudo pip3 install azure-servicebus', ignore_failure=False),
        mock.call.RemoteCommand(
            'mkdir -p ~/perfkitbenchmarker/scripts/messaging_service_scripts/azure'
        ),
        mock.call.PushDataFile(
            'messaging_service_scripts/azure/__init__.py',
            '~/perfkitbenchmarker/scripts/messaging_service_scripts/azure/__init__.py'
        ),
        mock.call.RemoteCommand(
            'mkdir -p ~/perfkitbenchmarker/scripts/messaging_service_scripts/azure'
        ),
        mock.call.PushDataFile(
            'messaging_service_scripts/azure/azure_service_bus_client.py',
            '~/perfkitbenchmarker/scripts/messaging_service_scripts/azure/azure_service_bus_client.py'
        ),
        mock.call.PushDataFile('messaging_service_scripts/azure_benchmark.py'),
    ])

  @mock.patch.object(
      asb.AzureServiceBus,
      '_GetPrimaryConnectionString',
      return_value='mocked_string')
  def testRun(self, get_connection_string_mock):

    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (
        f'python3 -m azure_benchmark '
        f'--topic_name={self.servicebus.topic_name} '
        f'--subscription_name={self.servicebus.subscription_name} '
        f'--benchmark_scenario={BENCHMARK_SCENARIO} '
        f'--number_of_messages={NUMBER_OF_MESSAGES} '
        f'--message_size={MESSAGE_SIZE} '
        f'--connection_str="mocked_string" ')

    self.servicebus.Run(BENCHMARK_SCENARIO, NUMBER_OF_MESSAGES, MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)

  @mock.patch.object(asb.AzureServiceBus, '_DeleteNamespace')
  @mock.patch.object(asb.AzureServiceBus, '_DeleteSubscription')
  @mock.patch.object(asb.AzureServiceBus, '_DeleteTopic')
  def testDelete(self, delete_topic_mock, delete_subscription_mock,
                 delete_namespace_mock):
    self.servicebus._Delete()
    self.assertEqual(delete_namespace_mock.call_count, 1)
    self.assertEqual(delete_subscription_mock.call_count, 1)
    self.assertEqual(delete_topic_mock.call_count, 1)


if __name__ == '__main__':
  unittest.main()
