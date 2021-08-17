"""Tests for azure_service_bus."""
import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_service_bus
from tests import pkb_common_test_case

_REGION = 'eastus'
BENCHMARK_SCENARIO = 'pull_latency'
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10
MESSAGING_SERVICE_DATA_DIR = 'messaging_service'

FLAGS = flags.FLAGS


class AzureServiceBusTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(azure_network, 'GetResourceGroup')
  def setUp(self, resource_group_mock):
    super().setUp()
    FLAGS.run_uri = 'uri'
    resource_group_mock.return_value.args = ['mocked_args']
    self.client = mock.Mock()
    self.client.zone = _REGION
    self.servicebus = azure_service_bus.AzureServiceBus(self.client)

  def _MockIssueCommand(self, return_value):
    return self.enter_context(mock.patch.object(
        vm_util, 'IssueCommand', return_value=return_value))

  def testCreateTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._create_topic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic create --name ' + self.servicebus.topic_name +
        ' --namespace-name ' + self.servicebus.namespace_name, cmd)

  def testTopicExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    topic = self.servicebus._topic_exists()
    self.assertTrue(topic)

  def testNotFoundTopic(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    topic = self.servicebus._topic_exists()
    self.assertFalse(topic)

  def testDeleteTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._delete_topic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus topic delete --name ' + self.servicebus.topic_name +
        ' --namespace-name ' + self.servicebus.namespace_name, cmd)

  def testCreateSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._create_subscription()
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

    subscription = self.servicebus._subscription_exists()
    self.assertTrue(subscription)

  def testNotFoundSubscription(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    subscription = self.servicebus._subscription_exists()
    self.assertFalse(subscription)

  def testDeleteSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._delete_subscription()
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

    self.servicebus._create_namespace()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace create --name ' + self.servicebus.namespace_name +
        ' --location ' + self.servicebus.location, cmd)

  def testNamespaceExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    namespace = self.servicebus._namespace_exists()
    self.assertTrue(namespace)

  def testNamespaceDoesntExist(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    namespace = self.servicebus._namespace_exists()
    self.assertFalse(namespace)

  def testDeleteNamespace(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._delete_namespace()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace delete --name ' +
        self.servicebus.namespace_name, cmd)

  def testGetConnectionString(self):
    # Don't actually issue a command.
    return_value = ['', None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.servicebus._get_primary_connection_string()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'servicebus namespace authorization-rule keys list ' +
        '--name=RootManageSharedAccessKey --namespace-name ' +
        self.servicebus.namespace_name +
        ' --query=primaryConnectionString -o=tsv', cmd)

  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_namespace')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_namespace_exists',
      side_effect=[False, True])
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_subscription')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_subscription_exists',
      side_effect=[False, True])
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_topic')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_topic_exists',
      side_effect=[False, True])
  def testProvisionResources(self, topic_exists_mock, create_topic_mock,
                             subscription_exists_mock,
                             create_subscription_mock,
                             namespace_exists_mock,
                             create_namespace_mock):
    self.servicebus.provision_resources()

    self.assertEqual(create_namespace_mock.call_count, 1)
    self.assertEqual(namespace_exists_mock.call_count, 2)
    self.assertEqual(create_subscription_mock.call_count, 1)
    self.assertEqual(subscription_exists_mock.call_count, 2)
    self.assertEqual(create_topic_mock.call_count, 1)
    self.assertEqual(topic_exists_mock.call_count, 2)

  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_namespace')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_namespace_exists',
      return_value=False)
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_subscription')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_subscription_exists',
      return_value=False)
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_create_topic')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus, '_topic_exists', return_value=False)
  def testProvisionResourcesException(
      self, topic_exists_mock, create_topic_mock, subscription_exists_mock,
      create_subscription_mock, create_namespace_mock, namespace_exists_mock):
    self.assertRaises(errors.Benchmarks.PrepareException,
                      self.servicebus.provision_resources)

  @mock.patch.object(azure_service_bus.AzureServiceBus, 'provision_resources')
  def testPrepare(self, provision_mock):
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    sdk_cmd = ('sudo pip3 install azure-servicebus')
    datafile_path = os.path.join(MESSAGING_SERVICE_DATA_DIR,
                                 'azure_service_bus_client.py')

    self.servicebus.prepare()
    self.client.RemoteCommand.assert_called_with(sdk_cmd, ignore_failure=False)
    self.client.PushDataFile.assert_called_with(datafile_path)
    provision_mock.assert_called()

  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_get_primary_connection_string',
      return_value='mocked_string')
  def testRun(self, get_connection_string_mock):

    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (
        f'python3 -m azure_service_bus_client '
        f'--topic_name={self.servicebus.topic_name} '
        f'--subscription_name={self.servicebus.subscription_name} '
        f'--benchmark_scenario={BENCHMARK_SCENARIO} '
        f'--number_of_messages={NUMBER_OF_MESSAGES} '
        f'--message_size={MESSAGE_SIZE} '
        f'--connection_str="mocked_string" ')

    self.servicebus.run(BENCHMARK_SCENARIO, NUMBER_OF_MESSAGES, MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)

  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_namespace')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_namespace_exists',
      side_effect=[True, False])
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_subscription')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_subscription_exists',
      side_effect=[True, False])
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_topic')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_topic_exists',
      side_effect=[True, False])
  def testCleanup(self, topic_exists_mock, delete_topic_mock,
                  subscription_exists_mock, delete_subscription_mock,
                  namespace_exists_mock, delete_namespace_mock):
    self.servicebus.cleanup()
    self.assertEqual(delete_namespace_mock.call_count, 1)
    self.assertEqual(namespace_exists_mock.call_count, 2)
    self.assertEqual(delete_subscription_mock.call_count, 1)
    self.assertEqual(subscription_exists_mock.call_count, 2)
    self.assertEqual(delete_topic_mock.call_count, 1)
    self.assertEqual(topic_exists_mock.call_count, 2)

  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_namespace')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus, '_namespace_exists', return_value=True)
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_subscription')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus,
      '_subscription_exists',
      return_value=True)
  @mock.patch.object(azure_service_bus.AzureServiceBus, '_delete_topic')
  @mock.patch.object(
      azure_service_bus.AzureServiceBus, '_topic_exists', return_value=True)
  def testCleanupException(self, topic_exists_mock, delete_topic_mock,
                           subscription_exists_mock, delete_subscription_mock,
                           namespace_exists_mock, delete_namespace_mock):
    self.assertRaises(errors.Resource.CleanupError, self.servicebus.cleanup)

if __name__ == '__main__':
  unittest.main()
