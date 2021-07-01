"""Tests for gcp_pubsub."""

import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp.gcp_pubsub import GCPCloudPubSub
from tests import pkb_common_test_case

PROJECT = None
TOPIC = 'pkb-topic-uri'
SUBSCRIPTION = 'pkb-subscription-uri'
BENCHMARK_SCENARIO = 'pull_latency'
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10
MESSAGING_SERVICE_DATA_DIR = 'messaging_service'

FLAGS = flags.FLAGS


class GcpPubsubTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'uri'
    self.client = mock.Mock()
    self.pubsub = GCPCloudPubSub(self.client)

  def _MockIssueCommand(self, return_value):
    return self.enter_context(mock.patch.object(
        vm_util, 'IssueCommand', return_value=return_value))

  def testCreateTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._create_topic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics create '+TOPIC, cmd)

  def testCreateTopicError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.CreationError, self.pubsub._create_topic)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics create '+TOPIC, cmd)

  def testTopicExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    topic = self.pubsub._topic_exists()
    self.assertTrue(topic)

  def testNotFoundTopic(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    topic = self.pubsub._topic_exists()
    self.assertFalse(topic)

  def testDeleteTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._delete_topic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics delete '+TOPIC, cmd)

  def testDeleteTopicError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.RetryableDeletionError,
                      self.pubsub._delete_topic)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics delete '+TOPIC, cmd)

  def testCreateSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._create_subscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions create '+SUBSCRIPTION, cmd)

  def testCreateSubscriptionError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.CreationError,
                      self.pubsub._create_subscription)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions create '+SUBSCRIPTION, cmd)

  def testSubscriptionExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    subscription = self.pubsub._subscription_exists()
    self.assertTrue(subscription)

  def testNotFoundSubscription(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    subscription = self.pubsub._subscription_exists()
    self.assertFalse(subscription)

  def testDeleteSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._delete_subscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions delete '+SUBSCRIPTION, cmd)

  def testDeleteSubscriptionError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.RetryableDeletionError,
                      self.pubsub._delete_subscription)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions delete '+SUBSCRIPTION, cmd)

  @mock.patch.object(GCPCloudPubSub, '_create_subscription')
  @mock.patch.object(
      GCPCloudPubSub, '_subscription_exists', side_effect=[False, True, True])
  @mock.patch.object(GCPCloudPubSub, '_create_topic')
  @mock.patch.object(
      GCPCloudPubSub, '_topic_exists', side_effect=[False, True, True])
  def testProvisionResources(self, topic_exists_mock, create_topic_mock,
                             subscription_exists_mock,
                             create_subscription_mock):
    self.pubsub.provision_resources()

    create_subscription_mock.assert_called()
    subscription_exists_mock.assert_called()
    create_topic_mock.assert_called()
    topic_exists_mock.assert_called()

  @mock.patch.object(GCPCloudPubSub, 'provision_resources')
  def testPrepare(self, provision_mock):
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    sdk_cmd = ('sudo pip3 install --upgrade --ignore-installed '
               'google-cloud-pubsub')
    datafile_path = os.path.join(MESSAGING_SERVICE_DATA_DIR,
                                 'gcp_pubsub_client.py')

    self.pubsub.prepare()
    self.client.RemoteCommand.assert_called_with(sdk_cmd, ignore_failure=False)
    self.client.PushDataFile.assert_called_with(datafile_path)
    provision_mock.assert_called()

  def testRun(self):

    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (f'python3 -m gcp_pubsub_client '
                      f'--project={PROJECT} '
                      f'--pubsub_topic={TOPIC} '
                      f'--pubsub_subscription={SUBSCRIPTION} '
                      f'--benchmark_scenario={BENCHMARK_SCENARIO} '
                      f'--number_of_messages={NUMBER_OF_MESSAGES} '
                      f'--message_size={MESSAGE_SIZE} ')

    self.pubsub.run(BENCHMARK_SCENARIO, NUMBER_OF_MESSAGES,
                    MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)

  @mock.patch.object(GCPCloudPubSub, '_delete_subscription')
  @mock.patch.object(
      GCPCloudPubSub, '_subscription_exists', side_effect=[False, True])
  @mock.patch.object(GCPCloudPubSub, '_delete_topic')
  @mock.patch.object(GCPCloudPubSub, '_topic_exists', side_effect=[False, True])
  def testCleanup(self, topic_exists_mock, delete_topic_mock,
                  subscription_exists_mock, delete_subscription_mock):
    self.pubsub.cleanup()

    delete_subscription_mock.assert_called()
    subscription_exists_mock.assert_called()
    delete_topic_mock.assert_called()
    topic_exists_mock.assert_called()


if __name__ == '__main__':
  unittest.main()
