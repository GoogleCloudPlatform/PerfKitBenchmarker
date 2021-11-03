"""Tests for gcp_pubsub."""

import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_pubsub as pubsub
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
    self.pubsub = pubsub.GCPCloudPubSub()
    self.pubsub.client_vm = self.client

  def _MockIssueCommand(self, return_value):
    return self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=return_value))

  def testCreateTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._CreateTopic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics create ' + TOPIC, cmd)

  def testCreateTopicError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.CreationError, self.pubsub._CreateTopic)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics create ' + TOPIC, cmd)

  def testTopicExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    topic = self.pubsub._TopicExists()
    self.assertTrue(topic)

  def testNotFoundTopic(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    topic = self.pubsub._TopicExists()
    self.assertFalse(topic)

  def testDeleteTopic(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._DeleteTopic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics delete ' + TOPIC, cmd)

  def testDeleteTopicError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._DeleteTopic()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub topics delete ' + TOPIC, cmd)

  def testCreateSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._CreateSubscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions create ' + SUBSCRIPTION, cmd)

  def testCreateSubscriptionError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.assertRaises(errors.Resource.CreationError,
                      self.pubsub._CreateSubscription)
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions create ' + SUBSCRIPTION, cmd)

  def testSubscriptionExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    subscription = self.pubsub._SubscriptionExists()
    self.assertTrue(subscription)

  def testNotFoundSubscription(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    self._MockIssueCommand(return_value)

    subscription = self.pubsub._SubscriptionExists()
    self.assertFalse(subscription)

  def testDeleteSubscription(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._DeleteSubscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions delete ' + SUBSCRIPTION, cmd)

  def testDeleteSubscriptionError(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    self.pubsub._DeleteSubscription()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn('gcloud pubsub subscriptions delete ' + SUBSCRIPTION, cmd)

  @mock.patch.object(pubsub.GCPCloudPubSub, '_CreateSubscription')
  @mock.patch.object(pubsub.GCPCloudPubSub, '_CreateTopic')
  def testCreate(self, create_topic_mock, create_subscription_mock):
    self.pubsub._Create()
    create_subscription_mock.assert_called()
    create_topic_mock.assert_called()

  def testPrepareClientVm(self):
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    sdk_cmd = ('sudo pip3 install --upgrade --ignore-installed '
               'google-cloud-pubsub')
    datafile_path = os.path.join(MESSAGING_SERVICE_DATA_DIR,
                                 'gcp_pubsub_client.py')

    self.pubsub.PrepareClientVm()
    self.client.RemoteCommand.assert_called_with(sdk_cmd, ignore_failure=False)
    self.client.PushDataFile.assert_called_with(datafile_path)

  def testRun(self):

    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (f'python3 -m gcp_pubsub_client '
                      f'--pubsub_project={PROJECT} '
                      f'--pubsub_topic={TOPIC} '
                      f'--pubsub_subscription={SUBSCRIPTION} '
                      f'--benchmark_scenario={BENCHMARK_SCENARIO} '
                      f'--number_of_messages={NUMBER_OF_MESSAGES} '
                      f'--message_size={MESSAGE_SIZE} ')

    self.pubsub.Run(BENCHMARK_SCENARIO, NUMBER_OF_MESSAGES, MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)

  @mock.patch.object(pubsub.GCPCloudPubSub, '_DeleteSubscription')
  @mock.patch.object(pubsub.GCPCloudPubSub, '_DeleteTopic')
  def testDelete(self, delete_topic_mock, delete_subscription_mock):
    self.pubsub._Delete()
    delete_subscription_mock.assert_called()
    delete_topic_mock.assert_called()


if __name__ == '__main__':
  unittest.main()
