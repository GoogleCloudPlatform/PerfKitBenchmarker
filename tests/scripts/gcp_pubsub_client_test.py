"""Tests for data/messaging_service/gcp_pubsub_client.py."""
import unittest
from unittest import mock

from perfkitbenchmarker.scripts.messaging_service_scripts.gcp import gcp_pubsub_client

NUMBER_OF_MESSAGES = 1
MESSAGE_SIZE = 10
PROJECT = 'pkb_test_project'
TOPIC = 'pkb_test_topic'
SUBSCRIPTION = 'pkb_test_subscription'


@mock.patch('google.cloud.pubsub_v1.PublisherClient')
@mock.patch('google.cloud.pubsub_v1.SubscriberClient')
class GCPPubSubClientTest(unittest.TestCase):

  def testPublishMessage(self, _, publisher_mock):
    message = 'test_message'.encode('utf-8')
    topic_path = publisher_mock.return_value.topic_path.return_value = 'test_topic_path'

    gcp_interface = gcp_pubsub_client.GCPPubSubClient(PROJECT, TOPIC,
                                                      SUBSCRIPTION)
    gcp_interface.publish_message(message)

    # assert publish was called
    publisher_mock.return_value.publish.assert_called_with(topic_path, message)

  def testPullMessage(self, subscriber_mock, _):
    gcp_interface = gcp_pubsub_client.GCPPubSubClient(PROJECT, TOPIC,
                                                      SUBSCRIPTION)
    gcp_interface.pull_message()

    # assert pull was called
    subscriber_mock.return_value.pull.assert_called()

  def testAcknowledgeReceivedMessage(self, subscriber_mock, _):
    response_mock = mock.MagicMock()
    response_mock.return_value.received_messages[
        0].message.data = 'mocked_message'

    gcp_interface = gcp_pubsub_client.GCPPubSubClient(PROJECT, TOPIC,
                                                      SUBSCRIPTION)
    gcp_interface.acknowledge_received_message(response_mock)

    # assert acknowledge was called
    subscriber_mock.return_value.acknowledge.assert_called()


if __name__ == '__main__':
  unittest.main()
