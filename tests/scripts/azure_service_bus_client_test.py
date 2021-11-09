"""Tests for scripts/messaging_service_scripts/azure/azure_service_bus_client.py."""
import sys
import unittest
from unittest import mock

AZURE_MOCK = mock.Mock()
sys.modules['azure'] = AZURE_MOCK
from perfkitbenchmarker.scripts.messaging_service_scripts.azure import azure_service_bus_client

_MESSAGE_SIZE = 10
_CONNECTION_STRING = 'pkb_test_string'
_TOPIC = 'pkb_test_topic'
_SUBSCRIPTION = 'pkb_test_subscription'


class AzureServiceBusClientTest(unittest.TestCase):

  def testGenerateRandomMessage(self):
    azure_interface = azure_service_bus_client.AzureServiceBusClient(
        _CONNECTION_STRING, _TOPIC, _SUBSCRIPTION)
    azure_interface.generate_random_message(_MESSAGE_SIZE)

    AZURE_MOCK.servicebus.ServiceBusMessage.assert_called()

  def testPublishMessage(self):
    message = 'mocked_message'
    azure_interface = azure_service_bus_client.AzureServiceBusClient(
        _CONNECTION_STRING, _TOPIC, _SUBSCRIPTION)
    azure_interface.publish_message(message)
    client = AZURE_MOCK.servicebus.ServiceBusClient
    connection_str = client.from_connection_string.return_value
    topic_sender = connection_str.get_topic_sender.return_value

    # assert publish was called
    topic_sender.send_messages.assert_called_with(message)

  def testPullMessage(self):
    azure_interface = azure_service_bus_client.AzureServiceBusClient(
        _CONNECTION_STRING, _TOPIC, _SUBSCRIPTION)
    azure_interface.pull_message()
    client = AZURE_MOCK.servicebus.ServiceBusClient
    connection_str = client.from_connection_string.return_value
    subscription_receiver = (
        connection_str.get_subscription_receiver.return_value)

    # assert pull was called
    subscription_receiver.receive_messages.assert_called_with(
        max_message_count=1)

  def testAcknowledgeReceivedMessage(self):
    message = ['mocked_message']
    azure_interface = azure_service_bus_client.AzureServiceBusClient(
        _CONNECTION_STRING, _TOPIC, _SUBSCRIPTION)
    azure_interface.acknowledge_received_message(message)

    client = AZURE_MOCK.servicebus.ServiceBusClient
    connection_str = client.from_connection_string.return_value
    subscription_receiver = (
        connection_str.get_subscription_receiver.return_value)
    # assert acknowledge was called
    subscription_receiver.complete_message.assert_called_with(message[0])


if __name__ == '__main__':
  unittest.main()
