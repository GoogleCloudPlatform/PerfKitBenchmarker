"""Azure Service Bus client interface.

This Azure ServiceBus client interface is implemented using Azure SDK for
Python:
https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/servicebus/azure-servicebus
"""
import random

from absl import flags
# pytype: disable=import-error
from azure import servicebus
# pytype: enable=import-error

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'connection_str', '', help='Azure Service Bus connection string.')
flags.DEFINE_string(
    'topic_name', 'pkb-topic-default', help='Azure Service Bus topic name.')
flags.DEFINE_string(
    'subscription_name',
    'pkb-subscription-default',
    help='Azure Service Bus subscription name.')


class AzureServiceBusClient(client.BaseMessagingServiceClient):
  """Azure ServiceBus Client Class."""

  @classmethod
  def from_flags(cls):
    return cls(FLAGS.connection_str, FLAGS.topic_name, FLAGS.subscription_name)

  def __init__(self, connection_str: str, topic_name: str,
               subscription_name: str):
    self.connection_str = connection_str
    self.topic_name = topic_name
    self.subscription_name = subscription_name

    self.servicebus_client = servicebus.ServiceBusClient.from_connection_string(
        conn_str=self.connection_str, logging_enable=True)
    self.topic_sender = self.servicebus_client.get_topic_sender(
        topic_name=self.topic_name)
    self.subscription_receiver = (
        self.servicebus_client.get_subscription_receiver(
            topic_name=self.topic_name,
            subscription_name=self.subscription_name,
            max_wait_time=client.TIMEOUT))

  def generate_random_message(
      self, message_size: int) -> servicebus.ServiceBusMessage:
    message = ''.join(
        random.choice(client.MESSAGE_CHARACTERS)
        for _ in range(message_size))
    return servicebus.ServiceBusMessage(message)

  def publish_message(self, message):
    self.topic_sender.send_messages(message)

  def pull_message(self):
    pulled_message = self.subscription_receiver.receive_messages(
        max_message_count=1)
    return pulled_message

  def acknowledge_received_message(self, response):
    message = response[0]
    self.subscription_receiver.complete_message(message)

  def close(self):
    self.topic_sender.close()
    self.subscription_receiver.close()
