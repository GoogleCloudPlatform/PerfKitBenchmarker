"""Azure Service Bus client interface.

This Azure ServiceBus client interface is implemented using Azure SDK for
Python:
https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/servicebus/azure-servicebus
"""
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
            subscription_name=self.subscription_name))

  def generate_message(
      self, seq, message_size: int) -> servicebus.ServiceBusMessage:
    return servicebus.ServiceBusMessage(
        super().generate_message(seq, message_size))

  def publish_message(self, message):
    self.topic_sender.send_messages(message)

  def pull_message(self, timeout: float = client.TIMEOUT):
    pulled_messages = self.subscription_receiver.receive_messages(
        max_message_count=1, max_wait_time=timeout)
    return pulled_messages[0] if pulled_messages else None

  def acknowledge_received_message(self, message):
    self.subscription_receiver.complete_message(message)

  def _get_first_six_bytes_from_payload(self, message) -> bytes:
    """Gets the first 6 bytes of a message (as returned by pull_message)."""
    return str(message)[:6].encode('utf-8')

  def close(self):
    self.topic_sender.close()
    self.subscription_receiver.close()
