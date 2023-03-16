"""GCP specific PubSub interface.

This PubSub client is implemented using Google Cloud SDK.
"""
import datetime
import typing
from typing import Any, Callable, Optional, Union

from absl import flags
from google.api_core import exceptions
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import types
from google.cloud.pubsub_v1.subscriber import message as pubsub_message

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client

from google.protobuf import timestamp_pb2

BATCH_SETTINGS_MAX_MESSAGES = 1
BATCH_SETTINGS_MAX_BYTES = 1
BATCH_SETTINGS_MAX_LATENCY = 0

FLAGS = flags.FLAGS

flags.DEFINE_string('pubsub_project', '', help='Project name.')
flags.DEFINE_string('pubsub_topic', 'pkb-topic-default', help='Topic name.')
flags.DEFINE_string(
    'pubsub_subscription',
    'pkb-subscription-default',
    help='Subscription name.')


class GCPPubSubClient(client.BaseMessagingServiceClient):
  """GCP PubSub Client Class.

  This class takes care of running the specified benchmark on GCP PubSub.
  """

  @classmethod
  def from_flags(cls):
    return cls(FLAGS.pubsub_project, FLAGS.pubsub_topic,
               FLAGS.pubsub_subscription)

  def __init__(self, project: str, topic: str, subscription: str):
    self.project = project
    self.topic = topic
    self.subscription = subscription
    batch_settings = types.BatchSettings(
        max_messages=BATCH_SETTINGS_MAX_MESSAGES,
        max_bytes=BATCH_SETTINGS_MAX_BYTES,
        max_latency=BATCH_SETTINGS_MAX_LATENCY,
    )
    self.publisher = pubsub_v1.PublisherClient(batch_settings)
    self.subscriber = pubsub_v1.SubscriberClient()

    self.topic_path = self.publisher.topic_path(self.project, self.topic)
    self.subscription_path = self.subscriber.subscription_path(
        self.project, self.subscription)

  def generate_message(self, seq: int, message_size: int) -> bytes:  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    return super().generate_message(seq, message_size).encode('utf-8')

  def publish_message(self, message: bytes) -> str:
    """Publishes a single message to a PubSub topic."""
    published_message = self.publisher.publish(self.topic_path, message)
    message_id = published_message.result(timeout=client.TIMEOUT)
    return message_id

  def pull_message(
      self, timeout: float = client.TIMEOUT) -> Optional[types.ReceivedMessage]:
    """See base class."""
    # Cloud Pub/Sub has support for 2 different ways of retrieving messages:
    # Pull, and StreamingPull. We're using Pull, and doing 1 message / request.
    try:
      pulled_messages = self.subscriber.pull(
          request={
              'subscription': self.subscription_path,
              'max_messages': 1
          },
          retry=retry.Retry(deadline=timeout),
          timeout=timeout)
      return (pulled_messages.received_messages[0]
              if pulled_messages.received_messages else None)
    except exceptions.DeadlineExceeded:
      return None

  def acknowledge_received_message(
      self,
      message: Union[types.ReceivedMessage, pubsub_message.Message]) -> None:
    # Acknowledges the received message so it will not be sent again.
    self.subscriber.acknowledge(request={
        'subscription': self.subscription_path,
        'ack_ids': [message.ack_id]
    })

  def _get_first_six_bytes_from_payload(
      self,
      message: Union[types.ReceivedMessage, pubsub_message.Message]) -> bytes:
    """Gets the first 6 bytes of a message (as returned by pull_message or PullingStream)."""
    if isinstance(message, types.ReceivedMessage):
      return typing.cast(types.ReceivedMessage, message).message.data[:6]
    elif isinstance(message, pubsub_message.Message):
      return typing.cast(pubsub_message.Message, message).data[:6]
    else:
      raise ValueError(f'Got unexpected message type: {type(message)}.')

  def purge_messages(self) -> None:
    """Purges all the messages for the underlying service."""
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(
        datetime.datetime.now() + datetime.timedelta(days=30))
    request = types.SeekRequest(
        subscription=self.subscription,
        time=timestamp,)
    self.subscriber.seek(request=request)

  def start_streaming_pull(self, callback: Callable[[Any], None]) -> None:
    """See base class."""
    self.subscriber.subscribe(self.subscription_path, callback=callback)

  def close(self):
    self.subscriber.close()
