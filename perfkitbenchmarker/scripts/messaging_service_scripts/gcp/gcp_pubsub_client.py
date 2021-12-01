"""GCP specific PubSub interface.

This PubSub client is implemented using Google Cloud SDK.
"""
import random

from absl import flags
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PullResponse

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client

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
    self.publisher = pubsub_v1.PublisherClient()
    self.subscriber = pubsub_v1.SubscriberClient()

    self.topic_path = self.publisher.topic_path(self.project, self.topic)
    self.subscription_path = self.subscriber.subscription_path(
        self.project, self.subscription)

  def generate_random_message(self, message_size: int) -> bytes:
    message = ''.join(
        random.choice(client.MESSAGE_CHARACTERS) for _ in range(message_size))
    return message.encode('utf-8')

  def publish_message(self, message: bytes) -> str:
    """Publishes a single message to a PubSub topic."""
    published_message = self.publisher.publish(self.topic_path, message)
    message_id = published_message.result(timeout=client.TIMEOUT)
    return message_id

  def pull_message(self) -> PullResponse:
    """Pulls a single message from a PubSub Subscription."""
    # Cloud Pub/Sub has support for 2 different ways of retrieving messages:
    # Pull, and StreamingPull. We're using Pull, and doing 1 message / request.
    pulled_message = self.subscriber.pull(
        request={
            'subscription': self.subscription_path,
            'max_messages': 1
        },
        retry=retry.Retry(deadline=client.TIMEOUT))
    return pulled_message

  def acknowledge_received_message(self, response: PullResponse) -> None:
    if response.received_messages[0].message.data:
      ack_ids = []

      for received_message in response.received_messages:
        ack_ids.append(received_message.ack_id)
      # Acknowledges the received messages so they will not be sent again.
      self.subscriber.acknowledge(request={
          'subscription': self.subscription_path,
          'ack_ids': ack_ids
      })
