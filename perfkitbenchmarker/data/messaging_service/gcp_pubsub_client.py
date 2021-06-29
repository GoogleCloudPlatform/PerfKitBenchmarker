"""GCP specific PubSub interface.

This PubSub client is implemented using Google Cloud SDK.
"""
# pylint: disable=g-import-not-at-top
import sys

from absl import flags
from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PullResponse
# see PEP 366 @ ReservedAssignment
if __name__ == '__main__' and not __package__:
  # import for client VM
  from messaging_service_client import MessagingServiceClient
  from messaging_service_client import TIMEOUT
else:
  # import for blaze test
  from perfkitbenchmarker.data.messaging_service.messaging_service_client import MessagingServiceClient
  from perfkitbenchmarker.data.messaging_service.messaging_service_client import TIMEOUT

FLAGS = flags.FLAGS

flags.DEFINE_string('project', '', help='Project name.')
flags.DEFINE_string('pubsub_topic', 'pkb-topic-default', help='Topic name.')
flags.DEFINE_string('pubsub_subscription',
                    'pkb-subscription-default',
                    help='Subscription name.')

FLAGS(sys.argv)


class GCPPubSubInterface(MessagingServiceClient):
  """GCP PubSub Interface Class.

  This class takes care of running the specified benchmark on GCP PubSub.
  """

  def __init__(self, project: str, topic: str, subscription: str):
    self.project = project
    self.topic = topic
    self.subscription = subscription
    self.publisher = pubsub_v1.PublisherClient()
    self.subscriber = pubsub_v1.SubscriberClient()

    self.topic_path = self.publisher.topic_path(self.project, self.topic)
    self.subscription_path = self.subscriber.subscription_path(
        self.project,
        self.subscription)

  def _publish_message(self, message: bytes) -> str:
    """Publishes a single message to a PubSub topic."""
    published_message = self.publisher.publish(
        self.topic_path,
        message)
    message_id = published_message.result(timeout=TIMEOUT)
    return message_id

  def _pull_message(self) -> PullResponse:
    """Pulls a single message from a PubSub Subscription."""
    # Cloud Pub/Sub has support for 2 different ways of retrieving messages:
    # Pull, and StreamingPull. We're using Pull, and doing 1 message / request.
    pulled_message = self.subscriber.pull(
        request={
            'subscription': self.subscription_path,
            'max_messages': 1
        },
        retry=retry.Retry(deadline=TIMEOUT))
    return pulled_message

  def _acknowledges_received_message(self, response: PullResponse) -> None:
    if response.received_messages[0].message.data:
      ack_ids = []

      for received_message in response.received_messages:
        ack_ids.append(received_message.ack_id)
      # Acknowledges the received messages so they will not be sent again.
      self.subscriber.acknowledge(
          request={'subscription': self.subscription_path, 'ack_ids': ack_ids})


if __name__ == '__main__':
  benchmark_runner = GCPPubSubInterface(FLAGS.project,
                                        FLAGS.pubsub_topic,
                                        FLAGS.pubsub_subscription)
  benchmark_runner.run_phase(FLAGS.benchmark_scenario, FLAGS.number_of_messages,
                             FLAGS.message_size)
