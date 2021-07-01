"""GCP PubSub interface for resources.

This class handles resource creation/cleanup for messaging service benchmark
on GCP Cloud PubSub. https://cloud.google.com/pubsub/docs
"""

import json
import logging
import os
import time
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.messaging_service import MESSAGING_SERVICE_DATA_DIR
from perfkitbenchmarker.messaging_service import MessagingService
from perfkitbenchmarker.messaging_service import SLEEP_TIME
from perfkitbenchmarker.messaging_service import TIMEOUT
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GCPCloudPubSub(MessagingService):
  """GCP Cloud PubSub Interface Class for prepare phase.

  This class has methods that allow us to run the provision/prepare and cleanup
  phase for GCP from the benchmark VM. The provision/prepare phase involve
  things like: installing specific packages on the client VM, uploading files
  to client VM, resource creation on the cloud provider (PubSub needs a topic
  and subcription).
  """

  def __init__(self,
               client: virtual_machine.BaseVirtualMachine):
    super().__init__(client)
    self.project = FLAGS.project
    self.pubsub_topic = 'pkb-topic-{0}'.format(FLAGS.run_uri)
    self.pubsub_subscription = 'pkb-subscription-{0}'.format(FLAGS.run_uri)

  def _create_topic(self):
    """Handles topic creation on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'create',
                             self.pubsub_topic)
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Creation of GCP PubSub topic failed.')
      raise errors.Resource.CreationError(
          'Failed to create PubSub Topic: %s return code: %s' %
          (retcode, stderr))

  def _topic_exists(self) -> bool:
    """Check if subscription exists on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'describe',
                             self.pubsub_topic)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_topic(self):
    """Handles topic deletion on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'delete',
                             self.pubsub_topic)
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Deletion of GCP PubSub topic failed.')
      raise errors.Resource.RetryableDeletionError(
          'Failed to delete PubSub Topic: %s return code: %s' %
          (retcode, stderr))

  def _create_subscription(self):
    """Handles Subscription creation on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'subscriptions', 'create',
                             self.pubsub_subscription)
    cmd.flags['topic'] = self.pubsub_topic
    cmd.flags['topic-project'] = self.project
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Creation of GCP PubSub subscription failed.')
      raise errors.Resource.CreationError(
          'Failed to create PubSub Subscription: %s return code: %s' %
          (retcode, stderr))

  def _subscription_exists(self) -> bool:
    """Check if subscription exists on GCP Pub/Sub.."""
    cmd = util.GcloudCommand(self, 'pubsub', 'subscriptions', 'describe',
                             self.pubsub_subscription)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_subscription(self):
    """Handles subscription deletion on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'subscriptions', 'delete',
                             self.pubsub_subscription)
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Deletion of GCP PubSub subscription failed.')
      raise errors.Resource.RetryableDeletionError(
          'Failed to delete PubSub Subscription: %s return code: %s' %
          (retcode, stderr))

  def provision_resources(self):
    """Handles provision of resources needed for GCP Pub/Sub benchmark."""
    self._create_topic()
    self._create_subscription()
    timeout = time.time() + TIMEOUT

    while not self._topic_exists() or not self._subscription_exists():
      if time.time() > timeout:
        raise errors.Benchmarks.PrepareException(
            'Timeout when creating PubSub topic or subscription.')
      time.sleep(SLEEP_TIME)

  def prepare(self):
    # Install/uploads common modules/files
    super().prepare()

    # Install/uploads GCP specific modules/files.
    self.client.RemoteCommand(
        'sudo pip3 install --upgrade --ignore-installed google-cloud-pubsub',
        ignore_failure=False)
    self.client.PushDataFile(os.path.join(
        MESSAGING_SERVICE_DATA_DIR,
        'gcp_pubsub_client.py'))

    # Create resources on GCP
    self.provision_resources()

  def run(self, benchmark_scenario: str, number_of_messages: str,
          message_size: str) -> Dict[str, Any]:
    """Runs a benchmark on GCP PubSub from the client VM.

    Runs a benchmark based on the configuration specified through the arguments:
    benchmark_scenario, number_of_messages, and message_size. This contains
    the GCP specific command that we need to run on the client VM to run the
    benchmark.

    Args:
      benchmark_scenario: Specifies which benchmark scenario to run.
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the
        benchmark. It specifies the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    command = (f'python3 -m gcp_pubsub_client '
               f'--project={self.project} '
               f'--pubsub_topic={self.pubsub_topic} '
               f'--pubsub_subscription={self.pubsub_subscription} '
               f'--benchmark_scenario={benchmark_scenario} '
               f'--number_of_messages={number_of_messages} '
               f'--message_size={message_size} ')
    stdout, _ = self.client.RemoteCommand(command)
    metrics = json.loads(stdout)
    return metrics

  def cleanup(self):
    timeout = time.time() + TIMEOUT
    self._delete_subscription()
    while self._subscription_exists():
      if time.time() > timeout:
        raise errors.Resource.CleanupError(
            'Timeout when deleting GCP PubSub subscription.')
      time.sleep(SLEEP_TIME)

    timeout = time.time() + TIMEOUT
    self._delete_topic()
    while self._topic_exists():
      if time.time() > timeout:
        raise errors.Resource.CleanupError(
            'Timeout when deleting GCP PubSub topic.')
      time.sleep(SLEEP_TIME)
