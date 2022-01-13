"""GCP PubSub interface for resources.

This class handles resource creation/cleanup for messaging service benchmark
on GCP Cloud PubSub. https://cloud.google.com/pubsub/docs
"""

import json
import logging
import os
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import messaging_service as msgsvc
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
MESSAGING_SERVICE_SCRIPTS_VM_GCP_DIR = os.path.join(
    msgsvc.MESSAGING_SERVICE_SCRIPTS_VM_LIB_DIR, 'gcp')
MESSAGING_SERVICE_SCRIPTS_GCP_PREFIX = 'messaging_service_scripts/gcp'
MESSAGING_SERVICE_SCRIPTS_GCP_FILES = ['__init__.py', 'gcp_pubsub_client.py']
MESSAGING_SERVICE_SCRIPTS_GCP_BIN = 'messaging_service_scripts/gcp_benchmark.py'


class GCPCloudPubSub(msgsvc.BaseMessagingService):
  """GCP Cloud PubSub Interface Class for prepare phase.

  This class has methods that allow us to run the provision/prepare and cleanup
  phase for GCP from the benchmark VM. The provision/prepare phase involve
  things like: installing specific packages on the client VM, uploading files
  to client VM, resource creation on the cloud provider (PubSub needs a topic
  and subcription).
  """

  CLOUD = providers.GCP

  def __init__(self):
    super().__init__()
    self.project = FLAGS.project
    self.pubsub_topic = 'pkb-topic-{0}'.format(FLAGS.run_uri)
    self.pubsub_subscription = 'pkb-subscription-{0}'.format(FLAGS.run_uri)

  def _Create(self):
    """Handles provision of resources needed for GCP Pub/Sub benchmark."""
    self._CreateTopic()
    self._CreateSubscription()

  def _Exists(self):
    return self._TopicExists() and self._SubscriptionExists()

  def _Delete(self):
    self._DeleteSubscription()
    self._DeleteTopic()

  def _IsDeleting(self):
    """Overrides BaseResource._IsDeleting.

    Used internally while deleting to check if the deletion is still in
    progress.

    Returns:
      A bool. True if the resource is not yet deleted, else False.
    """
    return self._SubscriptionExists() or self._TopicExists()

  def _InstallCloudClients(self):
    # Install/uploads GCP specific modules/files.
    self.client_vm.RemoteCommand(
        'sudo pip3 install --upgrade --ignore-installed google-cloud-pubsub',
        ignore_failure=False)

    self._CopyFiles(
        MESSAGING_SERVICE_SCRIPTS_GCP_PREFIX,
        MESSAGING_SERVICE_SCRIPTS_GCP_FILES,
        MESSAGING_SERVICE_SCRIPTS_VM_GCP_DIR)
    self.client_vm.PushDataFile(MESSAGING_SERVICE_SCRIPTS_GCP_BIN)

  def Run(self, benchmark_scenario: str, number_of_messages: str,
          message_size: str) -> Dict[str, Any]:
    """Runs a benchmark on GCP PubSub from the client VM.

    Runs a benchmark based on the configuration specified through the arguments:
    benchmark_scenario, number_of_messages, and message_size. This contains
    the GCP specific command that we need to run on the client VM to run the
    benchmark.

    Args:
      benchmark_scenario: Specifies which benchmark scenario to run.
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the benchmark. It
        specifies the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    command = (f'python3 -m gcp_benchmark '
               f'--pubsub_project={self.project} '
               f'--pubsub_topic={self.pubsub_topic} '
               f'--pubsub_subscription={self.pubsub_subscription} '
               f'--benchmark_scenario={benchmark_scenario} '
               f'--number_of_messages={number_of_messages} '
               f'--message_size={message_size} ')
    stdout, _ = self.client_vm.RemoteCommand(command)
    metrics = json.loads(stdout)
    return metrics

  def _CreateTopic(self):
    """Handles topic creation on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'create',
                             self.pubsub_topic)
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Creation of GCP PubSub topic failed.')
      raise errors.Resource.CreationError(
          'Failed to create PubSub Topic: %s return code: %s' %
          (retcode, stderr))

  def _TopicExists(self) -> bool:
    """Check if subscription exists on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'describe',
                             self.pubsub_topic)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return retcode == 0

  def _DeleteTopic(self):
    """Handles topic deletion on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'topics', 'delete',
                             self.pubsub_topic)
    cmd.Issue(raise_on_failure=False)

  def _CreateSubscription(self):
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

  def _SubscriptionExists(self) -> bool:
    """Check if subscription exists on GCP Pub/Sub.."""
    cmd = util.GcloudCommand(self, 'pubsub', 'subscriptions', 'describe',
                             self.pubsub_subscription)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return retcode == 0

  def _DeleteSubscription(self):
    """Handles subscription deletion on GCP Pub/Sub."""
    cmd = util.GcloudCommand(self, 'pubsub', 'subscriptions', 'delete',
                             self.pubsub_subscription)
    cmd.Issue(raise_on_failure=False)
