"""AWS SQS interface for resources.

This class handles resource creation/cleanup for SQS benchmark on AWS.
"""

import json
import os
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.messaging_service import MESSAGING_SERVICE_DATA_DIR
from perfkitbenchmarker.messaging_service import MessagingService
from perfkitbenchmarker.messaging_service import SLEEP_TIME
from perfkitbenchmarker.messaging_service import TIMEOUT
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util


MESSAGING_SERVICE_DATA_DIR = 'messaging_service'

FLAGS = flags.FLAGS


class AwsSqs(MessagingService):
  """AWS SQS Interface Class."""

  def __init__(self, client: aws_virtual_machine.AwsVirtualMachine):
    super().__init__(client)
    self.region = util.GetRegionFromZone(FLAGS.zones[0])
    self.queue_name = 'pkb-queue-{0}'.format(FLAGS.run_uri)

  def _get_queue(self) -> str:
    """Get SQS queue URL from AWS."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'get-queue-url',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(stdout)['QueueUrl']

  def _create_queue(self):
    cmd = util.AWS_PREFIX + [
        'sqs',
        'create-queue',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    vm_util.IssueCommand(cmd)

  def _queue_exists(self) -> bool:
    """Checks whether SQS queue already exists."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'get-queue-url',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_queue(self):
    """Handle SQS queue deletion."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'delete-queue',
        '--queue-url', self._get_queue(),
        '--region', self.region
    ]
    vm_util.IssueCommand(cmd)

  def provision_resources(self):
    """Handles AWS resources provision.

    It creates an AWS SQS queue.
    """
    self._create_queue()
    timeout = time.time() + TIMEOUT

    while not self._queue_exists():
      if time.time() > timeout:
        raise errors.Benchmarks.PrepareException(
            'Timeout when creating resource.')
      time.sleep(SLEEP_TIME)

  def prepare(self):
    """Handle the benchmark's prepare phase - done from benchmark VM."""
    # Install/uploads common modules/files
    super().prepare()

    self.client.RemoteCommand(
        'sudo pip3 install boto3',
        ignore_failure=False)
    self.client.PushDataFile(os.path.join(
        MESSAGING_SERVICE_DATA_DIR,
        'aws_sqs_client.py'))

    # copy AWS creds
    self.client.Install('aws_credentials')
    # provision resources needed on AWS
    self.provision_resources()

  def run(self,
          benchmark_scenario: str,
          number_of_messages: str,
          message_size: str):
    """Runs remote commands on client VM - benchmark's run phase."""
    command = (f'python3 -m aws_sqs_client '
               f'--queue_name={self.queue_name} '
               f'--region={self.region} '
               f'--benchmark_scenario={benchmark_scenario} '
               f'--number_of_messages={number_of_messages} '
               f'--message_size={message_size}')
    stdout, _ = self.client.RemoteCommand(command)
    results = json.loads(stdout)
    return results

  def cleanup(self):
    """Handles AWS resources cleanup.

    It cleans up all the resources that were created on 'provision_resources'.
    """
    self._delete_queue()
    timeout = time.time() + TIMEOUT

    while self._queue_exists():
      if time.time() > timeout:
        raise errors.Resource.CleanupError(
            'Timeout when deleting SQS.')
      time.sleep(SLEEP_TIME)
