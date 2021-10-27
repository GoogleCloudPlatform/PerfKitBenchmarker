"""AWS SQS interface for resources.

This class handles resource creation/cleanup for SQS benchmark on AWS.
"""

import json
import os

from absl import flags
from perfkitbenchmarker import messaging_service as msgsvc
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import aws
from perfkitbenchmarker.providers.aws import util


FLAGS = flags.FLAGS
AWS_SQS_CLIENT_PY = 'aws_sqs_client.py'


class AwsSqs(msgsvc.BaseMessagingService):
  """AWS SQS Interface Class."""

  CLOUD = aws.CLOUD

  def __init__(self):
    super().__init__()
    self.queue_name = 'pkb-queue-{0}'.format(FLAGS.run_uri)

  def _Create(self):
    """Handles AWS resources provision.

    It creates an AWS SQS queue.
    """
    cmd = util.AWS_PREFIX + [
        'sqs',
        'create-queue',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self) -> bool:
    """Checks whether SQS queue already exists."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'get-queue-url',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    return retcode == 0

  def _Delete(self):
    """Handle SQS queue deletion."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'delete-queue',
        '--queue-url', self._GetQueue(),
        '--region', self.region
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsDeleting(self):
    """Overrides BaseResource._IsDeleting.

    Used internally while deleting to check if the deletion is still in
    progress.

    Returns:
      A bool. True if the resource is not yet deleted, else False.
    """
    return self._Exists()

  def _InstallCloudClients(self):
    self.client_vm.RemoteCommand(
        'sudo pip3 install boto3',
        ignore_failure=False)
    self.client_vm.PushDataFile(os.path.join(
        msgsvc.MESSAGING_SERVICE_DATA_DIR,
        AWS_SQS_CLIENT_PY))
    # copy AWS creds
    self.client_vm.Install('aws_credentials')

  def Run(self,
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
    stdout, _ = self.client_vm.RemoteCommand(command)
    results = json.loads(stdout)
    return results

  @property
  def region(self):
    return util.GetRegionFromZone(self.client_vm.zone)

  def _GetQueue(self) -> str:
    """Get SQS queue URL from AWS."""
    cmd = util.AWS_PREFIX + [
        'sqs',
        'get-queue-url',
        '--queue-name', self.queue_name,
        '--region', self.region
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(stdout)['QueueUrl']
