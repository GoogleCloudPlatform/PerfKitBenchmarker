"""Tests for gcp_pubsub."""

import unittest

from absl import flags
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_sqs
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

_REGION = 'us-east-1b'
_BENCHMARK_SCENARIO = 'pull_latency'
_NUMBER_OF_MESSAGES = 10
_MESSAGE_SIZE = 100
_MESSAGING_SERVICE_DATA_DIR = 'messaging_service_scripts'

FLAGS = flags.FLAGS


class AwsSqsTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(util, 'GetAccount')
  def setUp(self, _):
    super().setUp()
    FLAGS.run_uri = 'uid'
    self.client = mock.Mock()
    self.client.zone = _REGION
    self.sqs = aws_sqs.AwsSqs()
    self.sqs.client_vm = self.client

  def _MockIssueCommand(self, return_value):
    return self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=return_value))

  def testGetQueue(self):
    # Don't actually issue a command.
    return_value = ['{"QueueUrl": "mocked_queue_url"}', None, 0]
    cmd = self._MockIssueCommand(return_value)

    actual_result = self.sqs._GetQueue()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertEqual(actual_result, 'mocked_queue_url')

  def testCreate(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.sqs._Create()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs create-queue --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)

  def testExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    result = self.sqs._Exists()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertTrue(result)

  def testDoesntExist(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    result = self.sqs._Exists()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertFalse(result)

  @mock.patch.object(aws_sqs.AwsSqs, '_GetQueue')
  def testDelete(self, get_queue_mock):
    get_queue_mock.return_value = 'queue_url'
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.sqs._Delete()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs delete-queue --queue-url ' + get_queue_mock.return_value +
        ' --region ' + self.sqs.region, cmd)

  def testPrepareClientVm(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    self.sqs.PrepareClientVm()
    self.client.assert_has_calls([
        mock.call.RemoteCommand(
            'sudo pip3 install boto3', ignore_failure=False),
        mock.call.RemoteCommand(
            'mkdir -p ~/perfkitbenchmarker/scripts/messaging_service_scripts/aws'
        ),
        mock.call.PushDataFile(
            'messaging_service_scripts/aws/__init__.py',
            '~/perfkitbenchmarker/scripts/messaging_service_scripts/aws/__init__.py'
        ),
        mock.call.RemoteCommand(
            'mkdir -p ~/perfkitbenchmarker/scripts/messaging_service_scripts/aws'
        ),
        mock.call.PushDataFile(
            'messaging_service_scripts/aws/aws_sqs_client.py',
            '~/perfkitbenchmarker/scripts/messaging_service_scripts/aws/aws_sqs_client.py'
        ),
        mock.call.PushDataFile('messaging_service_scripts/aws_benchmark.py'),
    ])
    self.client.Install.assert_called_with('aws_credentials')

  def testRun(self):
    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (f'python3 -m aws_benchmark '
                      f'--queue_name={self.sqs.queue_name} '
                      f'--region={self.sqs.region} '
                      f'--benchmark_scenario={_BENCHMARK_SCENARIO} '
                      f'--number_of_messages={_NUMBER_OF_MESSAGES} '
                      f'--message_size={_MESSAGE_SIZE}')

    self.sqs.Run(_BENCHMARK_SCENARIO, _NUMBER_OF_MESSAGES, _MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)


if __name__ == '__main__':
  unittest.main()
