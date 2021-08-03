"""Tests for gcp_pubsub."""

import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_sqs
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

_REGION = 'us-east-1b'
_BENCHMARK_SCENARIO = 'pull_latency'
_NUMBER_OF_MESSAGES = 10
_MESSAGE_SIZE = 100
_MESSAGING_SERVICE_DATA_DIR = 'messaging_service'

FLAGS = flags.FLAGS


class AwsSqsTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(util, 'GetAccount')
  def setUp(self, _):
    super().setUp()
    FLAGS.run_uri = 'uid'
    FLAGS.zones = [_REGION]
    self.client = mock.Mock()
    self.sqs = aws_sqs.AwsSqs(self.client)

  def _MockIssueCommand(self, return_value):
    return self.enter_context(mock.patch.object(
        vm_util, 'IssueCommand', return_value=return_value))

  def testGetQueue(self):
    # Don't actually issue a command.
    return_value = ['{"QueueUrl": "mocked_queue_url"}', None, 0]
    cmd = self._MockIssueCommand(return_value)

    actual_result = self.sqs._get_queue()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertEqual(actual_result, 'mocked_queue_url')

  def testCreateQueue(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.sqs._create_queue()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs create-queue --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)

  def testQueueExists(self):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    result = self.sqs._queue_exists()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertTrue(result)

  def testQueueDoesntExists(self):
    # Don't actually issue a command.
    return_value = ['', '', 1]
    cmd = self._MockIssueCommand(return_value)

    result = self.sqs._queue_exists()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs get-queue-url --queue-name ' + self.sqs.queue_name + ' --region ' +
        self.sqs.region, cmd)
    self.assertFalse(result)

  @mock.patch.object(aws_sqs.AwsSqs, '_get_queue')
  def testDeleteQueue(self, get_queue_mock):
    get_queue_mock.return_value = 'queue_url'
    # Don't actually issue a command.
    return_value = [None, None, 0]
    cmd = self._MockIssueCommand(return_value)

    self.sqs._delete_queue()
    cmd = ' '.join(cmd.call_args[0][0])
    self.assertIn(
        'sqs delete-queue --queue-url ' + get_queue_mock.return_value +
        ' --region ' + self.sqs.region, cmd)

  @mock.patch.object(aws_sqs.AwsSqs, '_create_queue')
  @mock.patch.object(aws_sqs.AwsSqs, '_queue_exists', side_effect=[False, True])
  def testProvisionResources(
      self, queue_exists_mock, create_queue_mock):
    self.sqs.provision_resources()
    self.assertEqual(create_queue_mock.call_count, 1)
    self.assertEqual(queue_exists_mock.call_count, 2)

  @mock.patch.object(aws_sqs.AwsSqs, '_create_queue')
  @mock.patch.object(aws_sqs.AwsSqs, '_queue_exists', return_value=False)
  def testProvisionResourcesException(self, queue_exists_mock,
                                      create_queue_mock):
    self.assertRaises(errors.Benchmarks.PrepareException,
                      self.sqs.provision_resources)

  @mock.patch.object(aws_sqs.AwsSqs, 'provision_resources')
  def testPrepare(self, provision_mock):
    # Don't actually issue a command.
    return_value = [None, None, 0]
    self._MockIssueCommand(return_value)

    sdk_cmd = ('sudo pip3 install boto3')
    datafile_path = os.path.join(_MESSAGING_SERVICE_DATA_DIR,
                                 'aws_sqs_client.py')

    self.sqs.prepare()
    self.client.RemoteCommand.assert_called_with(sdk_cmd, ignore_failure=False)
    self.client.PushDataFile.assert_called_with(datafile_path)
    self.client.Install.assert_called_with('aws_credentials')
    provision_mock.assert_called()

  @mock.patch.object(aws_sqs.AwsSqs, 'provision_resources')
  def testRun(self, provision_mock):
    return_value = ['{"mock1": 1}', None]
    self.client.RemoteCommand.return_value = return_value
    remote_run_cmd = (f'python3 -m aws_sqs_client '
                      f'--queue_name={self.sqs.queue_name} '
                      f'--region={self.sqs.region} '
                      f'--benchmark_scenario={_BENCHMARK_SCENARIO} '
                      f'--number_of_messages={_NUMBER_OF_MESSAGES} '
                      f'--message_size={_MESSAGE_SIZE}')

    self.sqs.run(_BENCHMARK_SCENARIO, _NUMBER_OF_MESSAGES, _MESSAGE_SIZE)
    self.client.RemoteCommand.assert_called_with(remote_run_cmd)

  @mock.patch.object(aws_sqs.AwsSqs, '_delete_queue')
  @mock.patch.object(aws_sqs.AwsSqs, '_queue_exists', side_effect=[True, False])
  def testCleanup(self, queue_exists_mock, delete_queue_mock):
    self.sqs.cleanup()
    self.assertEqual(delete_queue_mock.call_count, 1)
    self.assertEqual(queue_exists_mock.call_count, 2)

  @mock.patch.object(aws_sqs.AwsSqs, '_delete_queue')
  @mock.patch.object(aws_sqs.AwsSqs, '_queue_exists', return_value=True)
  def testCleanupException(self, queue_exists_mock, delete_queue_mock):
    self.assertRaises(errors.Resource.CleanupError,
                      self.sqs.cleanup)

if __name__ == '__main__':
  unittest.main()
