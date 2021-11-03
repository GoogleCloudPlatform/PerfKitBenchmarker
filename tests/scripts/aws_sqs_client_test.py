"""Tests for data/messaging_service/aws_sqs_client.py."""

import unittest
from unittest import mock

from perfkitbenchmarker.scripts.messaging_service_scripts.aws_sqs_client import AWSSQSInterface

NUMBER_OF_MESSAGES = 1
MESSAGE_SIZE = 10
REGION_NAME = 'pkb_test_region'
QUEUE_NAME = 'pkb_test_queue'
TIMEOUT = 10


@mock.patch('boto3.resource')
@mock.patch('boto3.client')
class AWSSQSClientTest(unittest.TestCase):

  def testPublishMessage(self, _, resource_mock):
    message = 'test_message'

    aws_interface = AWSSQSInterface(REGION_NAME, QUEUE_NAME)
    aws_interface._publish_message(message)

    # assert publish was called
    resource_mock.return_value.get_queue_by_name(
    ).send_message.assert_called_with(MessageBody=message)

  def testPullMessage(self, client_mock, resource_mock):
    client_mock.return_value.receive_message.return_value = {
        'Messages': [{
            'ReceiptHandle': 'MockedReceipt'
        }]
    }
    aws_interface = AWSSQSInterface(REGION_NAME, QUEUE_NAME)
    aws_interface._pull_message()
    queue_url = resource_mock.return_value.get_queue_by_name().url

    # assert pull was called
    client_mock.return_value.receive_message.assert_called_with(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=TIMEOUT)

  def testAcknowledgeReceivedMessage(self, client_mock, resource_mock):
    response = {
        'Messages': [{
            'ReceiptHandle': 'MockedReceipt'
        }]
    }

    aws_interface = AWSSQSInterface(REGION_NAME, QUEUE_NAME)
    aws_interface._acknowledge_received_message(response)
    queue_url = resource_mock.return_value.get_queue_by_name().url

    # assert acknowledge was called
    client_mock.return_value.delete_message.assert_called_with(
        QueueUrl=queue_url, ReceiptHandle='MockedReceipt')

if __name__ == '__main__':
  unittest.main()
