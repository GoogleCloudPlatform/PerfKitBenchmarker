"""AWS SQS interface.

This AWS SQS client interface is implemented using Boto3 - AWS SDK for Python.
Boto3 SQS Documentation:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
"""
from typing import Any, Dict, Optional

from absl import flags
import boto3

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client

FLAGS = flags.FLAGS

flags.DEFINE_string('region', 'us-west-1', help='AWS region to use.')
flags.DEFINE_string('queue_name', 'perfkit_queue', help='AWS SQS queue name.')


class AwsSqsClient(client.BaseMessagingServiceClient):
  """AWS SQS PubSub Client Class."""

  @classmethod
  def from_flags(cls):
    return cls(FLAGS.region, FLAGS.queue_name)

  def __init__(self, region_name: str, queue_name: str):
    self.region_name = region_name
    self.queue_name = queue_name
    # Get the service resource
    self.sqs_resource = boto3.resource('sqs', region_name=self.region_name)
    # Create SQS client
    self.sqs_client = boto3.client('sqs', region_name=self.region_name)
    self.queue = self.sqs_resource.get_queue_by_name(QueueName=self.queue_name)

  def publish_message(self, message: str) -> Dict[str, Any]:
    published_message = self.queue.send_message(MessageBody=message)
    return published_message

  def pull_message(
      self, timeout: float = client.TIMEOUT) -> Optional[Dict[str, Any]]:
    response = self.sqs_client.receive_message(
        QueueUrl=self.queue.url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=max(1, int(timeout)))  # WaitTimeSeconds must be an int
    messages = response.get('Messages', [])
    return messages[0] if messages else None

  def acknowledge_received_message(self, message: Dict[str, Any]):
    receipt_handle = message['ReceiptHandle']
    self.sqs_client.delete_message(
        QueueUrl=self.queue.url, ReceiptHandle=receipt_handle)

  def _get_first_six_bytes_from_payload(self, message: Dict[str, Any]) -> bytes:
    """Gets the first 6 bytes of a message (as returned by pull_message)."""
    return message['Body'][:6].encode('utf-8')
