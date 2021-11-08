"""Main script for AWS messaging service benchmark."""

from absl import app as absl_app

from perfkitbenchmarker.scripts.messaging_service_scripts.aws import aws_sqs_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import app

if __name__ == '__main__':
  absl_app.run(app.App.for_client(aws_sqs_client.AwsSqsClient))
