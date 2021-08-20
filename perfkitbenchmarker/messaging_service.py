"""Common interface for messaging services resources.

MessagingService class offers a common interface to provision resources, and to
run different phases of the benchmark [Prepare, Run, Cleanup]. The messaging
service benchmark uses the specific instance (from
messaging_service_util.py file) to run the phases of the benchmark on it.
Prepare and Cleanup phases runs from the benchmark VM, on the run phase the
benchmark VM send commands to the Client VM. Client VM's implementations that
runs the benchmark can be found on: /data/messaging_service.
"""

import abc
import os
from typing import Any, Dict
from perfkitbenchmarker import virtual_machine

MESSAGING_SERVICE_DATA_DIR = 'messaging_service'
TIMEOUT = 15  # 15 seconds from now
SLEEP_TIME = 2


class MessagingService:
  """Common interface of a messaging service resource.

  Attributes:
    client: The client virtual machine that runs the benchmark.
  """

  def __init__(self, client: virtual_machine.BaseVirtualMachine):
    """Initializes the instance with client VM."""
    self.client = client

  @abc.abstractmethod
  def prepare(self):
    """Prepares client VM and resources.

    This method takes care of preparing the client VM - installing common needed
    packages and uploading files. This method should be overwritten in
    child implementations to install/upload specific packages/files, and to
    create resources - on GCP it creates a Cloud PubSub topic and subscription.
    """
    # Install commom packages
    self.client.Install('python3')
    self.client.Install('pip3')
    self.client.RemoteCommand('sudo pip3 install absl-py numpy')

    # Upload Common Client Interface
    self.client.PushDataFile(os.path.join(
        MESSAGING_SERVICE_DATA_DIR,
        'messaging_service_client.py'))

  @abc.abstractmethod
  def run(self, benchmark_scenario: str, number_of_messages: str,
          message_size: str) -> Dict[str, Any]:
    """Runs remote commands on client VM - benchmark's run phase.

    Runs a benchmark that consists of first publishing messages and then
    pulling messages from messaging service, based on the configuration
    specified through the FLAGS: benchmark_scenario, number_of_messages, and
    message_size. Specific implementations should override this method.
    Different providers needs different info to run the benchmark - for GCP we
    need 'topic_name' and 'subscription_name', while for AWS 'queue_name'
    suffices.

    Args:
      benchmark_scenario: Specifies which benchmark scenario to run.
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the
        benchmark. It specifies the number of characters in those messages.

    Returns:
      Dictionary with metric_name (mean_latency, p50_latency...) as key and the
      results from the benchmark as the value:
        results = {
          'mean_latency': 0.3423443...
          ...
        }
    """

  @abc.abstractmethod
  def cleanup(self):
    """Delete resources that were created as part of the benchmark.

    This method takes care of cleaning resources that were created on the
    'self.Prepare' method. Specific implementations should
    override this method - on GCP it takes care of deleting the
    Cloud PubSub topic and subscription that were created.
    """
