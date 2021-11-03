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
from perfkitbenchmarker import resource

MESSAGING_SERVICE_DATA_DIR = 'messaging_service_scripts'
MESSAGING_SERVICE_CLIENT_PY = 'messaging_service_client.py'


def GetMessagingServiceClass(cloud, delivery):
  """Gets the underlying Messaging Service class."""
  return resource.GetResourceClass(BaseMessagingService, CLOUD=cloud,
                                   DELIVERY=delivery)


class BaseMessagingService(resource.BaseResource):
  """Common interface of a messaging service resource.

  Attributes:
    client: The client virtual machine that runs the benchmark.
  """

  REQUIRED_ATTRS = ['CLOUD', 'DELIVERY']
  RESOURCE_TYPE = 'BaseMessagingService'

  # TODO(odiego): Move DELIVERY down to child classes when adding more options
  DELIVERY = 'pull'

  @classmethod
  def FromSpec(cls, messaging_service_spec):
    return cls()

  def setVms(self, vm_groups):
    self.client_vm = vm_groups['clients' if 'clients' in
                               vm_groups else 'default'][0]

  def PrepareClientVm(self):
    self._InstallCommonClientPackages()
    self._InstallCloudClients()

  def _InstallCommonClientPackages(self):
    """Installs common software for running benchmarks on the client VM."""
    # Install commom packages
    self.client_vm.Install('python3')
    self.client_vm.Install('pip3')
    self.client_vm.RemoteCommand('sudo pip3 install absl-py numpy')

    # Upload Common Client Interface
    self.client_vm.PushDataFile(
        os.path.join(MESSAGING_SERVICE_DATA_DIR, MESSAGING_SERVICE_CLIENT_PY))

  @abc.abstractmethod
  def _InstallCloudClients(self):
    """Installs software for running benchmarks on the client VM.

    This method should be overriden by subclasses to install software specific
    to the flavor of MessagingService they provide.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def Run(self, benchmark_scenario: str, number_of_messages: str,
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
      message_size: Size of the messages that will be used on the benchmark. It
        specifies the number of characters in those messages.

    Returns:
      Dictionary with metric_name (mean_latency, p50_latency...) as key and the
      results from the benchmark as the value:
        results = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    raise NotImplementedError
