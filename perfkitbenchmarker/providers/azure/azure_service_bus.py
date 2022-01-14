"""Azure Service Bus interface for resources.

This class handles resource creation/cleanup for messaging service benchmark
on Azure Service Bus.
https://docs.microsoft.com/en-us/azure/service-bus-messaging/
"""

import json
import logging
import os
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import messaging_service as msgsvc
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

FLAGS = flags.FLAGS
MESSAGING_SERVICE_SCRIPTS_VM_AZURE_DIR = os.path.join(
    msgsvc.MESSAGING_SERVICE_SCRIPTS_VM_LIB_DIR, 'azure')
MESSAGING_SERVICE_SCRIPTS_AZURE_PREFIX = 'messaging_service_scripts/azure'
MESSAGING_SERVICE_SCRIPTS_AZURE_FILES = [
    '__init__.py', 'azure_service_bus_client.py'
]
MESSAGING_SERVICE_SCRIPTS_AZURE_BIN = 'messaging_service_scripts/azure_benchmark.py'


class AzureServiceBus(msgsvc.BaseMessagingService):
  """Azure Service Bus Interface Class."""

  CLOUD = providers.AZURE

  def __init__(self):
    super().__init__()
    self.topic_name = 'pkb-topic-{0}'.format(FLAGS.run_uri)
    self.subscription_name = 'pkb-subscription-{0}'.format(FLAGS.run_uri)
    self.namespace_name = 'pkb-namespace-{0}'.format(FLAGS.run_uri)
    self.resource_group = azure_network.GetResourceGroup()

  def _Create(self):
    """Handles provision of resources needed for Azure Service Bus benchmark."""
    self._CreateNamespace()
    self._CreateTopic()
    self._CreateSubscription()

  def _Exists(self):
    return (self._NamespaceExists() and self._TopicExists() and
            self._SubscriptionExists())

  def _Delete(self):
    self._DeleteSubscription()
    self._DeleteTopic()
    self._DeleteNamespace()

  def _IsDeleting(self):
    """Overrides BaseResource._IsDeleting.

    Used internally while deleting to check if the deletion is still in
    progress.

    Returns:
      A bool. True if the resource is not yet deleted, else False.
    """
    return (self._NamespaceExists() or self._TopicExists() or
            self._SubscriptionExists())

  def Run(self, benchmark_scenario: str, number_of_messages: str,
          message_size: str) -> Dict[str, Any]:
    connection_str = self._GetPrimaryConnectionString()
    command = (f'python3 -m azure_benchmark '
               f'--topic_name={self.topic_name} '
               f'--subscription_name={self.subscription_name} '
               f'--benchmark_scenario={benchmark_scenario} '
               f'--number_of_messages={number_of_messages} '
               f'--message_size={message_size} '
               f'--connection_str="{connection_str}" ')
    results = self.client_vm.RemoteCommand(command)
    results = json.loads(results[0])
    return results

  def _InstallCloudClients(self):
    # Install/uploads Azure specific modules/files.
    self.client_vm.RemoteCommand(
        'sudo pip3 install azure-servicebus', ignore_failure=False)

    self._CopyFiles(MESSAGING_SERVICE_SCRIPTS_AZURE_PREFIX,
                    MESSAGING_SERVICE_SCRIPTS_AZURE_FILES,
                    MESSAGING_SERVICE_SCRIPTS_VM_AZURE_DIR)
    self.client_vm.PushDataFile(MESSAGING_SERVICE_SCRIPTS_AZURE_BIN)

  @property
  def location(self):
    return self.client_vm.zone

  def _CreateTopic(self):
    """Creates Service Bus topic."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'create', '--name',
        self.topic_name, '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _TopicExists(self) -> bool:
    """Checks whether Service Bus topic already exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'show', '--name',
        self.topic_name, '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    return retcode == 0

  def _DeleteTopic(self):
    """Handle Service Bus topic deletion."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'delete', '--name',
        self.topic_name, '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _CreateSubscription(self):
    """Creates Service Bus subscription."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'create',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _SubscriptionExists(self) -> bool:
    """Checks whether Service Bus subscription already exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'show',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    return retcode == 0

  def _DeleteSubscription(self):
    """Handle Service Bus subscription deletion."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'delete',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _CreateNamespace(self):
    """Creates an Azure Service Bus Namespace."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'create', '--name',
        self.namespace_name, '--location', self.location
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _NamespaceExists(self) -> bool:
    """Checks if our Service Bus Namespace exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'show', '--name',
        self.namespace_name
    ] + self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    return retcode == 0

  def _DeleteNamespace(self):
    """Deletes the Azure Service Bus namespace."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'delete', '--name',
        self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _GetPrimaryConnectionString(self):
    """Gets Azure Service Bus Namespace connection string."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'authorization-rule',
        'keys', 'list', '--name=RootManageSharedAccessKey', '--namespace-name',
        self.namespace_name, '--query=primaryConnectionString', '-o=tsv'
    ] + self.resource_group.args
    output, stderror, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False)
    if retcode:
      logging.warning(
          'Failed to get Service Bus Namespace connection string! %s', stderror)
    return output.strip()
