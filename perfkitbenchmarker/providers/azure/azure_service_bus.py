"""Azure Service Bus interface for resources.

This class handles resource creation/cleanup for messaging service benchmark
on Azure Service Bus.
https://docs.microsoft.com/en-us/azure/service-bus-messaging/
"""

import json
import logging
import os
import time
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.messaging_service import MessagingService
from perfkitbenchmarker.messaging_service import SLEEP_TIME
from perfkitbenchmarker.messaging_service import TIMEOUT
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

MESSAGING_SERVICE_DATA_DIR = 'messaging_service'

FLAGS = flags.FLAGS


class AzureServiceBus(MessagingService):
  """Azure Service Bus Interface Class."""

  def __init__(self,
               client: virtual_machine.BaseVirtualMachine):
    super().__init__(client)
    self.location = client.zone
    self.topic_name = 'pkb-topic-{0}'.format(FLAGS.run_uri)
    self.subscription_name = 'pkb-subscription-{0}'.format(FLAGS.run_uri)
    self.namespace_name = 'pkb-namespace-{0}'.format(FLAGS.run_uri)
    self.resource_group = azure_network.GetResourceGroup()

  def _create_topic(self):
    """Creates Service Bus topic."""
    cmd = [
        azure.AZURE_PATH,
        'servicebus',
        'topic',
        'create',
        '--name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _topic_exists(self) -> bool:
    """Checks whether Service Bus topic already exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'show', '--name',
        self.topic_name,
        '--namespace-name', self.namespace_name
    ]+ self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_topic(self):
    """Handle Service Bus topic deletion."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'delete', '--name',
        self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _create_subscription(self):
    """Creates Service Bus subscription."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'create',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _subscription_exists(self) -> bool:
    """Checks whether Service Bus subscription already exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'show',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_subscription(self):
    """Handle Service Bus subscription deletion."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'topic', 'subscription', 'delete',
        '--name', self.subscription_name, '--topic-name', self.topic_name,
        '--namespace-name', self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _create_namespace(self):
    """Creates an Azure Service Bus Namespace."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'create', '--name',
        self.namespace_name, '--location', self.location
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _namespace_exists(self) -> bool:
    """Checks if our Service Bus Namespace exists."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'show', '--name',
        self.namespace_name
    ] + self.resource_group.args
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    return True

  def _delete_namespace(self):
    """Deletes the Azure Service Bus namespace."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'delete', '--name',
        self.namespace_name
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _get_primary_connection_string(self):
    """Gets Azure Service Bus Namespace connection string."""
    cmd = [
        azure.AZURE_PATH, 'servicebus', 'namespace', 'authorization-rule',
        'keys', 'list', '--name=RootManageSharedAccessKey', '--namespace-name',
        self.namespace_name, '--query=primaryConnectionString', '-o=tsv'
    ] + self.resource_group.args
    output, stderror, retcode = vm_util.IssueCommand(
        cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warning(
          'Failed to get Service Bus Namespace connection string! %s', stderror)
    return output.strip()

  def create_resource(self, create_function, exists_function):
    create_function()
    timeout = time.time() + TIMEOUT

    while not exists_function():
      if time.time() > timeout:
        raise errors.Benchmarks.PrepareException(
            'Timeout when creating resource.')
      time.sleep(SLEEP_TIME)

  def provision_resources(self):
    """Handles provision of resources needed for Azure Service Bus benchmark."""

    self.create_resource(self._create_namespace, self._namespace_exists)
    self.create_resource(self._create_topic, self._topic_exists)
    self.create_resource(self._create_subscription, self._subscription_exists)

  def prepare(self):
    # Install/uploads common modules/files
    super().prepare()

    # Install/uploads Azure specific modules/files.
    self.client.RemoteCommand(
        'sudo pip3 install azure-servicebus',
        ignore_failure=False)
    self.client.PushDataFile(os.path.join(
        MESSAGING_SERVICE_DATA_DIR,
        'azure_service_bus_client.py'))

    # Create resources on Azure
    self.provision_resources()

  def run(self, benchmark_scenario: str, number_of_messages: str,
          message_size: str) -> Dict[str, Any]:
    connection_str = self._get_primary_connection_string()
    command = (f'python3 -m azure_service_bus_client '
               f'--topic_name={self.topic_name} '
               f'--subscription_name={self.subscription_name} '
               f'--benchmark_scenario={benchmark_scenario} '
               f'--number_of_messages={number_of_messages} '
               f'--message_size={message_size} '
               f'--connection_str="{connection_str}" ')
    results = self.client.RemoteCommand(command)
    results = json.loads(results[0])
    return results

  def delete_resource(self, delete_function, exists_function):
    delete_function()
    timeout = time.time() + TIMEOUT

    while exists_function():
      if time.time() > timeout:
        raise errors.Resource.CleanupError(
            'Timeout when deleting resource.')
      time.sleep(SLEEP_TIME)

  def cleanup(self):
    self.delete_resource(self._delete_subscription, self._subscription_exists)
    self.delete_resource(self._delete_topic, self._topic_exists)
    self.delete_resource(self._delete_namespace, self._namespace_exists)
