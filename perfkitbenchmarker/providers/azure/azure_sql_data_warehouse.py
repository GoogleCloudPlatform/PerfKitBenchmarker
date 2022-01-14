# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing class for Azure's SQL data warehouse EDW service.

Clusters can be paused and unpaused.
"""

import copy
import json
import os
from typing import Dict, List, Text, Tuple

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure


FLAGS = flags.FLAGS


VALID_EXIST_STATUSES = ['Resuming', 'Online']
READY_STATUSES = ['Online']
PAUSING_STATUSES = ['Pausing']
SYNAPSE_JDBC_JAR = 'synapse-jdbc-client-1.0.jar'


def GetSqlDataWarehouseClientInterface(
    server_name: str, database: str, user: str, password: str,
    resource_group: str) -> edw_service.EdwClientInterface:
  """Builds and Returns the requested SqlDataWarehouse client Interface.

  Args:
    server_name: Name of the SqlDataWarehouse server to use.
    database: Name of the database to run queries against.
    user: SqlDataWarehouse username for authentication.
    password: SqlDataWarehouse password for authentication.
    resource_group: Azure resource group used to whitelist the VM's IP address.

  Returns:
    A concrete Client Interface object.

  Raises:
    RuntimeError: if an unsupported sqldatawarehouse_client_interface is
      requested.
  """
  if FLAGS.sqldatawarehouse_client_interface == 'CLI':
    return CliClientInterface(server_name, database, user, password,
                              resource_group)
  if FLAGS.sqldatawarehouse_client_interface == 'JDBC':
    return JdbcClientInterface(server_name, database, user, password,
                               resource_group)
  raise RuntimeError('Unknown SqlDataWarehouse Client Interface requested.')


class CliClientInterface(edw_service.EdwClientInterface):
  """Command Line Client Interface class for Azure SqlDataWarehouse.

  Uses the native SqlDataWarehouse client that ships with the Azure CLI.
  https://docs.microsoft.com/en-us/cli/azure/sql/server?view=azure-cli-latest

  Attributes:
    server_name: Name of the SqlDataWarehouse server to use.
    database: Name of the database to run queries against.
    user: Redshift username for authentication.
    password: Redshift password for authentication.
    resource_group: Azure resource group used to whitelist the VM's IP address.
  """

  def __init__(self, server_name: str, database: str, user: str, password: str,
               resource_group: str):
    self.server_name = server_name
    self.database = database
    self.user = user
    self.password = password
    self.resource_group = resource_group

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the sql server tool dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('sudo pip install absl-py')
    self.client_vm.Install('mssql_tools')
    self.whitelist_ip = self.client_vm.ip_address

    cmd = [
        azure.AZURE_PATH, 'sql', 'server', 'firewall-rule', 'create', '--name',
        self.whitelist_ip, '--resource-group', self.resource_group, '--server',
        self.server_name, '--end-ip-address', self.whitelist_ip,
        '--start-ip-address', self.whitelist_ip
    ]
    vm_util.IssueCommand(cmd)

    # Push the framework to execute a sql query and gather performance details
    service_specific_dir = os.path.join('edw',
                                        Azuresqldatawarehouse.SERVICE_TYPE)
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(service_specific_dir, 'script_runner.sh')))
    runner_permission_update_cmd = 'chmod 755 {}'.format('script_runner.sh')
    self.client_vm.RemoteCommand(runner_permission_update_cmd)
    self.client_vm.PushFile(
        data.ResourcePath(os.path.join('edw', 'script_driver.py')))
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(service_specific_dir,
                         'provider_specific_script_driver.py')))

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = (
        'python script_driver.py --script={} --server={} --database={} '
        '--user={} --password={} --query_timeout={}').format(
            query_name, self.server_name, self.database, self.user,
            self.password, FLAGS.query_timeout)
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    performance = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    details['job_id'] = performance[query_name]['job_id']
    return float(performance[query_name]['execution_time']), details

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.sqldatawarehouse_client_interface}


class JdbcClientInterface(edw_service.EdwClientInterface):
  """JDBC Client Interface class for Azure SqlDataWarehouse.

  Attributes:
    server_name: Name of the SqlDataWarehouse server to use.
    database: Name of the database to run queries against.
    user: Redshift username for authentication.
    password: Redshift password for authentication.
    resource_group: Azure resource group used to whitelist the VM's IP address.
  """

  def __init__(self, server_name: str, database: str, user: str, password: str,
               resource_group: str):
    self.server_name = server_name
    self.database = database
    self.user = user
    self.password = password
    self.resource_group = resource_group

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the sql server tool dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')
    self.client_vm.Install('mssql_tools')
    self.client_vm.Install('azure_cli')
    self.whitelist_ip = self.client_vm.ip_address

    cmd = [
        azure.AZURE_PATH, 'sql', 'server', 'firewall-rule', 'create', '--name',
        self.whitelist_ip, '--resource-group', self.resource_group, '--server',
        self.server_name, '--end-ip-address', self.whitelist_ip,
        '--start-ip-address', self.whitelist_ip
    ]
    vm_util.IssueCommand(cmd)

    # Push the executable jar to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(package_name,
                                                    [SYNAPSE_JDBC_JAR], '')

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = (f'java -cp {SYNAPSE_JDBC_JAR} '
                     f'com.google.cloud.performance.edw.Single '
                     f'--server {self.server_name} --database {self.database} '
                     f'--query_timeout {FLAGS.query_timeout} '
                     f'--query_file {query_name}')
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    performance = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    if 'failure_reason' in performance:
      details.update({'failure_reason': performance['failure_reason']})
    else:
      details.update(performance['details'])
    return performance['query_wall_time_in_secs'], details

  def ExecuteSimultaneous(self, submission_interval: int,
                          queries: List[str]) -> str:
    """Executes queries simultaneously on client and return performance details.

    Simultaneous app expects queries as white space separated query file names.

    Args:
      submission_interval: Simultaneous query submission interval in
        milliseconds.
      queries: List of strings (names) of queries to execute.

    Returns:
      A serialized dictionary of execution details.
    """
    query_list = ' '.join(queries)
    cmd = (f'java -cp {SYNAPSE_JDBC_JAR} '
           f'com.google.cloud.performance.edw.Simultaneous '
           f'--server {self.server_name} --database {self.database} '
           f'--submission_interval {submission_interval} --query_timeout '
           f'{FLAGS.query_timeout} --query_files {query_list}')
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout

  def ExecuteThroughput(self, concurrency_streams: List[List[str]]) -> str:
    """Executes a throughput test and returns performance details.

    Args:
      concurrency_streams: List of streams to execute simultaneously, each of
        which is a list of string names of queries.

    Returns:
      A serialized dictionary of execution details.
    """
    query_list = ' '.join([','.join(stream) for stream in concurrency_streams])
    cmd = (
        f'java -cp {SYNAPSE_JDBC_JAR} '
        f'com.google.cloud.performance.edw.Throughput '
        f'--server {self.server_name} --database {self.database} '
        f'--query_timeout {FLAGS.query_timeout} --query_streams {query_list}')
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.sqldatawarehouse_client_interface}


class Azuresqldatawarehouse(edw_service.EdwService):
  """Object representing an Azure SQL data warehouse."""

  CLOUD = providers.AZURE
  SERVICE_TYPE = 'azuresqldatawarehouse'

  def __init__(self, edw_service_spec):
    super(Azuresqldatawarehouse, self).__init__(edw_service_spec)
    self.whitelist_ip = None
    self.resource_group = edw_service_spec.resource_group
    self.server_name = edw_service_spec.server_name
    self.client_interface = GetSqlDataWarehouseClientInterface(
        self.server_name, self.db, self.user, self.password,
        self.resource_group)

  def WhitelistIPAddress(self, ip_address):
    """To whitelist the IP address on the cluster."""
    self.whitelist_ip = ip_address

    cmd = [azure.AZURE_PATH,
           'sql',
           'server',
           'firewall-rule',
           'create',
           '--name',
           self.whitelist_ip,
           '--resource-group',
           self.resource_group,
           '--server',
           self.server_name,
           '--end-ip-address',
           self.whitelist_ip,
           '--start-ip-address',
           self.whitelist_ip]
    vm_util.IssueCommand(cmd)

  def __DescribeCluster(self):
    """Describe cluster."""
    cmd = [azure.AZURE_PATH,
           'sql',
           'dw',
           'show',
           '--name',
           self.db,
           '--resource-group',
           self.resource_group,
           '--server',
           self.server_name]
    return vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self):
    """Method to validate the existence of cluster.

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    stdout, _, _ = self.__DescribeCluster()
    if not stdout or (json.loads(stdout)['status'] not in VALID_EXIST_STATUSES):
      return False
    else:
      return True

  def _IsReady(self):
    """Method to return if the cluster is ready to handle queries."""
    stdout, _, _ = self.__DescribeCluster()
    return json.loads(stdout)['status'] in READY_STATUSES

  def _Create(self):
    """Resuming the cluster."""
    cmd = [azure.AZURE_PATH,
           'sql',
           'dw',
           'resume',
           '--name',
           self.db,
           '--resource-group',
           self.resource_group,
           '--server',
           self.server_name]
    vm_util.IssueCommand(cmd, timeout=420)

  def _IsDeleting(self):
    """Method to check if the cluster is pausing."""
    stdout, _, _ = self.__DescribeCluster()
    if not stdout:
      return False
    else:
      return json.loads(stdout)['status'] in PAUSING_STATUSES

  def _Delete(self):
    """Pausing cluster."""
    cmd = [azure.AZURE_PATH,
           'sql',
           'dw',
           'pause',
           '--name',
           self.db,
           '--resource-group',
           self.resource_group,
           '--server',
           self.server_name]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _DeleteDependencies(self):
    """Delete dependencies of the cluster."""
    if self.client_interface.whitelist_ip is not None:
      cmd = [
          azure.AZURE_PATH, 'sql', 'server', 'firewall-rule', 'delete',
          '--name', self.client_interface.whitelist_ip, '--resource-group',
          self.resource_group, '--server', self.server_name
      ]
      vm_util.IssueCommand(cmd, raise_on_failure=False)

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(Azuresqldatawarehouse, self).GetMetadata()
    basic_data['resource_group'] = self.resource_group
    basic_data['server_name'] = self.server_name
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data
