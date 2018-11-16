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

import json
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure


FLAGS = flags.FLAGS


VALID_EXIST_STATUSES = ['Resuming', 'Online']
READY_STATUSES = ['Online']
PAUSING_STATUSES = ['Pausing']


class Azuresqldatawarehouse(edw_service.EdwService):
  """Object representing an Azure SQL data warehouse.
  """

  CLOUD = providers.AZURE
  SERVICE_TYPE = 'azuresqldatawarehouse'

  def __init__(self, edw_service_spec):
    super(Azuresqldatawarehouse, self).__init__(edw_service_spec)
    self.whitelist_ip = None

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
    return vm_util.IssueCommand(cmd)

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
    stdout, stderr, _ = vm_util.IssueCommand(cmd)

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
    stdout, stderr, _ = vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    """Delete dependencies of the cluster."""
    if self.whitelist_ip is not None:
      cmd = [azure.AZURE_PATH,
             'sql',
             'server',
             'firewall-rule',
             'delete',
             '--name',
             self.whitelist_ip,
             '--resource-group',
             self.resource_group,
             '--server',
             self.server_name]
      vm_util.IssueCommand(cmd)

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(Azuresqldatawarehouse, self).GetMetadata()
    basic_data['resource_group'] = self.resource_group
    basic_data['server_name'] = self.server_name
    return basic_data

  def RunCommandHelper(self):
    """Azure SQL data warehouse specific run script command components."""
    return '--server={} --database={} --user={} --password={}'.format(
        self.server_name, self.db, self.user, self.password)

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of azure runner.

    sqlcmd client
    https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-2017#ubuntu

    azure cli
    https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt?view=azure-cli-latest#install

    Args:
      vm: Client vm on which the script will be run.
    """
    vm.Install('mssql_tools')
