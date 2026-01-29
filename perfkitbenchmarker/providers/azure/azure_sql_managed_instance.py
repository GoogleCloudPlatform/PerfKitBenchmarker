# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Azure SQL Managed Instance provisioning and teardown."""

from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_relational_db
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS

_GENERAL_PURPOSE_TIER = 'GeneralPurpose'
_BUSINESS_CRITICAL_TIER = 'BusinessCritical'
_VALID_TIERS = [_GENERAL_PURPOSE_TIER, _BUSINESS_CRITICAL_TIER]

_GEN5_FAMILY = 'Gen5'

_UP_TO_DATE = 'AlwaysUpToDate'
_SQL_SERVER_2022 = 'SQLServer2022'
_SQL_SERVER_2025 = 'SQLServer2025'
_VALID_VERSIONS = [_UP_TO_DATE, _SQL_SERVER_2022, _SQL_SERVER_2025]

_DEFAULT_CPUS = 4
_DEFAULT_MEMORY_MB = 16 * 1024

# Azure SQL MI instance creation is unfortunately extremely long.
_CREATE_MANAGED_INSTANCE_TIMEOUT = 60 * 180


class AzureSqlManagedInstance(azure_relational_db.AzureRelationalDb):
  """An object representing Azure SQL Managed Instance."""

  SERVER_TYPE = 'mi'
  ENGINE = [sql_engine_utils.SQLSERVER]
  IS_MANAGED = True

  def __init__(self, relational_db_spec: Any):
    super().__init__(relational_db_spec)
    self.region = util.GetRegionFromZone(self.spec.db_spec.zone)
    self.tier = self.spec.db_tier or _GENERAL_PURPOSE_TIER
    self.family = self.spec.family or _GEN5_FAMILY
    self.version = self.spec.engine_version
    self.disk_size = self.spec.db_disk_spec.disk_size
    self.disk_iops = self.spec.db_disk_spec.provisioned_iops
    self.high_availability = self.spec.high_availability

    self.cpus = self.spec.db_spec.cpus or _DEFAULT_CPUS
    self.memory = self.spec.db_spec.memory
    if self.tier == _GENERAL_PURPOSE_TIER and self.memory is None:
      self.memory = _DEFAULT_MEMORY_MB

    self._CheckPrerequisites()

    self.resource_group = azure_network.GetResourceGroup(self.region)
    self.subnet: azure_network.AzureSubnet = None

  def _CheckPrerequisites(self):
    if self.tier not in _VALID_TIERS:
      raise errors.Config.InvalidValue(
          f'Invalid tier: {self.tier}, must be one of {_VALID_TIERS}'
      )
    if self.version not in _VALID_VERSIONS:
      raise errors.Config.InvalidValue(
          f'Invalid version: {self.version}, must be one of {_VALID_VERSIONS}'
      )
    if self.tier == _BUSINESS_CRITICAL_TIER and self.memory is not None:
      raise errors.Config.InvalidValue(
          'Memory cannot be specified for Business Critical tier, got'
          f' {self.memory}'
      )

  def GetResourceMetadata(self) -> dict[str, Any]:
    """See base class."""
    metadata = super().GetResourceMetadata()
    metadata.update({
        'tier': self.tier,
        'family': self.family,
        'cpus': self.cpus,
    })
    if self.memory is not None:
      metadata['memory'] = self.memory
    # Machine type ends up being a python object, remove it.
    metadata.pop('machine_type')
    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default version of a given database engine."""
    return _UP_TO_DATE

  def GetAzCommandForEngine(self) -> str:
    return 'sql'

  def _CreateRouteTable(self, route_table_name: str) -> None:
    """Creates a route table for the managed instance."""
    cmd = [
        azure.AZURE_PATH,
        'network',
        'route-table',
        'create',
        '--name',
        route_table_name,
        '--resource-group',
        self.resource_group.name,
        '--location',
        self.region,
    ]
    vm_util.IssueCommand(cmd)

    cmd = [
        azure.AZURE_PATH,
        'network',
        'route-table',
        'route',
        'create',
        '--address-prefix',
        '0.0.0.0/0',
        '--name',
        'primaryToMIManagementService',
        '--next-hop-type',
        'Internet',
        '--resource-group',
        self.resource_group.name,
        '--route-table-name',
        route_table_name,
    ]
    vm_util.IssueCommand(cmd)

    cmd = [
        azure.AZURE_PATH,
        'network',
        'route-table',
        'route',
        'create',
        '--address-prefix',
        self.subnet.address_space,
        '--name',
        'ToLocalClusterNode',
        '--next-hop-type',
        'VnetLocal',
        '--resource-group',
        self.resource_group.name,
        '--route-table-name',
        route_table_name,
    ]
    vm_util.IssueCommand(cmd)

  def _CreateDependencies(self):
    """See base class."""
    self.subnet = azure_network.AzureSubnet(
        self.client_vm.network.vnet,
        'pkb-' + FLAGS.run_uri + '-mi-subnet',
        'Microsoft.Sql/managedInstances',
    )
    self.client_vm.network.vnet.UpdateAddressSpace()
    self.subnet.Create()

    nsg_name = self.client_vm.network.nsgs[0].name
    route_table_name = self.subnet.name + '-route-table'
    self._CreateRouteTable(route_table_name)

    cmd = [
        azure.AZURE_PATH,
        'network',
        'vnet',
        'subnet',
        'update',
        '--name',
        self.subnet.name,
        '--network-security-group',
        nsg_name,
        '--route-table',
        route_table_name,
        '--vnet-name',
        self.client_vm.network.vnet.name,
        '--resource-group',
        self.resource_group.name,
    ]
    vm_util.IssueCommand(cmd)

  def _Create(self) -> None:
    """Creates a managed sql server instance."""
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'create',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
        '--location',
        self.region,
        '--admin-user',
        self.spec.database_username,
        '--admin-password',
        self.spec.database_password,
        '--subnet',
        self.subnet.name,
        '--vnet-name',
        self.client_vm.network.vnet.name,
        '--database-format',
        self.version,
        '--no-wait',
    ]
    options = [
        ('--edition', self.tier),
        ('--family', self.family),
        ('--capacity', self.cpus),
        ('--storage', self.disk_size),
        ('--iops', self.disk_iops),
    ]
    for flag, value in options:
      if value:
        cmd.extend([flag, str(value)])
    if self.high_availability:
      cmd.extend([
          '--zone-redundant',
          'true',
      ])
    if self.tier == 'GeneralPurpose':
      cmd.extend([
          '--gpv2',
          'true',
          '--memory',
          str(self.memory // 1024),
      ])
    vm_util.IssueCommand(cmd, timeout=300)
    self._WaitUntilRunning()

    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'midb',
        'create',
        '--resource-group',
        self.resource_group.name,
        '--mi',
        self.instance_id,
        '--name',
        azure_relational_db.DEFAULT_DATABASE_NAME,
    ]
    vm_util.IssueCommand(
        cmd, timeout=azure_relational_db.CREATE_AZURE_DB_TIMEOUT
    )

  @vm_util.Retry(
      poll_interval=60,
      log_errors=False,
      timeout=_CREATE_MANAGED_INSTANCE_TIMEOUT,
      retryable_exceptions=(errors.Resource.CreationError,),
  )
  def _WaitUntilRunning(self) -> None:
    """Waits until the managed instance is ready."""
    json_output = self._AzServerShow()
    if json_output['state'] != 'Ready':
      raise errors.Resource.CreationError(
          'Managed instance is not ready: {}'.format(json_output['state'])
      )

  def _PostCreate(self) -> None:
    """See base class."""
    # Calling the grand parent class.
    super(azure_relational_db.AzureRelationalDb, self)._PostCreate()

  def SetDbConfiguration(self, name: str, value: str) -> tuple[str, str, int]:
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'configuration',
        'set',
        '--name',
        name,
        '--value',
        value,
        '--resource-group',
        self.resource_group.name,
        '--server',
        self.instance_id,
    ]
    return vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Delete(self) -> None:
    self._deleted = True
