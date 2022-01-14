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
"""Relational database provisioning and teardown for Azure RDS."""

import datetime
import json
import logging
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util

DEFAULT_DATABASE_NAME = 'database'

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '5.7'
DEFAULT_POSTGRES_VERSION = '9.6'
DEFALUT_SQLSERVER_VERSION = 'DEFAULT'

# Disk size configurations details at
# https://docs.microsoft.com/en-us/cli/azure/mysql/server?view=azure-cli-latest#az_mysql_server_create
AZURE_MIN_DB_DISK_SIZE_MB = 5120  # Minimum db disk size supported by Azure
AZURE_MAX_DB_DISK_SIZE_MB = 16777216  # Maximum db disk size supported by Azure

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (might take some time to prepare)

# Longest time recorded is 20 minutes when
# creating STANDARD_D64_V3 - 12/02/2020
# The Azure command timeout with the following error message:
#
# Deployment failed. Correlation ID: fcdc3c76-33cc-4eb1-986c-fbc30ce7d820.
# The operation timed out and automatically rolled back.
# Please retry the operation.
CREATE_AZURE_DB_TIMEOUT = 60 * 30


class AzureRelationalDb(relational_db.BaseRelationalDb):
  """An object representing an Azure RDS relational database.

  Currently Postgres is supported. This class requires that a
  client vm be available as an attribute on the instance before Create() is
  called, which is the current behavior of PKB. This is necessary to setup the
  networking correctly. The following steps are performed to provision the
  database:
    1. create the RDS instance in the requested region.

  Instructions from:
  https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-server-database-azure-cli

  On teardown, all resources are deleted.

  Note that the client VM's region and the region requested for the database
  must be the same.

  """
  CLOUD = providers.AZURE

  database_name: str

  def __init__(self, relational_db_spec):
    super(AzureRelationalDb, self).__init__(relational_db_spec)
    if util.IsZone(self.spec.db_spec.zone):
      raise errors.Config.InvalidValue(
          'Availability zones are currently not supported by Azure DBs')
    self.region = util.GetRegionFromZone(self.spec.db_spec.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)

    self.unmanaged_db_exists = None if self.is_managed_db else False

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of Azure  DB metadata.

    """
    metadata = super(AzureRelationalDb, self).GetResourceMetadata()
    metadata.update({
        'zone': self.spec.db_spec.zone,
    })

    if hasattr(self.spec.db_disk_spec, 'iops'):
      metadata.update({
          'disk_iops': self.spec.db_disk_spec.iops,
      })

    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).
    Returns:
      (string): Default engine version.
    Raises:
      RelationalDbEngineNotFoundError: if an unknown engine is
                                                  requested.
    """
    if engine == sql_engine_utils.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif engine == sql_engine_utils.MYSQL:
      return DEFAULT_MYSQL_VERSION
    elif engine == sql_engine_utils.SQLSERVER:
      return DEFALUT_SQLSERVER_VERSION
    else:
      raise relational_db.RelationalDbEngineNotFoundError(
          'Unsupported engine {0}'.format(engine))

  def GetAzCommandForEngine(self):
    engine = self.spec.engine
    if engine == sql_engine_utils.POSTGRES:
      return 'postgres'
    elif engine == sql_engine_utils.MYSQL:
      return 'mysql'
    elif engine == sql_engine_utils.SQLSERVER:
      return 'sql'
    raise relational_db.RelationalDbEngineNotFoundError(
        'Unsupported engine {0}'.format(engine))

  def GetConfigFromMachineType(self, machine_type):
    """Returns a tuple of (edition, family, vcore) from Azure machine type.

    Args:
     machine_type (string): Azure machine type i.e GP_Gen5_4
    Returns:
      (string, string, string): edition, family, vcore
    Raises:
      UnsupportedError: if the machine type is not supported.
    """
    machine_type = machine_type.split('_')
    if len(machine_type) != 3:
      raise relational_db.UnsupportedError(
          'Unsupported machine type {0},'
          ' sample machine type GP_Gen5_2'.format(machine_type))
    edition = machine_type[0]
    if edition == 'BC':
      edition = 'BusinessCritical'
    elif edition == 'GP':
      edition = 'GeneralPurpose'
    else:
      raise relational_db.UnsupportedError(
          'Unsupported edition {}. Only supports BC or GP'.format(machine_type))

    family = machine_type[1]
    vcore = machine_type[2]
    return (edition, family, vcore)

  def SetDbConfiguration(self, name, value):
    """Set configuration for the database instance.

    Args:
        name: string, the name of the settings to change
        value: value, string the value to set
    """
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
        'configuration',
        'set',
        '--name',
        name,
        '--value',
        value,
        '--resource-group',
        self.resource_group.name,
        '--server',
        self.instance_id
    ]
    vm_util.IssueCommand(cmd)

  def RenameDatabase(self, new_name):
    """Renames an the database instace."""
    engine = self.spec.engine
    if engine == sql_engine_utils.SQLSERVER:
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          'db',
          'rename',
          '--resource-group',
          self.resource_group.name,
          '--server',
          self.instance_id,
          '--name',
          self.database_name,
          '--new-name',
          new_name
      ]
      vm_util.IssueCommand(cmd)
      self.database_name = new_name
    else:
      raise relational_db.RelationalDbEngineNotFoundError(
          'Unsupported engine {0}'.format(engine))

  def _ApplyManagedDbFlags(self):
    """Applies the MySqlFlags to a managed instance."""
    for flag in FLAGS.db_flags:
      name_and_value = flag.split('=')
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(), 'server', 'configuration', 'set',
          '--name', name_and_value[0], '--resource-group',
          self.resource_group.name, '--server', self.instance_id, '--value',
          name_and_value[1]
      ]
      _, stderr, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
      if stderr:
        raise Exception('Invalid MySQL flags: {0}.  Error {1}'.format(
            name_and_value, stderr))

    self._Reboot()

  def _CreateMySqlOrPostgresInstance(self):
    """Creates a managed MySql or Postgres instance."""
    if not self.spec.high_availability:
      raise Exception('Azure databases can only be used in high '
                      'availability. Please rerurn with flag '
                      '--managed_db_high_availability=True')

    # Valid storage sizes range from minimum of 5120 MB
    # and additional increments of 1024 MB up to maximum of 16777216 MB.
    azure_disk_size_mb = self.spec.db_disk_spec.disk_size * 1024
    if azure_disk_size_mb > AZURE_MAX_DB_DISK_SIZE_MB:
      error_msg = ('Azure disk size was specified as in the disk spec as %s,'
                   'got rounded to %s which is greater than the '
                   'maximum of 16777216 MB' % (
                       self.spec.db_disk_spec.disk_size, azure_disk_size_mb))
      raise errors.Config.InvalidValue(error_msg)

    elif azure_disk_size_mb < AZURE_MIN_DB_DISK_SIZE_MB:
      error_msg = ('Azure disk size was specified '
                   'as in the disk spec as %s, got rounded to %s '
                   'which is smaller than the minimum of 5120 MB' % (
                       self.spec.db_disk_spec.disk_size, azure_disk_size_mb))
      raise  errors.Config.InvalidValue(error_msg)

    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
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
        '--storage-size',
        str(azure_disk_size_mb),
        '--sku-name',
        self.spec.db_spec.machine_type,
        '--version',
        self.spec.engine_version,
    ]

    vm_util.IssueCommand(cmd, timeout=CREATE_AZURE_DB_TIMEOUT)

  def _CreateSqlServerInstance(self):
    """Creates a managed sql server instance."""
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
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
        self.spec.database_password
    ]
    vm_util.IssueCommand(cmd)

    # Azure support two ways of specifying machine type DTU or with vcores
    # if compute units is specified we will use the DTU model
    if self.spec.db_spec.compute_units is not None:
      # Supported families & capacities for 'Standard' are:
      # [(None, 10), (None, 20), (None, 50), (None, 100), (None, 200),
      # (None, 400), (None, 800), (None, 1600), (None, 3000)]

      # Supported families & capacities for 'Premium' are:
      # [(None, 125), (None, 250), (None, 500), (None, 1000), (None, 1750),
      #  (None, 4000)].

      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          'db',
          'create',
          '--resource-group',
          self.resource_group.name,
          '--server',
          self.instance_id,
          '--name',
          DEFAULT_DATABASE_NAME,
          '--edition',
          self.spec.db_spec.tier,
          '--capacity',
          str(self.spec.db_spec.compute_units),
          '--zone-redundant',
          'true' if self.spec.high_availability else 'false'
      ]
    else:
      # Sample machine_type: GP_Gen5_2
      edition, family, vcore = (
          self.GetConfigFromMachineType(self.spec.db_spec.machine_type))
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          'db',
          'create',
          '--resource-group',
          self.resource_group.name,
          '--server',
          self.instance_id,
          '--name',
          DEFAULT_DATABASE_NAME,
          '--edition',
          edition,
          '--family',
          family,
          '--capacity',
          vcore,
          '--zone-redundant',
          'true' if self.spec.high_availability else 'false'
      ]
    vm_util.IssueCommand(cmd)
    self.database_name = DEFAULT_DATABASE_NAME

  def _CreateAzureManagedSqlInstance(self):
    """Creates an Azure Sql Instance from a managed service."""
    if self.spec.engine == sql_engine_utils.POSTGRES:
      self._CreateMySqlOrPostgresInstance()
    elif self.spec.engine == sql_engine_utils.MYSQL:
      self._CreateMySqlOrPostgresInstance()
    elif self.spec.engine == sql_engine_utils.SQLSERVER:
      self._CreateSqlServerInstance()
    else:
      raise NotImplementedError('Unknown how to create Azure data base '
                                'engine {0}'.format(self.spec.engine))

  def _CreateAzureUnmanagedSqlInstance(self):
    """Creates an Azure Sql Instance hosted inside of a VM."""
    self.endpoint = self.server_vm.ip_address
    self._SetupUnmanagedDatabase()
    self.firewall = azure_network.AzureFirewall()
    self.firewall.AllowPort(
        self.server_vm,
        self.port,
        source_range=['%s/32' % self.client_vm.ip_address])

  def _Create(self):
    """Creates the Azure RDS instance.

    Raises:
      NotImplementedError: if unknown how to create self.spec.engine.
      Exception: if attempting to create a non high availability database.

    """
    if self.is_managed_db:
      self._CreateAzureManagedSqlInstance()
    else:
      self.unmanaged_db_exists = True
      self._CreateAzureUnmanagedSqlInstance()

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    if not self.is_managed_db:
      if hasattr(self, 'firewall'):
        self.firewall.DisallowAllPorts()
      self.unmanaged_db_exists = False
      self.PrintUnmanagedDbStats()
      return

    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
        'delete',
        '--resource-group', self.resource_group.name,
        '--name', self.instance_id,
        '--yes'
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    if not self.is_managed_db:
      return self.unmanaged_db_exists

    json_server_show = self._AzServerShow()
    if json_server_show is None:
      return False
    return True

  def _IsReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the underlying resource is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'available', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """

    return self._IsInstanceReady(timeout)

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    """
    super()._PostCreate()

    if self.is_managed_db:
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          'server',
          'firewall-rule',
          'create',
          '--resource-group', self.resource_group.name,
          '--server', self.instance_id,
          '--name', 'AllowAllIps',
          '--start-ip-address', '0.0.0.0',
          '--end-ip-address', '255.255.255.255'
      ]
      vm_util.IssueCommand(cmd)
      self._AssignEndpointForWriterInstance()

      if self.spec.engine == 'mysql' or self.spec.engine == 'postgres':
        # Azure will add @domainname after the database username
        self.spec.database_username = (self.spec.database_username + '@' +
                                       self.endpoint.split('.')[0])

    self.client_vm_query_tools.InstallPackages()

  def _Reboot(self):
    """Reboot the managed db."""
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
        'restart',
        '--resource-group', self.resource_group.name,
        '--name', self.instance_id
    ]
    vm_util.IssueCommand(cmd)

    if not self._IsInstanceReady():
      raise Exception('Instance could not be set to ready after '
                      'reboot')

  def _IsInstanceReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the instance is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'Ready', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """
    if not self.is_managed_db:
      return self._IsReadyUnmanaged()

    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.warning('Timeout waiting for sql instance to be ready')
        return False

      server_show_json = self._AzServerShow()
      if server_show_json is not None:
        engine = self.spec.engine
        if engine == sql_engine_utils.POSTGRES:
          state = server_show_json['userVisibleState']
        elif engine == sql_engine_utils.MYSQL:
          state = server_show_json['userVisibleState']
        elif engine == sql_engine_utils.SQLSERVER:
          state = server_show_json['state']
        else:
          raise relational_db.RelationalDbEngineNotFoundError(
              'The db engine does not contain a valid state')

        if state == 'Ready':
          break
      time.sleep(5)

    return True

  def _AzServerShow(self):
    """Runs the azure command az server show.

    Returns:
      json object representing holding the of the show command on success.
      None for a non 0 retcode.  A non 0 retcode can occur if queried
      before the database has finished being created.
    """
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
        'show',
        '--resource-group', self.resource_group.name,
        '--name', self.instance_id
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

  def _AssignEndpointForWriterInstance(self):
    """Assigns the ports and endpoints from the instance_id to self.

    These will be used to communicate with the data base
    """
    server_show_json = self._AzServerShow()
    self.endpoint = server_show_json['fullyQualifiedDomainName']

  def _FailoverHA(self):
    raise NotImplementedError()
