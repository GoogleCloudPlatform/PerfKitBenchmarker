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
"""Azure relational database provisioning and teardown.

NOTE: single-server mysql and postgres are deprecated. Use AzureFlexibleServer
instead.
"""

import datetime
import json
import logging
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import mysql_iaas_relational_db
from perfkitbenchmarker import postgres_iaas_relational_db
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import sqlserver_iaas_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util

DEFAULT_DATABASE_NAME = 'database'

FLAGS = flags.FLAGS

DEFAULT_SQLSERVER_VERSION = 'DEFAULT'

IS_READY_TIMEOUT = 60 * 60  # 1 hour (might take some time to prepare)

# Longest time recorded is 20 minutes when
# creating STANDARD_D64_V3 - 12/02/2020
# The Azure command timeout with the following error message:
#
# Deployment failed. Correlation ID: fcdc3c76-33cc-4eb1-986c-fbc30ce7d820.
# The operation timed out and automatically rolled back.
# Please retry the operation.
CREATE_AZURE_DB_TIMEOUT = 60 * 30


class AzureSQLServerIAASRelationalDb(
    sqlserver_iaas_relational_db.SQLServerIAASRelationalDb
):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AZURE
  TEMPDB_DISK_LETTER = 'D'

  def CreateIpReservation(self) -> str:
    cluster_ip_address = '.'.join(
        self.server_vm.internal_ip.split('.')[:-1] + ['128']
    )
    return cluster_ip_address

  def ReleaseIpReservation(self) -> bool:
    return True


class AzurePostgresIAASRelationalDb(
    postgres_iaas_relational_db.PostgresIAASRelationalDb
):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AZURE


class AzureMysqlIAASRelationalDb(
    mysql_iaas_relational_db.MysqlIAASRelationalDb
):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AZURE


class AzureRelationalDb(relational_db.BaseRelationalDb):
  """An object representing an Azure relational database.

  This class requires that a client VM be available as an attribute on the
  instance before Create() is called, which is the current behavior of PKB. This
  is necessary to setup the networking correctly.

  Note that the client VM's region and the region requested for the database
  must be the same.
  """

  CLOUD = provider_info.AZURE
  SERVER_TYPE = ''
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']

  database_name: str

  def __init__(self, relational_db_spec: Any):
    super().__init__(relational_db_spec)
    if util.IsZone(self.spec.db_spec.zone):
      raise errors.Config.InvalidValue(
          'Availability zones are currently not supported by Azure DBs'
      )
    self.region = util.GetRegionFromZone(self.spec.db_spec.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns the metadata associated with the resource.

    All keys will be prefaced with relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of Azure  DB metadata.
    """
    metadata = super().GetResourceMetadata()
    metadata.update({
        'zone': self.spec.db_spec.zone,
    })
    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (sqlserver).

    Returns:
      (string): Default engine version.
    Raises:
      RelationalDbEngineNotFoundError: if an unknown engine is
                                                  requested.
    """
    raise NotImplementedError()

  def GetAzCommandForEngine(self) -> str:
    """Returns the Azure CLI command group name for the database engine."""
    raise NotImplementedError()

  def SetDbConfiguration(self, name: str, value: str) -> tuple[str, str, int]:
    """Set configuration for the database instance.

    Args:
        name: string, the name of the settings to change
        value: value, string the value to set

    Returns:
      Tuple of standand output and standard error
    """
    raise NotImplementedError()

  def _ApplyDbFlags(self) -> None:
    """Apply database flags to the instance."""
    for flag in FLAGS.db_flags:
      name_and_value = flag.split('=')
      _, stderr, _ = self.SetDbConfiguration(
          name_and_value[0], name_and_value[1]
      )
      if stderr:
        raise KeyError(
            'Invalid flags: {}.  Error {}'.format(name_and_value, stderr)
        )

    self._Reboot()

  def _Delete(self) -> None:
    """See base class."""
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'delete',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
        '--yes',
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self) -> bool:
    """See base class."""
    json_server_show = self._AzServerShow()
    if json_server_show is None:
      return False
    return True

  def _IsReady(self, timeout: int = IS_READY_TIMEOUT) -> bool:
    """See base class."""
    return self._IsInstanceReady(timeout)

  def _PostCreate(self) -> None:
    """Perform general post create operations on the cluster."""
    super()._PostCreate()

    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'firewall-rule',
        'create',
        '--resource-group',
        self.resource_group.name,
        '--server',
        self.instance_id,
        '--name',
        'AllowAllIps',
        '--start-ip-address',
        '0.0.0.0',
        '--end-ip-address',
        '255.255.255.255',
    ]
    vm_util.IssueCommand(cmd)

  def _Reboot(self) -> None:
    """Reboot the managed db."""
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'restart',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
    ]
    vm_util.IssueCommand(cmd)

    if not self._IsInstanceReady():
      raise RuntimeError('Instance could not be set to ready after reboot')

  def _IsInstanceReady(self, timeout: int = IS_READY_TIMEOUT) -> bool:
    """Return true if the instance is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'Ready', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.warning('Timeout waiting for sql instance to be ready')
        return False

      server_show_json = self._AzServerShow()
      if server_show_json is not None:
        state = server_show_json['state']
        if state == 'Ready':
          break
      time.sleep(5)

    return True

  def _AzServerShow(self) -> dict[str, Any] | None:
    """Runs the azure command az server show.

    Returns:
      json object representing holding the of the show command on success.
      None for a non 0 retcode.  A non 0 retcode can occur if queried
      before the database has finished being created.
    """
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'show',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

  def _SetEndpoint(self) -> None:
    """Assigns the ports and endpoints from the instance_id to self.

    These will be used to communicate with the database. Called during
    _PostCreate().
    """
    server_show_json = self._AzServerShow()
    self.endpoint = server_show_json['fullyQualifiedDomainName']

  def _FailoverHA(self) -> None:
    raise NotImplementedError()


class AzureSqlManagedInstance(AzureRelationalDb):
  """An object representing Azure SQL Managed Instance."""

  SERVER_TYPE = 'mi'
  ENGINE = [sql_engine_utils.SQLSERVER]

  def __init__(self, relational_db_spec: Any):
    super().__init__(relational_db_spec)
    self.region = util.GetRegionFromZone(self.spec.db_spec.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default version of a given database engine."""
    return DEFAULT_SQLSERVER_VERSION

  def GetAzCommandForEngine(self) -> str:
    return 'sql'

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
          'midb',
          'create',
          '--resource-group',
          self.resource_group.name,
          '--mi',
          self.instance_id,
          '--name',
          DEFAULT_DATABASE_NAME,
      ]
    else:
      # Sample machine_type: GP_Gen5_2
      edition, family, vcore = self.GetConfigFromMachineType(
          self.spec.db_spec.machine_type
      )
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
          'true' if self.spec.high_availability else 'false',
      ]
    vm_util.IssueCommand(cmd, timeout=CREATE_AZURE_DB_TIMEOUT)
    self.database_name = DEFAULT_DATABASE_NAME

  def GetConfigFromMachineType(self, machine_type: str) -> tuple[str, str, str]:
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
          'Unsupported machine type {}, sample machine type GP_Gen5_2'.format(
              machine_type
          )
      )
    edition = machine_type[0]
    if edition == 'BC':
      edition = 'BusinessCritical'
    elif edition == 'GP':
      edition = 'GeneralPurpose'
    else:
      raise relational_db.UnsupportedError(
          'Unsupported edition {}. Only supports BC or GP'.format(machine_type)
      )

    family = machine_type[1]
    vcore = machine_type[2]
    return (edition, family, vcore)

  def _Create(self):
    raise NotImplementedError('Creation of SQL MI not implemented.')

  def _PostCreate(self) -> None:
    super(AzureRelationalDb, self)._PostCreate()
    logging.info(
        'Firewall rules are not configured for Azure SQL MI. '
        'It relies on subnet NSG rules for network access.'
    )

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

  def _Reboot(self) -> None:
    logging.info('Reboot is not supported for Azure SQL Managed Instance.')

  def _ApplyDbFlags(self) -> None:
    if FLAGS.db_flags:
      raise NotImplementedError(
          'Applying DB flags is not implemented for Azure SQL MI.'
      )
