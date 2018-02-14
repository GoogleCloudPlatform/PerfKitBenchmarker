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
"""Managed relational database provisioning and teardown for Azure RDS."""

import datetime
import json
import logging
import time
from perfkitbenchmarker import flags
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '5.7'
DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_VERSION = '9.6'
DEFAULT_POSTGRES_PORT = 5432

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (might take some time to prepare)


class AzureManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
  """An object representing an Azure RDS managed relational database.

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

  def __init__(self, managed_relational_db_spec):
    super(AzureManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri
    self.region = self.spec.vm_spec.zone
    self.resource_group = azure_network.GetResourceGroup(self.region)

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with managed_relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of Azure Managed DB metadata.

    """
    metadata = super(AzureManagedRelationalDb, self).GetResourceMetadata()
    metadata.update({
        'zone': self.spec.vm_spec.zone,
    })

    if hasattr(self.spec.disk_spec, 'iops'):
      metadata.update({
          'disk_iops': self.spec.disk_spec.iops,
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
      ManagedRelationalDbEngineNotFoundException: if an unknown engine is
                                                  requested.
    """
    if engine == managed_relational_db.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif engine == managed_relational_db.MYSQL:
      return DEFAULT_MYSQL_VERSION
    else:
      raise managed_relational_db.ManagedRelationalDbEngineNotFoundException(
          'Unsupported engine {0}'.format(engine))

  def GetDefaultPort(self):
    """Returns the default port of a given database engine.

    Returns:
      (string): Default port
    Raises:
      ManagedRelationalDbEngineNotFoundException: if an unknown engine is
                                                  requested.
    """
    engine = self.spec.engine
    if engine == managed_relational_db.POSTGRES:
      return DEFAULT_POSTGRES_PORT
    elif engine == managed_relational_db.MYSQL:
      return DEFAULT_MYSQL_PORT
    raise managed_relational_db.ManagedRelationalDbEngineNotFoundException(
        'Unsupported engine {0}'.format(engine))

  def GetAzCommandForEngine(self):
    engine = self.spec.engine
    if engine == managed_relational_db.POSTGRES:
      return 'postgres'
    elif engine == managed_relational_db.MYSQL:
      return 'mysql'
    raise managed_relational_db.ManagedRelationalDbEngineNotFoundException(
        'Unsupported engine {0}'.format(engine))

  def _Create(self):
    """Creates the Azure RDS instance.

    Raises:
      NotImplementedError: if unknown how to create self.spec.engine.

    """
    if self.spec.engine == managed_relational_db.POSTGRES or (
        self.spec.engine == managed_relational_db.MYSQL):
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          'server',
          'create',
          '--resource-group', self.resource_group.name,
          '--name', self.instance_id,
          '--location', self.region,
          '--admin-user', self.spec.database_username,
          '--admin-password', self.spec.database_password,
          '--performance-tier', self.spec.vm_spec.tier,
          '--compute-units', str(self.spec.vm_spec.compute_units),
          # AZ command line expects 128000MB-1024000MB in increments of 128000MB
          '--storage-size', str(self.spec.disk_spec.disk_size * 1000),
          '--version', self.spec.engine_version,
      ]
      vm_util.IssueCommand(cmd)

    else:
      raise NotImplementedError('Unknown how to create Azure data base '
                                'engine {0}'.format(self.spec.engine))

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """

    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        'server',
        'delete',
        '--resource-group', self.resource_group.name,
        '--name', self.instance_id,
        '--yes'
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """

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
    self._AssignPortsForWriterInstance()

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
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.warning('Timeout waiting for sql instance to be ready')
        return False

      server_show_json = self._AzServerShow()
      if server_show_json is not None:
        user_visibile_state = server_show_json['userVisibleState']
        if user_visibile_state == 'Ready':
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
    stdout, _, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

  def _AssignPortsForWriterInstance(self):
    """Assigns the ports and endpoints from the instance_id to self.

    These will be used to communicate with the data base
    """
    server_show_json = self._AzServerShow()
    self.endpoint = server_show_json['fullyQualifiedDomainName']
    self.port = self.GetDefaultPort()

  def MakePsqlConnectionString(self, database_name):
    """Makes the connection string used to connect via PSql.

    Override implemenation in base class.  Azure postgres needs this format.

    Args:
        database_name: string, the name of the database to connect to.

    Returns:
        The connection string to use.
    """
    return '\'host={0} user={1}@{2} password={3} dbname={4}\''.format(
        self.endpoint,
        self.spec.database_username,
        self.instance_id,
        self.spec.database_password,
        database_name)

  def MakeMysqlConnectionString(self):
    """Makes the connection string used to connect via mysql command.
    Override implemenation in base class.  Azure postgres needs this format.

    Returns:
        The connection string to use.
    """
    return '-h {0} -u {1}@{2} -p{3}'.format(
        self.endpoint,
        self.spec.database_username,
        self.endpoint,
        self.spec.database_password)

  def MakeSysbenchConnectionString(self):
    """Makes the connection string used to connect via sysbench command.

    Override implemenation in base class.  Azure postgres needs this format.

    Returns:
        The connection string to use.
    """
    return '--mysql-host={0} --mysql-user={1}@{2} --mysql-password="{3}" '.format(
        self.endpoint,
        self.spec.database_username,
        self.endpoint,
        self.spec.database_password)

