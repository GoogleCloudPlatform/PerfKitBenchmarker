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
"""Managed relational database provisioning for GCP.

As of June 2017 to make this benchmark run for GCP you must install the
gcloud beta component. This is necessary because creating a Cloud SQL instance
with a non-default storage size is in beta right now. This can be removed when
this feature is part of the default components.
See https://cloud.google.com/sdk/gcloud/reference/beta/sql/instances/create
for more information.
"""


import datetime
import json
import logging
import time

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import mysql_iaas_relational_db
from perfkitbenchmarker import omni_postgres_iaas_relational_db
from perfkitbenchmarker import postgres_iaas_relational_db
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import sqlserver_iaas_relational_db
from perfkitbenchmarker import timescaledb_iaas_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS

GCP_DATABASE_VERSION_MAPPING = {
    sql_engine_utils.MYSQL: {
        '5.5': 'MYSQL_5_5',
        '5.6': 'MYSQL_5_6',
        '5.7': 'MYSQL_5_7',
        '8.0': 'MYSQL_8_0',
        '8.0.31': 'MYSQL_8_0_31',
    },
    sql_engine_utils.POSTGRES: {
        '9.6': 'POSTGRES_9_6',
        '10': 'POSTGRES_10',
        '11': 'POSTGRES_11',
        '12': 'POSTGRES_12',
        '13': 'POSTGRES_13',
        '14': 'POSTGRES_14',
        '15': 'POSTGRES_15',
    },
    sql_engine_utils.SQLSERVER: {
        '2017_Standard': 'SQLSERVER_2017_Standard',
        '2017_Enterprise': 'SQLSERVER_2017_ENTERPRISE',
        '2017_Express': 'SQLSERVER_2017_EXPRESS',
        '2017_Web': 'SQLSERVER_2017_WEB',
        '2019_Standard': 'SQLSERVER_2019_Standard',
        '2019_Enterprise': 'SQLSERVER_2019_ENTERPRISE',
        '2019_Express': 'SQLSERVER_2019_EXPRESS',
        '2019_Web': 'SQLSERVER_2019_WEB',
    },
}


DEFAULT_MYSQL_VERSION = '5.7'
DEFAULT_POSTGRES_VERSION = '9.6'
DEFAULT_SQL_SERVER_VERSION = '2017_Standard'

DEFAULT_ENGINE_VERSIONS = {
    sql_engine_utils.MYSQL: DEFAULT_MYSQL_VERSION,
    sql_engine_utils.POSTGRES: DEFAULT_POSTGRES_VERSION,
    sql_engine_utils.SQLSERVER: DEFAULT_SQL_SERVER_VERSION,
}

# TODO(chunla): Move to engine specific module
DEFAULT_USERNAME = {
    sql_engine_utils.MYSQL: 'root',
    sql_engine_utils.POSTGRES: 'postgres',
    sql_engine_utils.SQLSERVER: 'sqlserver',
}

# PostgreSQL restrictions on memory.
# Source: https://cloud.google.com/sql/docs/postgres/instance-settings.
CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND = 0.9
CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND = 6.5
MIN_CUSTOM_MACHINE_MEM_MB = 3840

IS_READY_TIMEOUT = 600  # 10 minutes
DELETE_INSTANCE_TIMEOUT = 600  # 10 minutes
CREATION_TIMEOUT = 1200  # 20 minutes


class UnsupportedDatabaseEngineError(Exception):
  pass


class GCPSQLServerIAASRelationalDb(
    sqlserver_iaas_relational_db.SQLServerIAASRelationalDb
):
  """A GCP IAAS database resource."""

  CLOUD = provider_info.GCP

  def __init__(self, relational_db_spec):
    super().__init__(relational_db_spec)
    self._reserved_ip_address = None

  def CreateIpReservation(self) -> str:
    ip_address_name = 'fci-ip-{}'.format(FLAGS.run_uri)
    self._reserved_ip_address = gce_network.GceIPAddress(
        self.server_vm.project,
        util.GetRegionFromZone(self.server_vm.zone),
        ip_address_name,
        self.server_vm.network.primary_subnet_name,
    )
    self._reserved_ip_address.Create()
    return self._reserved_ip_address.ip_address

  def ReleaseIpReservation(self) -> bool:
    if self._reserved_ip_address:
      self._reserved_ip_address.Delete()
    return self._reserved_ip_address is None

  def _Delete(self):
    super()._Delete()
    if self._reserved_ip_address:
      self._reserved_ip_address.Delete()


class GCPPostgresIAASRelationalDb(
    postgres_iaas_relational_db.PostgresIAASRelationalDb
):
  """A GCP IAAS database resource."""

  CLOUD = provider_info.GCP


class GCPMysqlIAASRelationalDb(mysql_iaas_relational_db.MysqlIAASRelationalDb):
  """A GCP IAAS database resource."""

  CLOUD = provider_info.GCP


class GCPMariaDbIAASRelationalDb(
    mysql_iaas_relational_db.MariaDbIAASRelationalDB
):
  """A GCP IAAS database resource."""

  CLOUD = provider_info.GCP


class GCPOmniPostgresIAASRelationalDb(
    omni_postgres_iaas_relational_db.OmniPostgresIAASRelationalDb
):
  """A GCP Omni Postgres IAAS database resource."""

  CLOUD = provider_info.GCP


class GCPTimescaleDbPostgresIAASRelationalDb(
    timescaledb_iaas_relational_db.TimescaleDbIAASRelationalDb
):
  """A TimescaleDB Postgres IAAS database resource."""

  CLOUD = provider_info.GCP


class GCPRelationalDb(relational_db.BaseRelationalDb):
  """A GCP CloudSQL database resource.

  This class contains logic required to provision and teardown the database.
  Currently, the database will be open to the world (0.0.0.0/0) which is not
  ideal; however, a password is still required to connect. Currently only
  MySQL 5.7 and Postgres 9.6 are supported.
  """

  CLOUD = provider_info.GCP
  IS_MANAGED = True

  def __init__(self, relational_db_spec):
    super().__init__(relational_db_spec)
    self.project = FLAGS.project or util.GetDefaultProject()

  def _CreateDependencies(self):
    util.SetupPrivateServicesAccess(
        self.client_vm.network.network_resource.name, self.project
    )

  def _CreateGcloudSqlInstance(self):
    storage_size = self.spec.db_disk_spec.disk_size
    instance_zone = self.spec.db_spec.zone

    database_version_string = self._GetEngineVersionString(
        self.spec.engine, self.spec.engine_version
    )

    cmd_string = [
        self,
        'sql',
        'instances',
        'create',
        self.instance_id,
        '--quiet',
        '--format=json',
        '--activation-policy=ALWAYS',
        '--no-assign-ip',
        '--network=%s' % self.client_vm.network.network_resource.name,
        '--allocated-ip-range-name=google-service-range',
        '--zone=%s' % instance_zone,
        '--database-version=%s' % database_version_string,
        '--storage-size=%d' % storage_size,
        '--labels=%s' % util.MakeFormattedDefaultTags(),
    ]
    if self.spec.engine == sql_engine_utils.MYSQL:
      cmd_string.append('--enable-bin-log')

    if self.spec.engine == sql_engine_utils.SQLSERVER:
      # `--root-password` is required when creating SQL Server instances.
      cmd_string.append(
          '--root-password={}'.format(self.spec.database_password)
      )

    if self.spec.db_spec.cpus and self.spec.db_spec.memory:
      self._ValidateSpec()
      memory = self.spec.db_spec.memory
      cpus = self.spec.db_spec.cpus
      self._ValidateMachineType(memory, cpus)
      cmd_string.append('--cpu={}'.format(cpus))
      cmd_string.append('--memory={}MiB'.format(memory))
    elif hasattr(self.spec.db_spec, 'machine_type'):
      machine_type_flag = '--tier=%s' % self.spec.db_spec.machine_type
      cmd_string.append(machine_type_flag)
    else:
      raise RuntimeError('Unspecified machine type')

    if self.spec.high_availability:
      cmd_string.append(self._GetHighAvailabilityFlag())

    if self.spec.backup_enabled:
      cmd_string.append('--backup')
      cmd_string.append(
          '--backup-start-time={}'.format(self.spec.backup_start_time)
      )
    else:
      cmd_string.append('--no-backup')
    cmd = util.GcloudCommand(*cmd_string)
    cmd.flags['project'] = self.project
    cmd.use_beta_gcloud = True

    if self.spec.db_tier:
      cmd.flags['edition'] = self.spec.db_tier
      cmd.use_alpha_gcloud = True
      cmd.use_beta_gcloud = False
      if relational_db.ENABLE_DATA_CACHE.value:
        cmd.flags['enable-data-cache'] = True
      else:
        cmd.flags['no-enable-data-cache'] = True

    _, stderr, retcode = cmd.Issue(timeout=CREATION_TIMEOUT)

    util.CheckGcloudResponseKnownFailures(stderr, retcode)

  def _Create(self):
    """Creates the Cloud SQL instance and authorizes traffic from anywhere.

    Raises:
      UnsupportedDatabaseEngineError:
        if the database is unmanaged and the engine isn't MYSQL.
      Exception: if an invalid MySQL flag was used.
    """
    self._CreateGcloudSqlInstance()

  def _GetHighAvailabilityFlag(self):
    """Returns a flag that enables high-availability.

    Returns:
      Flag (as string) to be appended to the gcloud sql create command.
    """
    return '--availability-type=REGIONAL'

  def _ValidateSpec(self):
    """Validates PostgreSQL spec for CPU and memory.

    Raises:
      data.ResourceNotFound: On missing memory or cpus in postgres benchmark
        config.
    """
    if not hasattr(self.spec.db_spec, 'cpus') or not self.spec.db_spec.cpus:
      raise data.ResourceNotFound(
          'Must specify cpu count in benchmark config. See https://'
          'cloud.google.com/sql/docs/postgres/instance-settings for more '
          'details about size restrictions.'
      )
    if not hasattr(self.spec.db_spec, 'memory') or not self.spec.db_spec.memory:
      raise data.ResourceNotFound(
          'Must specify a memory amount in benchmark config. See https://'
          'cloud.google.com/sql/docs/postgres/instance-settings for more '
          'details about size restrictions.'
      )

  def _ValidateMachineType(self, memory, cpus):
    """Validates the custom machine type configuration.

    Memory and CPU must be within the parameters described here:
    https://cloud.google.com/sql/docs/postgres/instance-settings

    Args:
      memory: (int) in MiB
      cpus: (int)

    Raises:
      ValueError on invalid configuration.
    """
    if cpus not in [1] + list(range(2, 97, 2)):
      raise ValueError(
          'CPUs (%i) much be 1 or an even number in-between 2 and 96, '
          'inclusive.' % cpus
      )

    if memory % 256 != 0:
      raise ValueError(
          'Total memory (%dMiB) for a custom machine must be a multiple'
          'of 256MiB.' % memory
      )
    ratio = memory / 1024.0 / cpus
    if (
        ratio < CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND
        or ratio > CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND
    ):
      raise ValueError(
          'The memory (%.2fGiB) per vCPU (%d) of a custom machine '
          'type must be between %.2f GiB and %.2f GiB per vCPU, '
          'inclusive.'
          % (
              memory / 1024.0,
              cpus,
              CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND,
              CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND,
          )
      )
    if memory < MIN_CUSTOM_MACHINE_MEM_MB:
      raise ValueError(
          'The total memory (%dMiB) for a custom machine type'
          'must be at least %dMiB.' % (memory, MIN_CUSTOM_MACHINE_MEM_MB)
      )

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    if hasattr(self, 'replica_instance_id'):
      cmd = util.GcloudCommand(
          self,
          'sql',
          'instances',
          'delete',
          self.replica_instance_id,
          '--quiet',
      )
      cmd.Issue(raise_on_failure=False, timeout=DELETE_INSTANCE_TIMEOUT)

    cmd = util.GcloudCommand(
        self,
        'sql',
        'instances',
        'delete',
        self.instance_id,
        '--quiet',
        '--async',
    )
    cmd.Issue(raise_on_failure=False, timeout=DELETE_INSTANCE_TIMEOUT)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    cmd = util.GcloudCommand(
        self, 'sql', 'instances', 'describe', self.instance_id
    )
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    try:
      json_output = json.loads(stdout)
      return json_output['kind'] == 'sql#instance'
    except:  # pylint: disable=bare-except
      return False

  def _IsDBInstanceReady(self, instance_id, timeout=IS_READY_TIMEOUT):
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe', instance_id)
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds > timeout:
        logging.exception('Timeout waiting for sql instance to be ready')
        return False
      stdout, _, _ = cmd.Issue(raise_on_failure=False)

      try:
        json_output = json.loads(stdout)
        state = json_output['state']
        logging.info('Instance %s state: %s', instance_id, state)
        if state == 'RUNNABLE':
          break
      except:  # pylint: disable=bare-except
        logging.exception('Error attempting to read stdout. Creation failure.')
        return False
      time.sleep(5)

    return True

  def _IsReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Args:
      timeout: how long to wait when checking if the DB is ready.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    if not self._IsDBInstanceReady(self.instance_id, timeout):
      return False
    if self.spec.high_availability and hasattr(self, 'replica_instance_id'):
      if not self._IsDBInstanceReady(self.replica_instance_id, timeout):
        return False

    cmd = util.GcloudCommand(
        self, 'sql', 'instances', 'describe', self.instance_id
    )
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    self.endpoint = self._ParseEndpoint(json_output)
    return True

  def _ParseEndpoint(self, describe_instance_json):
    """Returns the IP of the resource given the metadata as JSON.

    Args:
      describe_instance_json: JSON output.

    Returns:
      public IP address (string)
    """
    if describe_instance_json is None:
      return ''
    try:
      selflink = describe_instance_json['ipAddresses'][0]['ipAddress']
    except:  # pylint: disable=bare-except
      selflink = ''
      logging.exception('Error attempting to read stdout. Creation failure.')
    return selflink

  @vm_util.Retry(max_retries=4, poll_interval=2)
  def SetManagedDatabasePassword(self):
    # The hostname '%' means unrestricted access from any host.
    cmd = util.GcloudCommand(
        self,
        'sql',
        'users',
        'create',
        self.spec.database_username,
        '--host=%',
        '--instance={}'.format(self.instance_id),
        '--password={}'.format(self.spec.database_password),
    )
    _, _, _ = cmd.Issue()

    # By default the empty password is a security violation.
    # Change the password to a non-default value.
    default_user = DEFAULT_USERNAME[self.spec.engine]

    cmd = util.GcloudCommand(
        self,
        'sql',
        'users',
        'set-password',
        default_user,
        '--host=%',
        '--instance={}'.format(self.instance_id),
        '--password={}'.format(self.spec.database_password),
    )
    _, _, _ = cmd.Issue()

  def _PostCreate(self):
    """Creates the PKB user and sets the password."""
    super()._PostCreate()
    self.SetManagedDatabasePassword()

  def _ApplyDbFlags(self):
    cmd_string = [
        self,
        'sql',
        'instances',
        'patch',
        self.instance_id,
        '--database-flags=%s' % ','.join(FLAGS.db_flags),
    ]
    cmd = util.GcloudCommand(*cmd_string)
    _, stderr, _ = cmd.Issue()
    if stderr:
      # sql instance patch outputs information to stderr
      # Reference to GCP documentation
      # https://cloud.google.com/sdk/gcloud/reference/sql/instances/patch
      # Example output
      # Updated [https://sqladmin.googleapis.com/].
      if 'Updated' in stderr:
        return
      raise RuntimeError('Invalid flags: %s' % stderr)

    self._Reboot()

  def _Reboot(self):
    cmd_string = [self, 'sql', 'instances', 'restart', self.instance_id]
    cmd = util.GcloudCommand(*cmd_string)
    cmd.Issue()

    if not self._IsReady():
      raise RuntimeError('Instance could not be set to ready after reboot')

  def GetResourceMetadata(self):
    metadata = super().GetResourceMetadata()
    if relational_db.ENABLE_DATA_CACHE.value:
      metadata['db_flags'] = metadata.get('db_flags', []) + [
          'enable-data-cache'
      ]
    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    if engine not in DEFAULT_ENGINE_VERSIONS:
      raise NotImplementedError(
          'Default engine not specified for engine {}'.format(engine)
      )
    return DEFAULT_ENGINE_VERSIONS[engine]

  @staticmethod
  def _GetEngineVersionString(engine, version):
    """Returns CloudSQL-specific version string for givin database engine.

    Args:
      engine: database engine
      version: engine version

    Returns:
      (string): CloudSQL-specific name for requested engine and version.

    Raises:
      NotImplementedError on invalid engine / version combination.
    """
    if engine not in GCP_DATABASE_VERSION_MAPPING:
      valid_databases = ', '.join(GCP_DATABASE_VERSION_MAPPING.keys())
      raise NotImplementedError(
          'Database {} is not supported,supported databases include {}'.format(
              engine, valid_databases
          )
      )

    version_mapping = GCP_DATABASE_VERSION_MAPPING[engine]
    if version not in version_mapping:
      valid_versions = ', '.join(version_mapping.keys())
      raise NotImplementedError(
          'Version {} is not supported,supported versions include {}'.format(
              version, valid_versions
          )
      )

    return version_mapping[version]

  def _FailoverHA(self):
    """Fail over from master to replica."""
    cmd_string = [
        self,
        'sql',
        'instances',
        'failover',
        self.instance_id,
    ]
    cmd = util.GcloudCommand(*cmd_string)
    cmd.flags['project'] = self.project
    # this command doesnt support the specifier: 'format'
    del cmd.flags['format']
    cmd.IssueRetryable()
