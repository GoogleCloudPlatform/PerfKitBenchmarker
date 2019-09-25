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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import json
import logging
import time
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util
from six.moves import range

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '5.7'
DEFAULT_POSTGRES_VERSION = '9.6'
DEFAULT_GCP_MYSQL_VERSION = 'MYSQL_5_7'
DEFAULT_GCP_POSTGRES_VERSION = 'POSTGRES_9_6'

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432

# PostgreSQL restrictions on memory.
# Source: https://cloud.google.com/sql/docs/postgres/instance-settings.
CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND = 0.9
CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND = 6.5
MIN_CUSTOM_MACHINE_MEM_MB = 3840

IS_READY_TIMEOUT = 600  # 10 minutes
CREATION_TIMEOUT = 600  # 10 minutes


class UnsupportedDatabaseEngineException(Exception):
  pass


class GCPRelationalDb(relational_db.BaseRelationalDb):
  """A GCP CloudSQL database resource.

  This class contains logic required to provision and teardown the database.
  Currently, the database will be open to the world (0.0.0.0/0) which is not
  ideal; however, a password is still required to connect. Currently only
  MySQL 5.7 and Postgres 9.6 are supported.
  """
  CLOUD = providers.GCP
  GCP_PRICING_PLAN = 'PACKAGE'

  def __init__(self, relational_db_spec):
    super(GCPRelationalDb, self).__init__(relational_db_spec)
    self.project = FLAGS.project or util.GetDefaultProject()
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri

    self.unmanaged_db_exists = None if self.is_managed_db else False

  def _GetAuthorizedNetworks(self, vms):
    """Get CIDR connections for list of VM specs that need to access the db."""
    for vm in vms:
      if not vm.HasIpAddress:
        raise Exception('Client vm needs to be initialized before database can '
                        'discover authorized network.')
    # create the CIDR of the client VM that is configured to access
    # the database
    return ','.join('{0}/32'.format(vm.ip_address) for vm in vms)

  def _CreateGcloudSqlInstance(self):
    storage_size = self.spec.db_disk_spec.disk_size
    instance_zone = self.spec.db_spec.zone

    authorized_network = self._GetAuthorizedNetworks([self.client_vm])

    database_version_string = self._GetEngineVersionString(
        self.spec.engine, self.spec.engine_version)

    cmd_string = [
        self,
        'beta',
        'sql',
        'instances',
        'create',
        self.instance_id,
        '--quiet',
        '--format=json',
        '--activation-policy=ALWAYS',
        '--assign-ip',
        '--authorized-networks=%s' % authorized_network,
        '--zone=%s' % instance_zone,
        '--database-version=%s' % database_version_string,
        '--pricing-plan=%s' % self.GCP_PRICING_PLAN,
        '--storage-size=%d' % storage_size,
    ]
    if self.spec.engine == relational_db.MYSQL:
      cmd_string.append('--enable-bin-log')

    if (self.spec.db_spec.cpus and self.spec.db_spec.memory):
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
      raise Exception('Unspecified machine type')

    if self.spec.high_availability:
      cmd_string.append(self._GetHighAvailabilityFlag())

    if self.spec.backup_enabled:
      cmd_string.append('--backup')
      cmd_string.append('--backup-start-time={}'.format(
          self.spec.backup_start_time))
    else:
      cmd_string.append('--no-backup')
    cmd = util.GcloudCommand(*cmd_string)
    cmd.flags['project'] = self.project

    _, _, _ = cmd.Issue(timeout=CREATION_TIMEOUT)

    if FLAGS.mysql_flags:
      cmd_string = [
          self, 'sql', 'instances', 'patch', self.instance_id,
          '--database-flags=%s' % ','.join(FLAGS.mysql_flags)
      ]
      cmd = util.GcloudCommand(*cmd_string)
      _, stderr, _ = cmd.Issue()
      if stderr:
        raise Exception('Invalid MySQL flags: %s' % stderr)

  def _Create(self):
    """Creates the Cloud SQL instance and authorizes traffic from anywhere.

    Raises:
      UnsupportedDatabaseEngineException:
        if the database is unmanaged and the engine isn't MYSQL.
      Exception: if an invalid MySQL flag was used.
    """
    if self.spec.engine == relational_db.MYSQL:
      self._InstallMySQLClient()
    if self.is_managed_db:
      self._CreateGcloudSqlInstance()
    else:
      self.endpoint = self.server_vm.ip_address
      if self.spec.engine == relational_db.MYSQL:
        self._InstallMySQLServer()
      else:
        raise UnsupportedDatabaseEngineException(
            'Engine {0} not supported for unmanaged databases.'.format(
                self.spec.engine))
      self.firewall = gce_network.GceFirewall()
      self.firewall.AllowPort(
          self.server_vm, 3306, source_range=[self.client_vm.ip_address])
      self.unmanaged_db_exists = True
      self._ApplyMySqlFlags()

  def _GetHighAvailabilityFlag(self):
    """Returns a flag that enables high-availability for the specified engine.

    Returns:
      Flag (as string) to be appended to the gcloud sql create command.

    Raises:
      UnsupportedDatabaseEngineException:
        if engine does not support high availability.
    """
    if self.spec.engine == relational_db.MYSQL:
      self.replica_instance_id = 'replica-' + self.instance_id
      return '--failover-replica-name=' + self.replica_instance_id
    elif self.spec.engine == relational_db.POSTGRES:
      return '--availability-type=REGIONAL'
    else:
      raise UnsupportedDatabaseEngineException(
          'High availability not supported on engine {0}'.format(
              self.spec.engine))

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
          'details about size restrictions.')
    if not hasattr(self.spec.db_spec, 'memory') or not self.spec.db_spec.memory:
      raise data.ResourceNotFound(
          'Must specify a memory amount in benchmark config. See https://'
          'cloud.google.com/sql/docs/postgres/instance-settings for more '
          'details about size restrictions.')

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
    if cpus not in [1] + list(range(2, 32, 2)):
      raise ValueError(
          'CPUs (%i) much be 1 or an even number in-between 2 and 32, '
          'inclusive.' % cpus)

    if memory % 256 != 0:
      raise ValueError(
          'Total memory (%dMiB) for a custom machine must be a multiple'
          'of 256MiB.' % memory)
    ratio = memory / 1024.0 / cpus
    if (ratio < CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND or
        ratio > CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND):
      raise ValueError(
          'The memory (%.2fGiB) per vCPU (%d) of a custom machine '
          'type must be between %.2f GiB and %.2f GiB per vCPU, '
          'inclusive.' %
          (memory / 1024.0, cpus, CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND,
           CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND))
    if memory < MIN_CUSTOM_MACHINE_MEM_MB:
      raise ValueError('The total memory (%dMiB) for a custom machine type'
                       'must be at least %dMiB.' %
                       (memory,
                        MIN_CUSTOM_MACHINE_MEM_MB))

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
      return
    if hasattr(self, 'replica_instance_id'):
      cmd = util.GcloudCommand(self, 'sql', 'instances', 'delete',
                               self.replica_instance_id, '--quiet')
      cmd.Issue(raise_on_failure=False)

    cmd = util.GcloudCommand(self, 'sql', 'instances', 'delete',
                             self.instance_id, '--quiet')
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    if not self.is_managed_db:
      return self.unmanaged_db_exists
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe',
                             self.instance_id)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    try:
      json_output = json.loads(stdout)
      return json_output['kind'] == 'sql#instance'
    except:
      return False

  def _IsDBInstanceReady(self, instance_id, timeout=IS_READY_TIMEOUT):
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe',
                             instance_id)
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds > timeout:
        logging.exception('Timeout waiting for sql instance to be ready')
        return False
      stdout, _, _ = cmd.Issue(suppress_warning=True, raise_on_failure=False)

      try:
        json_output = json.loads(stdout)
        state = json_output['state']
        logging.info('Instance %s state: %s', instance_id, state)
        if state == 'RUNNABLE':
          break
      except:
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
    if not self.is_managed_db:
      return self._IsReadyUnmanaged()

    if not self._IsDBInstanceReady(self.instance_id, timeout):
      return False
    if self.spec.high_availability and hasattr(self, 'replica_instance_id'):
      if not self._IsDBInstanceReady(self.replica_instance_id, timeout):
        return False

    cmd = util.GcloudCommand(
        self, 'sql', 'instances', 'describe', self.instance_id)
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    self.endpoint = self._ParseEndpoint(json_output)
    self.port = DEFAULT_MYSQL_PORT
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
    except:
      selflink = ''
      logging.exception('Error attempting to read stdout. Creation failure.')
    return selflink

  def _PostCreate(self):
    """Creates the PKB user and sets the password.
    """
    if not self.is_managed_db:
      return

    # The hostname '%' means unrestricted access from any host.
    cmd = util.GcloudCommand(
        self, 'sql', 'users', 'create', self.spec.database_username,
        '--host=%', '--instance={0}'.format(self.instance_id),
        '--password={0}'.format(self.spec.database_password))
    _, _, _ = cmd.Issue()

    # this is a fix for b/71594701
    # by default the empty password on 'postgres'
    # is a security violation.  Change the password to a non-default value.
    if self.spec.engine == relational_db.POSTGRES:
      cmd = util.GcloudCommand(
          self, 'sql', 'users', 'set-password', 'postgres',
          '--host=dummy_host', '--instance={0}'.format(self.instance_id),
          '--password={0}'.format(self.spec.database_password))
      _, _, _ = cmd.Issue()

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    if engine == relational_db.MYSQL:
      return DEFAULT_MYSQL_VERSION
    elif engine == relational_db.POSTGRES:
      return DEFAULT_POSTGRES_VERSION

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
    if engine == relational_db.MYSQL:
      if version == DEFAULT_MYSQL_VERSION:
        return DEFAULT_GCP_MYSQL_VERSION
      elif version == '5.6':
        return 'MYSQL_5_6'
    elif engine == relational_db.POSTGRES:
      if version == DEFAULT_POSTGRES_VERSION:
        return DEFAULT_GCP_POSTGRES_VERSION
    raise NotImplementedError('GCP managed databases only support MySQL 5.7 and'
                              'POSTGRES 9.6')

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
