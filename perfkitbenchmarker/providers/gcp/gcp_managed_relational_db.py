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
To run this benchmark for GCP it is required to install a non-default gcloud
component. Otherwise this benchmark will fail.
To ensure that gcloud beta is installed, type
        'gcloud components list'
into the terminal. This will output all components and status of each.
Make sure that
  name: gcloud Beta Commands
  id:  beta
has status: Installed.
If not, run
        'gcloud components install beta'
to install it. This will allow this benchmark to properly create an instance.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import data
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '5.7'
DEFAULT_POSTGRES_VERSION = '9.6'
DEFAULT_GCP_MYSQL_VERSION = 'MYSQL_5_7'
DEFAULT_GCP_POSTGRES_VERSION = 'POSTGRES_9_6'

# PostgreSQL restrictions on memory.
# Source: https://cloud.google.com/sql/docs/postgres/instance-settings.
CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND = 0.9
CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND = 6.5
MIN_CUSTOM_MACHINE_MEM_MB = 3840


class GCPManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
  """An object representing a GCP managed relational database.

  Attributes:
    created: True if the resource has been created.
    pkb_managed: Whether the resource is managed (created and deleted) by PKB.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'managed_relational_db'
  # These are the constants that should be specified in GCP's cloud SQL command.
  GCP_PRICING_PLAN = 'PACKAGE'
  MYSQL_DEFAULT_PORT = 3306

  def __init__(self, managed_relational_db_spec):
    super(GCPManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.project = FLAGS.project or util.GetDefaultProject()
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri

  def _Create(self):
    """Creates the GCP Cloud SQL instance."""
    storage_size = self.spec.disk_spec.disk_size
    instance_zone = self.spec.vm_spec.zone

    # TODO: Close authorized networks to VM once spec is updated so client
    # VM is child of managed_relational_db. Then client VM ip address will be
    # available from managed_relational_db_spec.
    authorized_network = '0.0.0.0/0'
    cloudsql_specific_database_version = self._GetDatabaseVersionNameFromFlavor(
        self.spec.database, self.spec.database_version)
    # Please install gcloud component beta for this to work. See note in
    # module level docstring.
    # This is necessary only because creating a SQL instance with a non-default
    # storage size is in beta right now in gcloud. This can be removed when
    # this feature is part of the default components. See
    # https://cloud.google.com/sdk/gcloud/reference/beta/sql/instances/create
    # for more information. When this flag is allowed in the default gcloud
    # components the create_db_cmd below can be updated.
    cmd_string = [
        self,
        'beta',
        'sql',
        'instances',
        'create',
        self.instance_id,
        '--quiet',
        '--format=json',
        '--async',
        '--activation-policy=ALWAYS',
        '--assign-ip',
        '--authorized-networks=%s' % authorized_network,
        '--enable-bin-log',
        '--gce-zone=%s' % instance_zone,
        '--database-version=%s' % cloudsql_specific_database_version,
        '--pricing-plan=%s' % self.GCP_PRICING_PLAN,
        '--storage-size=%d' % storage_size
    ]
    if self.spec.database == managed_relational_db.MYSQL:
      machine_type_flag = '--tier=%s' % self.spec.vm_spec.machine_type
    else:
      self._ValidateSpec()
      memory = self.spec.vm_spec.memory
      cpus = self.spec.vm_spec.cpus
      self._ValidateMachineType(memory, cpus)
      machine_type_flag = ('--cpu={} --ram={}'.format(cpus, memory))
    cmd_string.append(machine_type_flag)
    if self.spec.high_availability:
      ha_flag = '--failover-replica-name=replica-' + self.instance_id
      cmd_string.append(ha_flag)
    if self.spec.backup_enabled:
      cmd_string.append('--backup')
      cmd_string.append('--backup-start-time={}'.format(
          self.spec.backup_start_time))
    else:
      cmd_string.append('--no-backup')
    cmd = util.GcloudCommand(*cmd_string)
    cmd.flags['project'] = self.project

    _, _, _ = cmd.Issue()

  def _ValidateSpec(self):
    """Validate postgreSQL spec for CPU and memory.

    Raises:
      data.ResourceNotFound: On missing memory or cpus in postgres benchmark
                              config.
    """
    if not hasattr(self.spec.vm_spec, 'cpus'):
      raise data.ResourceNotFound(
          'Must specify cpu count in benchmark config. See https://'
          'cloud.google.com/sql/docs/postgres/instance-settings for more '
          'details about size restrictions.')
    if not hasattr(self.spec.vm_spec, 'memory'):
      raise data.ResourceNotFound(
          'Must specify a memory amount in benchmark config. See https://'
          'cloud.google.com/sql/docs/postgres/instance-settings for more '
          'details about size restrictions.')

  def _ValidateMachineType(self, memory, cpus):
    """ validated machine configurations.

    Args:
      memory: (int) in MiB.
      cpus: (int).

    Raises:
      ValueError.
    """
    if cpus not in [1] + range(2, 32, 2):
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
      raise ValueError('The memory (%.2fGiB) per vCPU (%d) of a custom machine '
                       'type must be between %.2f GiB and %.2f GiB per vCPU, '
                       'inclusive.' %
                       (memory / 1024.0,
                        cpus,
                        CUSTOM_MACHINE_CPU_MEM_RATIO_LOWER_BOUND,
                        CUSTOM_MACHINE_CPU_MEM_RATIO_UPPER_BOUND)
                       )
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
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'delete',
                             self.instance_id, '--quiet')
    cmd.Issue()

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe',
                             self.instance_id)
    stdout, _, _ = cmd.Issue()
    try:
      json_output = json.loads(stdout)
      return json_output['kind'] == 'sql#instance'
    except:
      return False

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    cmd = util.GcloudCommand(self, 'sql', 'instances', 'describe',
                             self.instance_id)
    stdout, _, _ = cmd.Issue()
    try:
      json_output = json.loads(stdout)
      if not json_output['state'] == 'RUNNABLE':
        return False
    except:
      logging.exception('Error attempting to read stdout. Creation failure.')
      return False
    self.endpoint = self._ParseEndpoint(json_output)
    self.port = self.MYSQL_DEFAULT_PORT
    return True

  def _ParseEndpoint(self, describe_instance_json):
    """Return the URI of the resource given the metadata as JSON.

    Args:
      describe_instance_json: JSON output.
    Returns:
      resource URI (string)
    """
    try:
      selflink = describe_instance_json['selfLink']
    except:
      selflink = ''
      logging.exception('Error attempting to read stdout. Creation failure.')
    return selflink

  def _PostCreate(self):
    """Method that will be called once after _CreateReource is called.

    Supplying this method is optional. If it is supplied, it will be called
    once, after the resource is confirmed to exist. It is intended to allow
    data about the resource to be collected or for the resource to be tagged.
    """
    pass

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    pass

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    pass

  @staticmethod
  def GetDefaultDatabaseVersion(database):
    """Returns the default version of a given database.

    Args:
      database (string): type of database (my_sql or postgres).
    Returns:
      (string): Default database version.
    """
    if database == managed_relational_db.MYSQL:
      return DEFAULT_MYSQL_VERSION
    elif database == managed_relational_db.POSTGRES:
      return DEFAULT_POSTGRES_VERSION

  @staticmethod
  def _GetDatabaseVersionNameFromFlavor(flavor, version):
    """Returns internal name for database type.

    Args:
      flavor:
      version:
    Returns:
      (string): Internal name for database type.
    """
    if flavor == managed_relational_db.MYSQL:
      if version == DEFAULT_MYSQL_VERSION:
        return DEFAULT_GCP_MYSQL_VERSION
    elif flavor == managed_relational_db.POSTGRES:
      if version == DEFAULT_POSTGRES_VERSION:
        return DEFAULT_GCP_POSTGRES_VERSION
    raise NotImplementedError('GCP managed databases only support MySQL 5.7 and'
                              'POSTGRES 9.6')

  def GetEndpoint(self):
    """Return the endpoint of the managed database.

    Returns:
      database endpoint (IP or dns name)
    """
    return self.endpoint

  def GetPort(self):
    """Return the port of the managed database.

    Returns:
      database port number
    """
    return self.port
