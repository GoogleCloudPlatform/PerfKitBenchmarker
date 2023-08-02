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

"""Relational DB class.

This is the base implementation of all relational db.
"""

import abc
import re
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sql_engine_utils
import six

# TODO(chunla): Move IAAS flag to file
flags.DEFINE_string('db_engine', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('db_engine_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
flags.DEFINE_string('database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('database_username', None,
                    'Database username. Defaults to '
                    'pkb-db-user-[run-uri]')
flags.DEFINE_string('database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
flags.DEFINE_boolean('db_high_availability', False,
                     'Specifies if the database should be high availability')
flags.DEFINE_boolean('db_backup_enabled', True,
                     'Whether or not to enable automated backups')
flags.DEFINE_string('db_backup_start_time', '07:00',
                    'Time in UTC that automated backups (if enabled) '
                    'will be scheduled. In the form HH:MM UTC. '
                    'Defaults to 07:00 UTC')
flags.DEFINE_list('db_zone', None,
                  'zone or region to launch the database in. '
                  'Defaults to the client vm\'s zone.')
flags.DEFINE_string('client_vm_zone', None,
                    'zone or region to launch the client in. ')
flags.DEFINE_string('db_machine_type', None,
                    'Machine type of the database.')
flags.DEFINE_integer('db_cpus', None,
                     'Number of Cpus in the database.')
flags.DEFINE_string('db_memory', None,
                    'Amount of Memory in the database.  Uses the same format '
                    'string as custom machine memory type.')
flags.DEFINE_integer('db_disk_size', None,
                     'Size of the database disk in GB.')
flags.DEFINE_integer('db_num_striped_disks', None,
                     'The number of data disks to stripe together to form one.')
flags.DEFINE_string('db_disk_type', None, 'Disk type of the database.')
flags.DEFINE_integer('managed_db_disk_iops', None,
                     'Disk iops of the database on AWS io1 disks.')

flags.DEFINE_integer('managed_db_azure_compute_units', None,
                     'Number of Dtus in the database.')
flags.DEFINE_string('managed_db_tier', None,
                    'Tier in azure. (Basic, Standard, Premium).')
flags.DEFINE_string('server_vm_os_type', None,
                    'OS type of the client vm.')
flags.DEFINE_string('client_vm_os_type', None,
                    'OS type of the client vm.')
flags.DEFINE_string('server_gcp_min_cpu_platform', None,
                    'Cpu platform of the server vm.')
flags.DEFINE_string('client_gcp_min_cpu_platform', None,
                    'CPU platform of the client vm.')
flags.DEFINE_string('client_vm_machine_type', None,
                    'Machine type of the client vm.')
flags.DEFINE_integer('client_vm_cpus', None, 'Number of Cpus in the client vm.')
flags.DEFINE_string(
    'client_vm_memory', None,
    'Amount of Memory in the vm.  Uses the same format '
    'string as custom machine memory type.')
flags.DEFINE_integer('client_vm_disk_size', None,
                     'Size of the client vm disk in GB.')
flags.DEFINE_string('client_vm_disk_type', None, 'Disk type of the client vm.')
flags.DEFINE_integer('client_vm_disk_iops', None,
                     'Disk iops of the database on AWS for client vm.')
flags.DEFINE_boolean(
    'use_managed_db', True, 'If true, uses the managed MySql '
    'service for the requested cloud provider. If false, uses '
    'MySql installed on a VM.')
flags.DEFINE_list(
    'db_flags', '', 'Flags to apply to the managed relational database '
    'on the cloud that\'s being used. Example: '
    'binlog_cache_size=4096,innodb_log_buffer_size=4294967295')
flags.DEFINE_integer(
    'innodb_buffer_pool_size', None,
    'Size of the innodb buffer pool size in GB. '
    'Defaults to 25% of VM memory if unset')

flags.DEFINE_bool(
    'mysql_bin_log', False,
    'Flag to turn binary logging on. '
    'Defaults to False')
flags.DEFINE_integer('innodb_log_file_size', 1000,
                     'Size of the log file in MB. Defaults to 1000M.')
flags.DEFINE_integer(
    'postgres_shared_buffer_size', None,
    'Size of the shared buffer size in GB. '
    'Defaults to 25% of VM memory if unset')

OPTIMIZE_DB_SYSCTL_CONFIG = flags.DEFINE_bool(
    'optimize_db_sysctl_config', True,
    'Flag to run sysctl optimization for IAAS relational database.')

SERVER_GCE_NUM_LOCAL_SSDS = flags.DEFINE_integer(
    'server_gce_num_local_ssds', 0,
    'The number of ssds that should be added to the Server.')
SERVER_GCE_SSD_INTERFACE = flags.DEFINE_enum(
    'server_gce_ssd_interface', 'SCSI', ['SCSI', 'NVME'],
    'The ssd interface for GCE local SSD.')


BACKUP_TIME_REGULAR_EXPRESSION = r'^\d\d\:\d\d$'
flags.register_validator(
    'db_backup_start_time',
    lambda value: re.search(BACKUP_TIME_REGULAR_EXPRESSION, value) is not None,
    message=('--database_backup_start_time must be in the form HH:MM'))


FLAGS = flags.FLAGS


DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_SQLSERVER_PORT = 1433

DEFAULT_PORTS = {
    sql_engine_utils.MYSQL: DEFAULT_MYSQL_PORT,
    sql_engine_utils.POSTGRES: DEFAULT_POSTGRES_PORT,
    sql_engine_utils.SQLSERVER: DEFAULT_SQLSERVER_PORT,
}


class RelationalDbPropertyNotSetError(Exception):
  pass


class RelationalDbEngineNotFoundError(Exception):
  pass


class UnsupportedError(Exception):
  pass


def GetRelationalDbClass(cloud, is_managed_db, engine):
  """Get the RelationalDb class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for
    is_managed_db: is the database self managed or database a a service
    engine: database engine type

  Returns:
    BaseRelationalDb class with the cloud attribute of 'cloud'.
  """
  try:
    return resource.GetResourceClass(
        BaseRelationalDb, CLOUD=cloud, IS_MANAGED=is_managed_db, ENGINE=engine)
  except errors.Resource.SubclassNotFoundError:
    pass

  return resource.GetResourceClass(
      BaseRelationalDb, CLOUD=cloud, IS_MANAGED=is_managed_db)


def VmsToBoot(vm_groups):
  # TODO(user): Enable replications.
  return {
      name: spec  # pylint: disable=g-complex-comprehension
      for name, spec in six.iteritems(vm_groups)
      if name == 'clients' or name == 'default' or
      (not FLAGS.use_managed_db and name == 'servers')
  }


class BaseRelationalDb(resource.BaseResource):
  """Object representing a relational database Service."""
  IS_MANAGED = True
  RESOURCE_TYPE = 'BaseRelationalDb'
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED']

  def __init__(self, relational_db_spec):
    """Initialize the managed relational database object.

    Args:
      relational_db_spec: spec of the managed database.
    """
    super(BaseRelationalDb, self).__init__(
        enable_freeze_restore=relational_db_spec.enable_freeze_restore,
        create_on_restore_error=relational_db_spec.create_on_restore_error,
        delete_on_freeze_error=relational_db_spec.delete_on_freeze_error)
    self.spec = relational_db_spec
    self.engine = self.spec.engine
    self.engine_type = sql_engine_utils.GetDbEngineType(self.engine)
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri
    self.port = self.GetDefaultPort()
    self.is_managed_db = self.IS_MANAGED
    self.endpoint = ''
    self.replica_endpoint = ''
    self.client_vms = []

  @property
  def client_vm(self):
    """Client VM which will drive the database test.

    This is required by subclasses to perform client-vm
    network-specific tasks, such as getting information about
    the VPC, IP address, etc.

    Raises:
      RelationalDbPropertyNotSetError: if the client_vm is missing.

    Returns:
      The client_vm.
    """
    if not hasattr(self, '_client_vm'):
      raise RelationalDbPropertyNotSetError('client_vm is not set')
    return self._client_vm

  # TODO(user): add support for multiple client VMs
  @client_vm.setter
  def client_vm(self, client_vm):
    self._client_vm = client_vm

  def _GetDbConnectionProperties(
      self,
  ) -> sql_engine_utils.DbConnectionProperties:
    return sql_engine_utils.DbConnectionProperties(
        self.spec.engine,
        self.spec.engine_version,
        self.endpoint,
        self.port,
        self.spec.database_username,
        self.spec.database_password,
    )

  # TODO(user): Deprecate in favor of client_vms_query_tools
  @property
  def client_vm_query_tools(self):
    return self.client_vms_query_tools[0]

  @property
  def client_vms_query_tools(self) -> list[sql_engine_utils.ISQLQueryTools]:
    connection_properties = self._GetDbConnectionProperties()
    tools = [
        sql_engine_utils.GetQueryToolsByEngine(vm, connection_properties)
        for vm in self.client_vms
    ]
    return [t for t in tools if t is not None]

  @property
  def client_vm_query_tools_for_replica(self):
    """Query tools to make custom queries on replica."""
    if not self.replica_endpoint:
      raise ValueError('Database does not have replica.')
    connection_properties = sql_engine_utils.DbConnectionProperties(
        self.spec.engine,
        self.spec.engine_version,
        self.replica_endpoint,
        self.port,
        self.spec.database_username,
        self.spec.database_password,
    )
    return sql_engine_utils.GetQueryToolsByEngine(
        self.client_vm, connection_properties
    )

  def SetVms(self, vm_groups):
    self.client_vms = vm_groups[
        'clients' if 'clients' in vm_groups else 'default'
    ]
    # TODO(user): Remove this after moving to multiple client VMs.
    self.client_vm = self.client_vms[0]

  @property
  def port(self):
    """Port (int) on which the database server is listening."""
    if not hasattr(self, '_port'):
      raise RelationalDbPropertyNotSetError('port not set')
    return self._port

  @port.setter
  def port(self, port):
    self._port = int(port)

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata.

    Child classes can extend this if needed.

    Raises:
       RelationalDbPropertyNotSetError: if any expected metadata is missing.
    """
    metadata = {
        'zone': self.spec.db_spec.zone,
        'disk_type': self.spec.db_disk_spec.disk_type,
        'disk_size': self.spec.db_disk_spec.disk_size,
        'engine': self.spec.engine,
        'high_availability': self.spec.high_availability,
        'backup_enabled': self.spec.backup_enabled,
        'backup_start_time': self.spec.backup_start_time,
        'engine_version': self.spec.engine_version,
        'client_vm_zone': self.spec.vm_groups['clients'].vm_spec.zone,
        'use_managed_db': self.is_managed_db,
        'instance_id': self.instance_id,
        'client_vm_disk_type':
            self.spec.vm_groups['clients'].disk_spec.disk_type,
        'client_vm_disk_size':
            self.spec.vm_groups['clients'].disk_spec.disk_size,
    }

    if (hasattr(self.spec.db_spec, 'machine_type') and
        self.spec.db_spec.machine_type):
      metadata.update({
          'machine_type': self.spec.db_spec.machine_type,
      })
    elif hasattr(self.spec.db_spec, 'cpus') and (hasattr(
        self.spec.db_spec, 'memory')):
      metadata.update({
          'cpus': self.spec.db_spec.cpus,
      })
      metadata.update({
          'memory': self.spec.db_spec.memory,
      })
    elif hasattr(self.spec.db_spec, 'tier') and (
        hasattr(self.spec.db_spec, 'compute_units')
    ):
      metadata.update({
          'tier': self.spec.db_spec.tier,
      })
      metadata.update({
          'compute_units': self.spec.db_spec.compute_units,
      })
    else:
      raise RelationalDbPropertyNotSetError(
          'Machine type of the database must be set.')

    if (hasattr(self.spec.vm_groups['clients'].vm_spec, 'machine_type') and
        self.spec.vm_groups['clients'].vm_spec.machine_type):
      metadata.update({
          'client_vm_machine_type':
              self.spec.vm_groups['clients'].vm_spec.machine_type,
      })
    elif hasattr(self.spec.vm_groups['clients'].vm_spec, 'cpus') and (hasattr(
        self.spec.vm_groups['clients'].vm_spec, 'memory')):
      metadata.update({
          'client_vm_cpus': self.spec.vm_groups['clients'].vm_spec.cpus,
      })
      metadata.update({
          'client_vm_memory': self.spec.vm_groups['clients'].vm_spec.memory,
      })
    else:
      raise RelationalDbPropertyNotSetError(
          'Machine type of the client VM must be set.')

    if FLAGS.db_flags:
      metadata.update({
          'db_flags': FLAGS.db_flags,
      })

    if hasattr(self.spec, 'db_tier'):
      metadata.update({
          'db_tier': self.spec.db_tier,
      })

    return metadata

  @abc.abstractmethod
  def GetDefaultEngineVersion(self, engine):
    """Return the default version (for PKB) for the given database engine.

    Args:
      engine: name of the database engine

    Returns: default version as a string for the given engine.
    """

  def _SetEndpoint(self):
    """Set the DB endpoint for this instance during _PostCreate."""
    pass

  def _PostCreate(self):
    if self.spec.db_flags:
      self._ApplyDbFlags()
    self._SetEndpoint()
    background_tasks.RunThreaded(
        lambda client_query_tools: client_query_tools.InstallPackages(),
        self.client_vms_query_tools,
    )

  def UpdateCapacityForLoad(self) -> None:
    """Updates infrastructure to the correct capacity for loading."""
    pass

  def UpdateCapacityForRun(self) -> None:
    """Updates infrastructure to the correct capacity for running."""
    pass

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    raise NotImplementedError('Managed Db flags is not supported for %s' %
                              type(self).__name__)

  def GetDefaultPort(self):
    """Returns default port for the db engine from the spec."""
    engine = sql_engine_utils.GetDbEngineType(self.spec.engine)
    if engine not in DEFAULT_PORTS:
      raise NotImplementedError('Default port not specified for '
                                'engine {0}'.format(engine))
    return DEFAULT_PORTS[engine]

  def CreateDatabase(self, database_name: str) -> tuple[str, str]:
    """Creates the database."""
    return self.client_vms_query_tools[0].CreateDatabase(database_name)

  def DeleteDatabase(self, database_name: str) -> tuple[str, str]:
    """Deletes the database."""
    return self.client_vms_query_tools[0].DeleteDatabase(database_name)

  def Failover(self):
    """Fail over the database.  Throws exception if not high available."""
    if not self.spec.high_availability:
      raise RuntimeError(
          "Attempt to fail over a database that isn't marked as high available"
      )
    self._FailoverHA()

  def _FailoverHA(self):
    """Fail over from master to replica."""
    raise NotImplementedError('Failover is not implemented.')
