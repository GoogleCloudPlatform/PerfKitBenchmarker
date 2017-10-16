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


from abc import ABCMeta, abstractmethod
import re
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

# TODO (ferneyhough): change to enum
flags.DEFINE_string('managed_db_engine', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('managed_db_engine_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
flags.DEFINE_string('managed_db_database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('managed_db_database_username', None,
                    'Database username. Defaults to '
                    'pkb-db-user-[run-uri]')
flags.DEFINE_string('managed_db_database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
flags.DEFINE_boolean('managed_db_high_availability', False,
                     'Specifies if the database should be high availability')
flags.DEFINE_boolean('managed_db_backup_enabled', True,
                     'Whether or not to enable automated backups')
flags.DEFINE_string('managed_db_backup_start_time', '07:00',
                    'Time in UTC that automated backups (if enabled) '
                    'will be scheduled. In the form HH:MM UTC. '
                    'Defaults to 07:00 UTC')
flags.DEFINE_string('managed_db_zone', None,
                    'zone or region to launch the database in. '
                    'Defaults to the client vm\'s zone.')

BACKUP_TIME_REGULAR_EXPRESSION = '^\d\d\:\d\d$'
flags.register_validator(
    'managed_db_backup_start_time',
    lambda value: re.search(BACKUP_TIME_REGULAR_EXPRESSION, value) is not None,
    message=('--database_backup_start_time must be in the form HH:MM'))

MYSQL = 'mysql'
POSTGRES = 'postgres'
AURORA_POSTGRES = 'aurora-postgresql'

_MANAGED_RELATIONAL_DB_REGISTRY = {}
FLAGS = flags.FLAGS

# TODO: Implement DEFAULT BACKUP_START_TIME for instances.


class ManagedRelationalDbPropertyNotSet(Exception):
  pass


def GenerateRandomDbPassword():
  """Generate a random password 10 characters in length."""
  return str(uuid.uuid4())[:10]


def GetManagedRelationalDbClass(cloud):
  """Get the ManagedRelationalDb class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for
  """
  if cloud in _MANAGED_RELATIONAL_DB_REGISTRY:
    return _MANAGED_RELATIONAL_DB_REGISTRY.get(cloud)
  raise NotImplementedError(('No ManagedRelationalDatabase implementation '
                             'found for {0}'.format(cloud)))


class AutoRegisterManagedRelationalDbMeta(ABCMeta):
  """Metaclass which allows ManagedRelationalDb to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('ManagedRelationalDb subclasses must '
                        'have a CLOUD' 'attribute.')
      else:
        _MANAGED_RELATIONAL_DB_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterManagedRelationalDbMeta, cls).__init__(name, bases, dct)


class BaseManagedRelationalDb(resource.BaseResource):
  """Object representing a managed relational database Service."""

  __metaclass__ = AutoRegisterManagedRelationalDbMeta

  def __init__(self, managed_relational_db_spec):
    """Initialize the managed relational database object

    Args:
      managed_relational_db_spec: spec of the managed database
    """
    super(BaseManagedRelationalDb, self).__init__()
    self.spec = managed_relational_db_spec

  @property
  def client_vm(self):
    """Client VM which will drive the database test.

    This is required by subclasses to perform client-vm
    network-specific tasks, such as getting information about
    the VPC, IP address, etc.
    """
    if not hasattr(self, '_client_vm'):
      raise ManagedRelationalDbPropertyNotSet('client_vm is not set')
    return self._client_vm

  @client_vm.setter
  def client_vm(self, client_vm):
    self._client_vm = client_vm

  def MakePsqlConnectionString(self, database_name):
    return '\'host={0} user={1} password={2} dbname={3}\''.format(
        self.endpoint,
        self.spec.database_username,
        self.spec.database_password,
        database_name)

  @property
  def endpoint(self):
    """Endpoint of the database server (exclusing port)."""
    if not hasattr(self, '_endpoint'):
      raise ManagedRelationalDbPropertyNotSet('endpoint not set')
    return self._endpoint

  @endpoint.setter
  def endpoint(self, endpoint):
    self._endpoint = endpoint

  @property
  def port(self):
    """Port (int) on which the database server is listening."""
    if not hasattr(self, '_port'):
      raise ManagedRelationalDbPropertyNotSet('port not set')
    return self._port

  @port.setter
  def port(self, port):
    self._port = int(port)

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata.

   Child classes can extend this if needed.
   """
    metadata = {
        'zone': self.spec.vm_spec.zone,
        'disk_type': self.spec.disk_spec.disk_type,
        'disk_size': self.spec.disk_spec.disk_size,
        'engine': self.spec.engine,
        'high_availability': self.spec.high_availability,
        'backup_enabled': self.spec.backup_enabled,
        'backup_start_time': self.spec.backup_start_time,
        'engine_version': self.spec.engine_version,
    }
    if self.spec.vm_spec.machine_type:
      metadata.update({
          'machine_type': self.spec.vm_spec.machine_type,
      })
    else:
      metadata.update({
          'cpus': self.spec.vm_spec.cpus,
          'memory': self.spec.vm_spec.memory,
      })

    return metadata

  @abstractmethod
  def GetDefaultEngineVersion(self, engine):
    """Return the default version (for PKB) for the given database engine.

    Args:
      engine: name of the database engine

    Returns: default version as a string for the given engine.
  """
