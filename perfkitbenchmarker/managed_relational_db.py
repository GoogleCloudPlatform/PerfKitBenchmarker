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
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

# TODO (ferneyhough): change to enum
flags.DEFINE_string('database', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('database_username', None,
                    'Database username. Defaults to '
                    'pkb-db-user-[run-uri]')
flags.DEFINE_string('database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
# TODO (ferneyhough): write a validator
flags.DEFINE_string('database_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
flags.DEFINE_boolean('high_availability', False,
                     'Specifies if the database should be high availability')
MYSQL = 'mysql'
POSTGRES = 'postgres'

_MANAGED_RELATIONAL_DB_REGISTRY = {}
FLAGS = flags.FLAGS


# TODO: Implement DEFAULT BACKUP_START_TIME for instances.


def generateRandomDbPassword():
  """Generate a random password 10 characters in length."""
  return str(uuid.uuid4())[:10]


def GetManagedRelationalDbClass(cloud):
  """Get the ManagedRelationalDb class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for
  """
  if cloud in _MANAGED_RELATIONAL_DB_REGISTRY:
    return _MANAGED_RELATIONAL_DB_REGISTRY.get(cloud)
  raise Exception('No ManagedDb found for {0}'.format(cloud))


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

  @abstractmethod
  def GetEndpoint(self):
    """Return the endpoint of the managed database.

    Returns:
      database endpoint (IP or dns name)
    """
    pass

  @abstractmethod
  def GetPort(self):
    """Return the port of the managed database.

    Returns:
      database port number
    """
    pass

  @abstractmethod
  def GetDefaultDatabaseVersion(self, database):
    """Return the default version (for PKB) for the given database.

    Args:
      database: name of the database

    Returns: default database version as a string for the given database.
  """

  def GetUsername(self):
    """Return the username associated with the managed database.

    Returns:
      database username
    """
    return self.spec.database_username

  def GetPassword(self):
    """Return the password associated with the managed database.

    Returns:
      database password
    """
    return self.spec.database_password
