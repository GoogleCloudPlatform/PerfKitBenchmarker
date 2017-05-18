from abc import ABCMeta

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

# TODO: change to enum
flags.DEFINE_string('database', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
# TODO: write a validator
flags.DEFINE_string('database_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
MYSQL = 'mysql'
POSTGRES = 'postgres'

_MANAGED_RELATIONAL_DB_REGISTRY = {}
FLAGS = flags.FLAGS


def GetManagedRelationalDbClass(cloud):
  """Get the ManagedRelationalDb class corresponding to 'cloud'."""
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
