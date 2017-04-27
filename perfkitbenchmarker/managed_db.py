import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

flags.DEFINE_string('managed_db_flavor', None,
                    'Flavor (mysql, postgres) of the database')
flags.DEFINE_string('managed_db_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
MYSQL = 'mysql'
POSTGRES = 'postgres'

_MANAGED_DB_REGISTRY = {}
FLAGS = flags.FLAGS

def GetManagedDbClass(cloud):
  """Get the ManagedDb class corresponding to 'cloud'."""
  if cloud in _MANAGED_DB_REGISTRY:
    return _MANAGED_DB_REGISTRY.get(cloud)
  raise Exception('No ManagedDb found for {0}'.format(cloud))


class AutoRegisterManagedDbMeta(abc.ABCMeta):
  """Metaclass which allows ManagedDb to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('ManagedDb subclasses must have a CLOUD' 'attribute.')
      else:
        _MANAGED_DB_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterManagedDbMeta, cls).__init__(name, bases, dct)


class BaseManagedDb(resource.BaseResource):
  """Object representing a managed database Service."""

  __metaclass__ = AutoRegisterManagedDbMeta

  def __init__(self, managed_db_spec):
    """Initialize the managed database object 

    Args:
      managed_db_spec: spec of the managed database 
    """
    super(BaseManagedDb, self).__init__()
    self.spec = managed_db_spec
