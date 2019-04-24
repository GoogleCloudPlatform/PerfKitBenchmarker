# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for cloud managed memory stores."""

import abc
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


# List of memory store types
REDIS = 'REDIS'
MEMCACHED = 'MEMCACHED'


FLAGS = flags.FLAGS


class Failover(object):
  """Enum for redis failover options."""
  FAILOVER_NONE = 'failover_none'
  FAILOVER_SAME_ZONE = 'failover_same_zone'
  FAILOVER_SAME_REGION = 'failover_same_region'

flags.DEFINE_enum(
    'redis_failover_style',
    Failover.FAILOVER_NONE,
    [Failover.FAILOVER_NONE,
     Failover.FAILOVER_SAME_ZONE,
     Failover.FAILOVER_SAME_REGION],
    'Failover behavior of cloud redis cluster. Acceptable values are:'
    'failover_none, failover_same_zone, and failover_same_region')

# List of redis versions
REDIS_3_2 = 'redis_3_2'
REDIS_4_0 = 'redis_4_0'
REDIS_VERSIONS = [REDIS_3_2, REDIS_4_0]

flags.DEFINE_string('managed_memory_store_version',
                    None,
                    'The version of managed memory store to use. This flag '
                    'overrides Redis or Memcached version defaults that is set '
                    'in benchmark config. Defaults to None so that benchmark '
                    'config defaults are used.')

MEMCACHED_NODE_COUNT = 1


def GetManagedMemoryStoreClass(cloud, memory_store):
  """Gets the cloud managed memory store class corresponding to 'cloud'.

  Args:
    cloud: String. Name of cloud to get the class for.
    memory_store: String. Type of memory store to get the class for.

  Returns:
    Implementation class corresponding to the argument cloud

  Raises:
    Exception: An invalid cloud was provided
  """
  return resource.GetResourceClass(BaseManagedMemoryStore,
                                   CLOUD=cloud,
                                   MEMORY_STORE=memory_store)


class BaseManagedMemoryStore(resource.BaseResource):
  """Object representing a cloud managed memory store."""

  REQUIRED_ATTRS = ['CLOUD', 'MEMORY_STORE']
  RESOURCE_TYPE = 'BaseManagedMemoryStore'

  def __init__(self, spec):
    """Initialize the cloud managed memory store object.

    Args:
      spec: spec of the managed memory store.
    """
    super(BaseManagedMemoryStore, self).__init__()
    self.spec = spec
    self.name = 'pkb-%s' % FLAGS.run_uri
    self._ip = None
    self._port = None
    self._password = None

  def GetMemoryStoreIp(self):
    """Returns the Ip address of the managed memory store."""
    if not self._ip:
      self._PopulateEndpoint()
    return self._ip

  def GetMemoryStorePort(self):
    """Returns the port number of the managed memory store."""
    if not self._port:
      self._PopulateEndpoint()
    return self._port

  @abc.abstractmethod
  def _PopulateEndpoint(self):
    """Populates the endpoint information for the managed memory store."""
    raise NotImplementedError()

  def GetMemoryStorePassword(self):
    """Returns the access password of the managed memory store, if any."""
    return self._password
