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

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


# List of memory store types
REDIS = 'REDIS'


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
REDIS_3_2 = 'REDIS_3_2'
REDIS_VERSIONS = [REDIS_3_2]


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
