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
import dataclasses
import re
from typing import Optional
from absl import flags
from absl import logging
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine

# List of memory store types
REDIS = 'REDIS'
MEMCACHED = 'MEMCACHED'

_REDIS_SHARDS_REGEX = r'(?s)slots\n(\d+)\n(\d+).+?port\n(\d+)\nip\n(\S+)'

FLAGS = flags.FLAGS


class Failover(object):
  """Enum for redis failover options."""

  FAILOVER_NONE = 'failover_none'
  FAILOVER_SAME_ZONE = 'failover_same_zone'
  FAILOVER_SAME_REGION = 'failover_same_region'


flags.DEFINE_enum(
    'redis_failover_style',
    Failover.FAILOVER_NONE,
    [
        Failover.FAILOVER_NONE,
        Failover.FAILOVER_SAME_ZONE,
        Failover.FAILOVER_SAME_REGION,
    ],
    (
        'Failover behavior of cloud redis cluster. Acceptable values are:'
        'failover_none, failover_same_zone, and failover_same_region'
    ),
)

# List of redis versions
REDIS_3_2 = 'redis_3_2'
REDIS_4_0 = 'redis_4_0'
REDIS_5_0 = 'redis_5_0'
REDIS_6_X = 'redis_6_x'
REDIS_7_X = 'redis_7_x'
REDIS_VERSIONS = [REDIS_3_2, REDIS_4_0, REDIS_5_0, REDIS_6_X, REDIS_7_X]

flags.DEFINE_string(
    'managed_memory_store_version',
    None,
    (
        'The version of managed memory store to use. This flag '
        'overrides Redis or Memcached version defaults that is set '
        'in benchmark config. Defaults to None so that benchmark '
        'config defaults are used.'
    ),
)
_MANAGED_MEMORY_STORE_CLUSTER = flags.DEFINE_bool(
    'managed_memory_store_cluster',
    False,
    'If True, provisions a cluster instead of a standalone instance.',
)
_NODE_COUNT = flags.DEFINE_integer(
    'managed_memory_store_node_count',
    1,
    (
        'Number of cache nodes (shards) to use. Only used if '
        'managed_memory_store_cluster is True.'
    ),
)
_ZONES = flags.DEFINE_list(
    'cloud_redis_zones',
    [],
    'If using cluster mode, the zones to distribute shards between.',
)
flags.DEFINE_string(
    'cloud_redis_region',
    'us-central1',
    (
        'The region to spin up cloud redis in.'
        'Defaults to the GCP region of us-central1.'
    ),
)
_TLS = flags.DEFINE_bool(
    'cloud_redis_tls', False, 'Whether to enable TLS on the instance.'
)

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
  return resource.GetResourceClass(
      BaseManagedMemoryStore, CLOUD=cloud, MEMORY_STORE=memory_store
  )


def ParseReadableVersion(version: str) -> str:
  """Parses Redis major and minor version number.

  Used for Azure and AWS versions.

  Args:
    version: String. Version string to get parsed.

  Returns:
    Parsed version
  """
  if version.count('.') < 1:
    logging.info(
        (
            'Could not parse version string correctly,'
            'full Redis version returned: %s'
        ),
        version,
    )
    return version
  return '.'.join(version.split('.', 2)[:2])


@dataclasses.dataclass
class RedisShard:
  """An object representing a Redis shard.

  Attributes:
    slots: formatted like 2731-5461
    ip: address of the redis shard
    port: port of the redis shard
    zone: location where the shard is located
  """

  slots: str
  ip: str
  port: int
  zone: Optional[str] = None


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
    self.name: str = 'pkb-%s' % FLAGS.run_uri
    self._ip: str = None
    self._port: int = None
    self._password: str = None
    self._clustered: bool = _MANAGED_MEMORY_STORE_CLUSTER.value
    self.node_count = _NODE_COUNT.value if self._clustered else 1
    self.zones = _ZONES.value if self._clustered else []
    self.enable_tls = _TLS.value

    self.metadata['clustered'] = self._clustered
    self.metadata['node_count'] = self.node_count
    self.metadata['enable_tls'] = self.enable_tls

  def GetMemoryStoreIp(self) -> str:
    """Returns the Ip address of the managed memory store."""
    if not self._ip:
      self._PopulateEndpoint()
    return self._ip

  def GetMemoryStorePort(self) -> int:
    """Returns the port number of the managed memory store."""
    if not self._port:
      self._PopulateEndpoint()
    return self._port

  def GetShardEndpoints(
      self, client: virtual_machine.BaseVirtualMachine
  ) -> list[RedisShard]:
    """Returns shard endpoints for the cluster.

    The format of the `cluster shards` command can be found here:
    https://redis.io/commands/cluster-shards/.

    Args:
      client: VM that has access to the redis cluster.

    Returns:
      A list of redis shards.
    """
    result, _ = client.RemoteCommand(
        f'redis-cli -h {self.GetMemoryStoreIp()} -p'
        f' {self.GetMemoryStorePort()} cluster shards'
    )
    shards = re.findall(_REDIS_SHARDS_REGEX, result)
    return [
        RedisShard(slots=f'{slot_begin}-{slot_end}', ip=ip, port=int(port))
        for slot_begin, slot_end, port, ip in shards
    ]

  @abc.abstractmethod
  def _PopulateEndpoint(self) -> None:
    """Populates the endpoint information for the managed memory store."""
    raise NotImplementedError()

  def GetMemoryStorePassword(self) -> str:
    """Returns the access password of the managed memory store, if any."""
    return self._password

  def MeasureCpuUtilization(self) -> Optional[float]:
    """Measures the CPU utilization of an instance using the cloud's API."""
    raise NotImplementedError()

  def GetInstanceSize(self) -> int:
    """Returns size of instance in gigabytes."""
    raise NotImplementedError()
