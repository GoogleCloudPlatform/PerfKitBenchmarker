# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS' elasticache redis clusters.

Clusters can be created and deleted.
"""
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker.providers.aws import aws_elasticache_cluster

FLAGS = flags.FLAGS
REDIS_VERSION_MAPPING = {
    'redis_3_2': '3.2.10',
    'redis_4_0': '4.0.10',
    'redis_5_0': '5.0.6',
    'redis_6_x': '6.x',
    'redis_7_x': '7.1',
}


class ElastiCacheRedis(aws_elasticache_cluster.ElastiCacheCluster):
  """Object representing a AWS Elasticache redis instance."""

  MEMORY_STORE = managed_memory_store.REDIS

  # AWS Clusters can take up to 2 hours to create
  READY_TIMEOUT = 120 * 60

  def __init__(self, spec):
    super().__init__(spec)
    self.version = REDIS_VERSION_MAPPING[spec.version]

  def CheckPrerequisites(self):
    if managed_memory_store.FAILOVER_STYLE.value in [
        managed_memory_store.Failover.FAILOVER_NONE,
        managed_memory_store.Failover.FAILOVER_SAME_ZONE,
    ]:
      if self.failover_zone:
        raise errors.Config.InvalidValue(
            'The aws_elasticache_failover_zone flag is ignored. '
            'There is no need for a failover zone when there is no failover. '
            'Same zone failover will fail over to the same zone.'
        )
    else:
      if not self.failover_zone or self.failover_zone[:-1] != self.redis_region:
        raise errors.Config.InvalidValue(
            'Invalid failover zone. A failover zone in'
            f' {self.redis_region} must be specified. '
        )

  def _GetEngine(self) -> str:
    """Returns the engine name."""
    return 'redis'
