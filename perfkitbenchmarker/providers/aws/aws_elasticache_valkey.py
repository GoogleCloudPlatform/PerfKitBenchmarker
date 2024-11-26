# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS' Elasticache Valkey clusters."""
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker.providers.aws import aws_elasticache_cluster

FLAGS = flags.FLAGS

_DEFAULT_VERSION = '8.0'
_VALKEY_VERSION_MAPPING = {
    'VALKEY_7_2': '7.2',
    'VALKEY_8_0': '8.0',
}


class ElastiCacheValkey(aws_elasticache_cluster.ElastiCacheCluster):
  """Object representing a AWS Elasticache valkey instance."""

  MEMORY_STORE = managed_memory_store.VALKEY

  def __init__(self, spec):
    super().__init__(spec)
    self.version = _VALKEY_VERSION_MAPPING[spec.version] or _DEFAULT_VERSION

  def CheckPrerequisites(self):
    if (
        managed_memory_store.FAILOVER_STYLE.value
        != managed_memory_store.Failover.FAILOVER_NONE
        or self.failover_zone
    ):
      raise errors.Config.InvalidValue(
          'The Valkey implementation currently does not support failover.'
      )

  def _GetEngine(self) -> str:
    """Returns the engine name."""
    return 'valkey'
