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

"""Module containing classes for Azure Redis Cache.
"""

import json
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

FLAGS = flags.FLAGS
# 15min timeout for issuing az redis delete command.
DELETE_TIMEOUT = 900
REDIS_VERSION = '3.2'


class AzureRedisCache(managed_memory_store.BaseManagedMemoryStore):
  """Object representing an Azure Redis Cache."""

  CLOUD = providers.AZURE
  MEMORY_STORE = managed_memory_store.REDIS

  def __init__(self, spec):
    super(AzureRedisCache, self).__init__(spec)
    self.redis_region = FLAGS.redis_region
    self.resource_group = azure_network.GetResourceGroup(self.redis_region)
    self.azure_tier = FLAGS.azure_tier
    self.azure_redis_size = FLAGS.azure_redis_size
    self.failover_style = FLAGS.redis_failover_style

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cache.

    Returns:
      dict mapping string property key to value.
    """
    result = {
        'cloud_redis_failover_style': self.failover_style,
        'cloud_redis_region': self.redis_region,
        'cloud_redis_azure_tier': self.azure_tier,
        'cloud_redis_azure_redis_size': self.azure_redis_size,
        'cloud_redis_azure_version': REDIS_VERSION,
    }
    return result

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    """Check benchmark prerequisites on the input flag parameters.

    Args:
      benchmark_config: Unused.

    Raises:
      errors.Config.InvalidValue: Input flag parameters are invalid.
    """
    if FLAGS.managed_memory_store_version:
      raise errors.Config.InvalidValue(
          'Custom Redis version not supported on Azure Redis. '
          'Redis version is {0}.'.format(REDIS_VERSION))
    if FLAGS.redis_failover_style in [
        managed_memory_store.Failover.FAILOVER_SAME_REGION,
        managed_memory_store.Failover.FAILOVER_SAME_ZONE]:
      raise errors.Config.InvalidValue(
          'Azure redis with failover is not yet available.')

  def _Create(self):
    """Creates the cache."""
    cmd = [
        azure.AZURE_PATH, 'redis', 'create',
        '--resource-group', self.resource_group.name,
        '--location', self.redis_region,
        '--name', self.name,
        '--sku', self.azure_tier,
        '--vm-size', self.azure_redis_size,
        '--enable-non-ssl-port',
    ]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the cache."""
    cmd = [
        azure.AZURE_PATH, 'redis', 'delete',
        '--resource-group', self.resource_group.name,
        '--name', self.name,
        '--yes',
    ]
    vm_util.IssueCommand(cmd, timeout=DELETE_TIMEOUT)

  def DescribeCache(self):
    """Calls show on the cache to get information about it.

    Returns:
      stdout, stderr and retcode.
    """
    stdout, stderr, retcode = vm_util.IssueCommand([
        azure.AZURE_PATH, 'redis', 'show',
        '--resource-group', self.resource_group.name,
        '--name', self.name,
    ])
    return stdout, stderr, retcode

  def _Exists(self):
    """Returns True if the cache exists.

    Returns:
      True if cache exists and false otherwise.
    """
    _, _, retcode = self.DescribeCache()
    return retcode == 0

  def _IsReady(self):
    """Returns True if the cache is ready.

    Returns:
      True if cache is ready and false otherwise.
    """
    stdout, _, retcode = self.DescribeCache()
    if (retcode == 0 and
        json.loads(stdout).get('provisioningState', None) == 'Succeeded'):
      return True
    return False

  def GetMemoryStorePassword(self):
    """See base class."""
    if not self._password:
      self._PopulateEndpoint()
    return self._password

  @vm_util.Retry(max_retries=5)
  def _PopulateEndpoint(self):
    """Populates endpoint information for the instance.

    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on cache.
    """
    stdout, _, retcode = self.DescribeCache()
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on %s.', self.name)
    response = json.loads(stdout)
    self._ip = response['hostName']
    self._port = response['port']

    stdout, _, retcode = vm_util.IssueCommand([
        azure.AZURE_PATH, 'redis', 'list-keys',
        '--resource-group', self.resource_group.name,
        '--name', self.name,
    ])
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on %s.', self.name)
    response = json.loads(stdout)
    self._password = response['primaryKey']
