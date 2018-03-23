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
from perfkitbenchmarker import cloud_redis
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

FLAGS = flags.FLAGS


class AzureRedisCache(cloud_redis.BaseCloudRedis):
  """Object representing an Azure Redis Cache."""

  CLOUD = providers.AZURE

  def __init__(self, spec):
    super(AzureRedisCache, self).__init__(spec)
    self.name = 'pkb-%s' % FLAGS.run_uri
    self.redis_region = FLAGS.redis_region
    self.resource_group = azure_network.GetResourceGroup(self.redis_region)
    self.azure_tier = FLAGS.azure_tier
    self.azure_redis_size = FLAGS.azure_redis_size

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cache.

    Returns:
      dict mapping string property key to value.
    """
    result = super(AzureRedisCache, self).GetResourceMetadata()
    result['region'] = self.redis_region
    result['azure_tier'] = self.azure_tier
    result['azure_redis_size'] = self.azure_redis_size
    return result

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    """Check benchmark prerequisites on the input flag parameters.

    Args:
      benchmark_config: Unused.

    Raises:
      errors.Config.InvalidValue: Input flag parameters are invalid.
    """
    if FLAGS.redis_failover_style in [cloud_redis.Failover.FAILOVER_SAME_REGION,
                                      cloud_redis.Failover.FAILOVER_SAME_ZONE]:
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
    ]
    vm_util.IssueCommand(cmd)

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

  @vm_util.Retry(max_retries=5)
  def GetInstanceDetails(self):
    """Returns a dict containing details about the instance.

    Returns:
      dict mapping string property key to value.
    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on cache.
    """
    instance_details = {}

    stdout, _, retcode = self.DescribeCache()
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on %s.', self.name)
    response = json.loads(stdout)
    instance_details['host'] = response['hostName']
    instance_details['port'] = response['port']

    stdout, _, retcode = vm_util.IssueCommand([
        azure.AZURE_PATH, 'redis', 'list-keys',
        '--resource-group', self.resource_group.name,
        '--name', self.name,
    ])
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on %s.', self.name)
    response = json.loads(stdout)
    instance_details['password'] = response['primaryKey']
    return instance_details
