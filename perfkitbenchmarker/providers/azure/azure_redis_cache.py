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

"""Module containing classes for Azure Redis Cache."""

import json
import time

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import flags as azure_flags

FLAGS = flags.FLAGS
# 15min timeout for issuing az redis delete command.
TIMEOUT = 900
EXISTS_RETRY_TIMES = 3
EXISTS_RETRY_POLL = 30

REDIS_VERSION_MAPPING = {
    'redis_4_0': '4.0',
    'redis_6_x': '6.0',
}


class AzureRedisCache(managed_memory_store.BaseManagedMemoryStore):
  """Object representing an Azure Redis Cache."""

  CLOUD = provider_info.AZURE
  SERVICE_TYPE = 'cache'
  MEMORY_STORE = managed_memory_store.REDIS

  # Azure redis could take up to an hour to create
  READY_TIMEOUT = 60 * 60  # 60 minutes

  def __init__(self, spec):
    super().__init__(spec)
    self.redis_region = managed_memory_store.REGION.value
    self.resource_group = azure_network.GetResourceGroup(self.redis_region)
    self.azure_redis_size = azure_flags.REDIS_SIZE.value
    if (
        self.failover_style
        == managed_memory_store.Failover.FAILOVER_SAME_REGION
    ):
      self.azure_tier = 'Premium'
    else:
      self.azure_tier = 'Basic'
    self.version = REDIS_VERSION_MAPPING[spec.version]

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cache.

    Returns:
      dict mapping string property key to value.
    """
    self.metadata.update({
        'cloud_redis_failover_style': self.failover_style,
        'cloud_redis_region': self.redis_region,
        'cloud_redis_azure_tier': self.azure_tier,
        'cloud_redis_azure_redis_size': self.azure_redis_size,
        'cloud_redis_version': self.GetReadableVersion(),
    })
    return self.metadata

  def CheckPrerequisites(self):
    """Check benchmark prerequisites on the input flag parameters.

    Raises:
      errors.Config.InvalidValue: Input flag parameters are invalid.
    """
    if managed_memory_store.FAILOVER_STYLE.value in [
        managed_memory_store.Failover.FAILOVER_SAME_ZONE
    ]:
      raise errors.Config.InvalidValue(
          'Azure redis with failover in the same zone is not supported.'
      )

  def _Create(self):
    """Creates the cache."""
    cmd = [
        azure.AZURE_PATH,
        'redis',
        'create',
        '--resource-group',
        self.resource_group.name,
        '--location',
        self.redis_region,
        '--name',
        self.name,
        '--sku',
        self.azure_tier,
        '--vm-size',
        self.azure_redis_size,
        '--enable-non-ssl-port',
        '--redis-version',
        self.version,
    ]
    vm_util.IssueCommand(cmd, timeout=TIMEOUT)

  def _Delete(self):
    """Deletes the cache."""
    cmd = [
        azure.AZURE_PATH,
        'redis',
        'delete',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.name,
        '--yes',
    ]
    vm_util.IssueCommand(cmd, timeout=TIMEOUT)

  def DescribeCache(self):
    """Calls show on the cache to get information about it.

    Returns:
      stdout, stderr and retcode.
    """
    stdout, stderr, retcode = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'redis',
            'show',
            '--resource-group',
            self.resource_group.name,
            '--name',
            self.name,
        ],
        raise_on_failure=False,
    )
    return stdout, stderr, retcode

  def _Exists(self):
    """Returns True if the cache exists.

    Returns:
      True if cache exists and false otherwise.
    """
    # Retry to ensure there is no transient error in describe cache
    for _ in range(EXISTS_RETRY_TIMES):
      _, _, retcode = self.DescribeCache()

      if retcode == 0:
        return True
      time.sleep(EXISTS_RETRY_POLL)
    return retcode == 0

  def _IsReady(self):
    """Returns True if the cache is ready.

    Returns:
      True if cache is ready and false otherwise.
    """
    stdout, _, retcode = self.DescribeCache()
    if (
        retcode == 0
        and json.loads(stdout).get('provisioningState', None) == 'Succeeded'
    ):
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
          f'Failed to retrieve information on {self.name}.'
      )
    response = json.loads(stdout)
    self._ip = response['hostName']
    self._port = response['port']

    stdout, _, retcode = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'redis',
            'list-keys',
            '--resource-group',
            self.resource_group.name,
            '--name',
            self.name,
        ],
        raise_on_failure=False,
    )
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          f'Failed to retrieve information on {self.name}.'
      )
    response = json.loads(stdout)
    self._password = response['primaryKey']
