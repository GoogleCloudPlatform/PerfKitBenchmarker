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
"""Module containing class for GCP's cloud redis instances.

Instances can be created and deleted.
"""
import json
import logging

from perfkitbenchmarker import cloud_redis
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
STANDARD_TIER = 'STANDARD_HA'
BASIC_TIER = 'BASIC'


class CloudRedis(cloud_redis.BaseCloudRedis):
  """Object representing a GCP cloud redis instance."""

  CLOUD = providers.GCP

  def __init__(self, spec):
    super(CloudRedis, self).__init__(spec)
    self.project = FLAGS.project
    self.size = FLAGS.gcp_redis_gb
    self.redis_region = FLAGS.redis_region
    self.failover_style = FLAGS.redis_failover_style
    if self.failover_style == cloud_redis.Failover.FAILOVER_NONE:
      self.tier = BASIC_TIER
    elif self.failover_style == cloud_redis.Failover.FAILOVER_SAME_REGION:
      self.tier = STANDARD_TIER

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    if FLAGS.redis_failover_style == cloud_redis.Failover.FAILOVER_SAME_ZONE:
      raise errors.Config.InvalidValue(
          'GCP cloud redis does not support same zone failover')

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    result = super(CloudRedis, self).GetResourceMetadata()
    result['size'] = self.size
    result['tier'] = self.tier
    result['region'] = self.redis_region
    return result

  def _Create(self):
    """Creates the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'redis', 'instances', 'create',
                             self.spec.redis_name)
    cmd.flags['region'] = self.redis_region
    cmd.flags['zone'] = self.spec.client_vm.zone
    cmd.flags['network'] = FLAGS.gce_network_name
    cmd.flags['tier'] = self.tier
    cmd.flags['size'] = self.size
    cmd.Issue()

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'redis', 'instances', 'delete',
                             self.spec.redis_name)
    cmd.flags['region'] = self.redis_region
    cmd.Issue()

  def _Exists(self):
    """Returns true if the instance exists."""
    _, _, retcode = self.DescribeInstance()
    return retcode == 0

  def DescribeInstance(self):
    """Calls describe instance using the gcloud tool.

    Returns:
      stdout, stderr, and retcode.
    """
    cmd = util.GcloudCommand(self, 'beta', 'redis', 'instances', 'describe',
                             self.spec.redis_name)
    cmd.flags['region'] = self.redis_region
    stdout, stderr, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      logging.info('Could not find redis instance %s', self.spec.redis_name)
    return stdout, stderr, retcode

  @vm_util.Retry(max_retries=5)
  def GetInstanceDetails(self):
    """Returns a dict containing details about the instance.

    Returns:
      dict mapping string property key to value.
    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on instance
    """
    stdout, _, retcode = self.DescribeInstance()
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on {}'.format(self.spec.redis_name))
    return json.loads(stdout)
