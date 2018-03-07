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
"""Module containing class for cloud Redis."""

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


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


def GetCloudRedisClass(cloud):
  """Gets the cloud Redis class corresponding to 'cloud'.

  Args:
    cloud: String. name of cloud to get the class for.

  Returns:
    Implementation class corresponding to the argument cloud

  Raises:
    Exception: An invalid cloud was provided
  """
  return resource.GetResourceClass(BaseCloudRedis, CLOUD=cloud)


class BaseCloudRedis(resource.BaseResource):
  """Object representing a cloud redis."""

  RESOURCE_TYPE = 'BaseCloudRedis'

  def __init__(self, cloud_redis_spec):
    """Initialize the cloud redis object.

    Args:
      cloud_redis_spec: spec of the cloud redis.
    """
    super(BaseCloudRedis, self).__init__()
    self.spec = cloud_redis_spec
    self.failover_style = FLAGS.redis_failover_style

  def GetResourceMetadata(self):
    """Returns a dictionary of cluster metadata."""
    metadata = {
        'failover_style': self.failover_style
    }
    return metadata
