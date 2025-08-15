# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Resource encapsulating provisioned cloud Lustre services.

Defines a resource for use in other benchmarks such as io500.
"""

import abc
import logging

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource

flags.DEFINE_integer('lustre_tier', None, 'Lustre bandwidth tier.')
flags.DEFINE_integer('lustre_capacity_gib', None, 'Lustre capacity in GiB.')

FLAGS = flags.FLAGS


def GetLustreServiceClass(cloud):
  """Get Lustre class corresponding to the cloud.

  Args:
    cloud: The name of the cloud to supply the NFS service.

  Returns:
    The Lustre service class for this cloud.

  Raises:
    NotImplementedError: No service found for this cloud.
  """
  return resource.GetResourceClass(BaseLustreService, CLOUD=cloud)


class BaseLustreService(resource.BaseResource):
  """Object representing an Lustre Service."""

  # subclasses must override this with a list or tuple for acceptable
  # "lustre_tier" values if applicable.
  CLOUD = 'Unknown'
  LUSTRE_TIERS = None
  RESOURCE_TYPE = 'BaseLustreService'
  DEFAULT_TIER = None
  DEFAULT_CAPACITY = None

  def __init__(self, disk_spec: disk.BaseLustreDiskSpec, zone):
    super().__init__()
    self.disk_spec = disk_spec
    self.zone = zone
    self.lustre_tier = FLAGS.lustre_tier or self.DEFAULT_TIER
    self.capacity = FLAGS.lustre_capacity_gib or self.DEFAULT_CAPACITY
    if (
        self.lustre_tier
        and self.LUSTRE_TIERS
        and self.lustre_tier not in self.LUSTRE_TIERS
    ):
      # Lustre service does not have to have a list of lustre_tiers nor does it
      # have to be implemented by a provider
      raise errors.Config.InvalidValue(
          'lustre_tier "%s" not in acceptable list "%s" for cloud %s'
          % (self.lustre_tier, self.LUSTRE_TIERS, self.CLOUD)
      )
    logging.debug(
        '%s Lustre service with lustre_tier %s zone %s',
        self.CLOUD,
        self.lustre_tier,
        self.zone,
    )
    self.metadata.update(
        {'lustre_tier': self.lustre_tier, 'lustre_capacity_gib': self.capacity}
    )

  def CreateLustreDisk(self):
    lustre_disk = disk.LustreDisk(self.disk_spec)
    lustre_disk.metadata.update(self.metadata)
    return lustre_disk

  @abc.abstractmethod
  def _IsReady(self):
    """Boolean function to determine if disk is Lustre mountable."""
    pass

  @abc.abstractmethod
  def GetMountPoint(self):
    """The Lustre mount point."""
    pass
