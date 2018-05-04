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
"""Resource encapsulating provisioned cloud NFS services.

Defines a resource for use in other benchmarks such as SpecSFS2014 and FIO.

Example --benchmark_config_file:

nfs_10_tb: &nfs_10_tb
  AWS:
    disk_type: nfs
    mount_point: /scratch

specsfs:
  name: specsfs2014
  flags:
    specsfs2014_num_runs: 1
    specsfs2014_load: 1
  vm_groups:
    clients:
      disk_spec: *nfs_10_tb
      vm_count: 1
      os_type: rhel
    gluster_servers:
      vm_count: 0
"""

import abc
import logging
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

flags.DEFINE_string('nfs_tier', None, 'NFS Mode')
flags.DEFINE_string('nfs_version', None, 'NFS Version')

FLAGS = flags.FLAGS

_MOUNT_NFS_RE = re.compile(r'.*type nfs \((.*?)\)', re.MULTILINE)


def GetNfsServiceClass(cloud):
  """Get the NFS service corresponding to the cloud.

  Args:
    cloud: The name of the cloud to supply the NFS service.

  Returns:
    The NFS service class for this cloud.

  Raises:
    NotImplementedError: No service found for this cloud.
  """
  return resource.GetResourceClass(BaseNfsService, CLOUD=cloud)


class BaseNfsService(resource.BaseResource):
  """Object representing an NFS Service."""

  # subclasses must override this with a list or tuple for acceptable
  # "nfs_tier" values if applicable.
  NFS_TIERS = None
  RESOURCE_TYPE = 'BaseNfsService'
  DEFAULT_NFS_VERSION = None
  DEFAULT_TIER = None
  VOLUME_NAME = ''

  def __init__(self, disk_spec, zone):
    super(BaseNfsService, self).__init__()
    self.disk_spec = disk_spec
    self.zone = zone
    self.nfs_tier = FLAGS.nfs_tier or self.DEFAULT_TIER
    if self.nfs_tier and self.NFS_TIERS and self.nfs_tier not in self.NFS_TIERS:
      # NFS service does not have to have a list of nfs_tiers nor does it have
      # to be implemented by a provider
      raise errors.Config.InvalidValue(
          ('nfs_tier "%s" not in acceptable list "%s" '
           'for cloud %s') % (self.nfs_tier, self.NFS_TIERS, self.CLOUD))
    logging.info('%s NFS service with nfs_tier %s zone %s default version %s',
                 self.CLOUD, self.nfs_tier, self.zone,
                 self.DEFAULT_NFS_VERSION)

  def CreateNfsDisk(self):
    mount_point = '%s:/%s' % (self.GetRemoteAddress(), self.VOLUME_NAME)
    return disk.NfsDisk(self.disk_spec, mount_point, self.DEFAULT_NFS_VERSION,
                        self.nfs_tier)

  @abc.abstractmethod
  def _IsReady(self):
    """Boolean function to determine if disk is NFS mountable."""
    pass

  @abc.abstractmethod
  def GetRemoteAddress(self):
    """The NFS server's address."""
    pass
