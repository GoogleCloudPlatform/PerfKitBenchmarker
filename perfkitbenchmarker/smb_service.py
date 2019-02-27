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
"""Resource encapsulating provisioned cloud SMB services.

Defines a resource for use in other benchmarks such as SpecSFS2014 and FIO.

Example --benchmark_config_file:

smb_10_tb: &smb_10_tb
  Azure:
    disk_type: smb
    mount_point: /scratch

specsfs:
  name: specsfs2014
  flags:
    specsfs2014_num_runs: 1
    specsfs2014_load: 1
  vm_groups:
    clients:
      disk_spec: *smb_10_tb
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

flags.DEFINE_string('smb_tier', 'Standard', 'SMB Mode')
FLAGS = flags.FLAGS

_MOUNT_SMB_RE = re.compile(r'.*type smb \((.*?)\)', re.MULTILINE)


def GetSmbServiceClass(cloud):
  """Get the SMB service corresponding to the cloud.

  Args:
    cloud: The name of the cloud to supply the SMB service.

  Returns:
    The SMB service class for this cloud.

  Raises:
    NotImplementedError: No service found for this cloud.
  """
  return resource.GetResourceClass(BaseSmbService, CLOUD=cloud)


class BaseSmbService(resource.BaseResource):
  """Object representing an SMB Service."""

  # subclasses must override this with a list or tuple
  SMB_TIERS = None
  RESOURCE_TYPE = 'BaseSmbService'
  DEFAULT_SMB_VERSION = None
  VOLUME_NAME = ''

  def __init__(self, disk_spec, zone):
    super(BaseSmbService, self).__init__()
    self.disk_spec = disk_spec
    self.zone = zone
    self.smb_tier = FLAGS.smb_tier or self.DEFAULT_TIER
    if self.smb_tier and self.SMB_TIERS and self.smb_tier not in self.SMB_TIERS:
      # SMB service does not have to have a list of smb_tiers nor does it have
      # to be implemented by a provider
      raise errors.Config.InvalidValue(
          ('smb_tier "%s" not in acceptable list "%s" '
           'for cloud %s') % (self.smb_tier, self.SMB_TIERS, self.CLOUD))
    logging.debug('%s SMB service with smb_tier %s zone %s default version %s',
                  self.CLOUD, self.smb_tier, self.zone,
                  self.DEFAULT_SMB_VERSION)

  def CreateSmbDisk(self):
    return disk.SmbDisk(self.disk_spec, self.GetRemoteAddress(),
                        self.GetStorageAccountAndKey(),
                        self.DEFAULT_SMB_VERSION, self.smb_tier)

  @abc.abstractmethod
  def _IsReady(self):
    """Boolean function to determine if disk is SMB mountable."""
    pass

  @abc.abstractmethod
  def GetRemoteAddress(self):
    """The SMB server's address."""
    pass

  @abc.abstractmethod
  def GetStorageAccountAndKey(self):
    """The SMB server's storage account's name and key."""
    pass
