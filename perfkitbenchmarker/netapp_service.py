# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Resource encapsulating provisioned cloud NetApp Volumes services."""

import abc
import logging

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util


def GetNetAppServiceClass(cloud):
  """Get the NetApp service class corresponding to the cloud."""
  return resource.GetResourceClass(BaseNetAppService, CLOUD=cloud)


class BaseNetAppService(resource.BaseResource):
  """Object representing a NetApp Service."""

  CLOUD = 'Unknown'
  RESOURCE_TYPE = 'BaseNetAppService'
  REQUIRED_ATTRS = ['CLOUD']
  DEFAULT_NFS_VERSION = '4.1'

  def __init__(self, disk_spec: disk.BaseNetAppDiskSpec, zone):
    super().__init__()
    self.disk_spec = disk_spec
    self.zone = zone
    self.server_directory = '/'
    logging.debug(
        '%s NetApp service with zone %s default version %s',
        self.CLOUD,
        self.zone,
        self.DEFAULT_NFS_VERSION,
    )

  def CreateNetAppDisk(self):
    mount_point = '%s:%s' % (self.GetRemoteAddress(), self.server_directory)
    return disk.NetAppDisk(
        self.disk_spec, mount_point, self.DEFAULT_NFS_VERSION
    )

  @abc.abstractmethod
  def _IsReady(self):
    """Boolean function to determine if volume is mountable."""
    pass

  @vm_util.Retry(timeout=300, retryable_exceptions=(errors.Error,))
  def GetRemoteAddress(self):
    """The NetApp volume server's address."""
    return self._GetRemoteAddress()

  @abc.abstractmethod
  def _GetRemoteAddress(self):
    """Subclasses implement provider-specific logic to get remote address."""
    pass
