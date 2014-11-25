# Copyright 2014 Google Inc. All rights reserved.
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

"""Module containing abstract classes related to disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
"""

import abc

from perfkitbenchmarker import resource


class BaseDiskSpec(object):
  """Storing type and size about a disk."""

  def __init__(self, disk_size, disk_type, mount_point):
    """Storing various data about a single disk.

    Args:
      disk_size: Size of the disk in GB.
      disk_type: Disk types in string. See cloud specific disk classes for
          more information on acceptable values.
      mount_point: Directory of mount point in string.

    """
    self.disk_size = disk_size
    self.disk_type = disk_type
    self.mount_point = mount_point


class BaseDisk(resource.BaseResource):
  """Object representing a Base Disk."""

  def __init__(self, disk_spec):
    super(BaseDisk, self).__init__()
    self.disk_size = disk_spec.disk_size
    self.disk_type = disk_spec.disk_type
    self.mount_point = disk_spec.mount_point

  @abc.abstractmethod
  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The BaseVirtualMachine instance to which the disk will be attached.
    """
    pass

  @abc.abstractmethod
  def Detach(self):
    """Detaches the disk from a VM."""
    pass

  @abc.abstractmethod
  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    pass
