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

STANDARD = 'standard'
REMOTE_SSD = 'remote_ssd'
PIOPS = 'piops'  # Provisioned IOPS (SSD) in AWS
LOCAL = 'local'


class BaseDiskSpec(object):
  """Storing type and size about a disk."""

  def __init__(self, disk_size, disk_type,
               mount_point, iops=None, num_striped_disks=1):
    """Storing various data about a single disk.

    Args:
      disk_size: Size of the disk in GB.
      disk_type: Disk types in string. See cloud specific disk classes for
          more information on acceptable values.
      mount_point: Directory of mount point in string.
      num_striped_disks: The number of disks to stripe together. If this is 1,
          it means no striping will occur. This must be >= 1.
    """
    self.disk_size = disk_size
    self.disk_type = disk_type
    self.mount_point = mount_point
    self.iops = iops
    assert num_striped_disks >= 1, ('Disk objects must correspond to at least '
                                    '1 real disk.')
    self.num_striped_disks = num_striped_disks


class BaseDisk(resource.BaseResource):
  """Object representing a Base Disk."""

  is_striped = False

  def __init__(self, disk_spec):
    super(BaseDisk, self).__init__()
    self.disk_size = disk_spec.disk_size
    self.disk_type = disk_spec.disk_type
    self.mount_point = disk_spec.mount_point
    self.iops = disk_spec.iops

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


class StripedDisk(BaseDisk):
  """Object representing several disks striped together."""

  is_striped = True

  def __init__(self, disk_spec, disks, device_path):
    """Initializes a StripedDisk object.

    Args:
      disk_spec: A BaseDiskSpec containing the desired mount point.
      disks: A list of BaseDisk objects that constitute the StripedDisk.
      device_path: The path of the striped device.
    """
    super(StripedDisk, self).__init__(disk_spec)
    self.disks = disks
    self.device_path = device_path

  def _Create(self):
    for disk in self.disks:
      disk.Create()

  def _Delete(self):
    for disk in self.disks:
      disk.Delete()

  def Attach(self, vm):
    for disk in self.disks:
      disk.Attach(vm)

  def Detach(self):
    for disk in self.disks:
      disk.Detach()

  def GetDevicePath(self):
    return self.device_path
