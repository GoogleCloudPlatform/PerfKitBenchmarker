# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing classes related to SoftLayer disks.
Currently 2 disks are attached to the VM at create"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import providers

LOCAL = 'local'

DISK_TYPE = {
    disk.LOCAL: LOCAL,

}

DISK_METADATA = {
    LOCAL: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
#        disk.LEGACY_DISK_TYPE: disk.STANDARD
        disk.STANDARD: disk.STANDARD
    }
}

SoftLayer = 'SoftLayer'
disk.RegisterDiskTypeMap(SoftLayer, DISK_TYPE)


class SoftLayerDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create a SoftLayerDisk."""

  CLOUD = providers.SOFTLAYER

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """

    super(SoftLayerDiskSpec, cls)._ApplyFlags(config_values, flag_values)


class SoftLayerDisk(disk.BaseDisk):
  """Object representing an SoftLayer Disk."""

  def __init__(self, disk_spec, zone, machine_type):
    super(SoftLayerDisk, self).__init__(disk_spec)
    self.id = None
    self.zone = zone
    self.device_letter = 'c'
    self.attached_vm_id = None
    self.metadata = DISK_METADATA[self.disk_type]

  def _Create(self):
    """Creates the disk."""
    # Currently all disks are local or SAN created with the VM

  def _Delete(self):
    """Deletes the disk."""
    # Currently all disks are local or SAN created with the VM

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The SoftLayer VM instance to which the disk will be attached.
    """
    # Currently all disks are local or SAN created with the VM

  def Detach(self):
    """Detaches the disk from a VM."""
    # Currently all disks are local or SAN created with the VM

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    return '/dev/xvd%s' % self.device_letter
