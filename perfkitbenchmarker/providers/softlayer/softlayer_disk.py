# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

Disks can be created, deleted, attached to VMs, and detached from VMs.
"""

import json
import logging
import string
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.softlayer import util

VOLUME_EXISTS_STATUSES = frozenset(['creating', 'available', 'in-use', 'error'])
VOLUME_DELETED_STATUSES = frozenset(['deleting', 'deleted'])
VOLUME_KNOWN_STATUSES = VOLUME_EXISTS_STATUSES | VOLUME_DELETED_STATUSES

LOCAL = 'local'


DISK_TYPE = {
    disk.LOCAL: LOCAL,
    
}

DISK_METADATA = {
    LOCAL: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
        disk.LEGACY_DISK_TYPE: disk.STANDARD
    }
}

LOCAL_SSD_METADATA = {
    disk.MEDIA: disk.SSD,
    disk.REPLICATION: disk.NONE,
    disk.LEGACY_DISK_TYPE: disk.LOCAL
}

LOCAL_HDD_METADATA = {
    disk.MEDIA: disk.HDD,
    disk.REPLICATION: disk.NONE,
    disk.LEGACY_DISK_TYPE: disk.LOCAL
}

LOCAL_HDD_PREFIXES = ['d2', 'hs']


def LocalDiskIsHDD(machine_type):
  """Check whether the local disks use spinning magnetic storage."""

  return True


SoftLayer = 'SoftLayer'
disk.RegisterDiskTypeMap(SoftLayer, DISK_TYPE)


class SoftLayerDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create an SoftLayerDisk.

  Attributes:
    iops: None or int. IOPS for Provisioned IOPS (SSD) volumes in SoftLayer.
  """

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
    # TODO: IOPS based storage
    #if flag_values['softlayer_provisioned_iops'].present:
    #  config_values['iops'] = 500 #flag_values.softLayer_provisioned_iops

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(SoftLayerDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({'iops': (option_decoders.IntDecoder, {'default': None,
                                                         'none_ok': True})})
    return result


class SoftLayerDisk(disk.BaseDisk):
  """Object representing an SoftLayer Disk."""

  _lock = threading.Lock()
  vm_devices = {}

  def __init__(self, disk_spec, zone, machine_type):
    super(SoftLayerDisk, self).__init__(disk_spec)
    self.iops = disk_spec.iops
    self.id = None
    self.zone = zone
    self.region = util.GetRegionFromZone(zone)
    self.device_letter = 'c'
    self.attached_vm_id = None

    if self.disk_type != disk.LOCAL:
      self.metadata = DISK_METADATA[self.disk_type]
    else:
      self.metadata = (LOCAL_HDD_METADATA
                       if LocalDiskIsHDD(machine_type)
                       else LOCAL_SSD_METADATA)

  def _Create(self):
    """Creates the disk."""
    logging.warn("create disk")
    

  def _Delete(self):
    """Deletes the disk."""
    print "delete disk"

  def _Exists(self):
    """Returns true if the disk exists."""
    logging.warn("exists ")
    return True


  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The SoftLayerVirtualMachine instance to which the disk will be attached.
    """
    logging.info("attach disk")
    vm.num_local_disks = vm.num_local_disks + 1

  def Detach(self):
    """Detaches the disk from a VM."""
    print "detach"

    

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      return '/dev/xvd%s' % self.device_letter
    else:
      return '/dev/xvdb%s' % self.device_letter
