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

"""Module containing abstract classes related to disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
"""

import abc
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

FLAGS = flags.FLAGS


# These are the (deprecated) old disk type names
STANDARD = 'standard'
REMOTE_SSD = 'remote_ssd'
PIOPS = 'piops'  # Provisioned IOPS (SSD) in AWS and Alicloud

# 'local' refers to disks that come attached to VMs. It is the only
# "universal" disk type that is not associated with a provider. It
# exists because we can provision a local disk without creating a disk
# spec. The Aerospike benchmarks use this fact in their config
# methods, and they need to be able to tell when a disk is local. So
# until that changes, 'local' is a special disk type.
LOCAL = 'local'

# Map old disk type names to new disk type names
DISK_TYPE_MAPS = dict()

# Standard metadata keys relating to disks
MEDIA = 'media'
REPLICATION = 'replication'
# And some possible values
HDD = 'hdd'
SSD = 'ssd'
NONE = 'none'
ZONE = 'zone'
REGION = 'region'
LEGACY_DISK_TYPE = 'legacy_disk_type'


# TODO(nlavine): remove this function when we remove the deprecated
# flags and disk type names.
def RegisterDiskTypeMap(provider_name, type_map):
  """Register a map from legacy disk type names to modern ones.

  The translation machinery looks here to find the map corresponding
  to the chosen provider and translates the user's flags and configs
  to the new naming system. This function should be removed once the
  (deprecated) legacy flags are removed.

  Args:
    provider_name: a string. The name of the provider. Must match
      the names we give to providers in benchmark_spec.py.
    type_map: a dict. Maps generic disk type names (STANDARD,
      REMOTE_SSD, PIOPS) to provider-specific names.
  """

  DISK_TYPE_MAPS[provider_name] = type_map


_DISK_SPEC_REGISTRY = {}


def GetDiskSpecClass(cloud):
  """Get the DiskSpec class corresponding to 'cloud'."""
  return _DISK_SPEC_REGISTRY.get(cloud, BaseDiskSpec)


class AutoRegisterDiskSpecMeta(type):
  """Metaclass which automatically registers DiskSpecs."""

  def __init__(cls, name, bases, dct):
    if cls.CLOUD in _DISK_SPEC_REGISTRY:
      raise Exception('BaseDiskSpec subclasses must have a CLOUD attribute.')
    else:
      _DISK_SPEC_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterDiskSpecMeta, cls).__init__(name, bases, dct)


def WarnAndTranslateDiskTypes(name, cloud):
  """Translate old disk types to new disk types, printing warnings if needed.

  Args:
    name: a string specifying a disk type, either new or old.
    cloud: the cloud we're running on.

  Returns:
    The new-style disk type name (i.e. the provider's name for the type).
  """

  if cloud in DISK_TYPE_MAPS:
    disk_type_map = DISK_TYPE_MAPS[cloud]
    if name in disk_type_map and disk_type_map[name] != name:
      new_name = disk_type_map[name]
      logging.warning('Disk type name %s is deprecated and will be removed. '
                      'Translating to %s for now.', name, new_name)
      return new_name
    else:
      return name
  else:
    logging.info('No legacy->new disk type map for provider %s', cloud)
    # The provider has not been updated to use new-style names. We
    # need to keep benchmarks working, so we pass through the name.
    return name


def WarnAndCopyFlag(old_name, new_name):
  """Copy a value from an old flag to a new one, warning the user.
  """

  if FLAGS[old_name].present:
    logging.warning('Flag --%s is deprecated and will be removed. Please '
                    'switch to --%s.', old_name, new_name)
    if not FLAGS[new_name].present:
      FLAGS[new_name].value = FLAGS[old_name].value

      # Mark the new flag as present so we'll print it out in our list
      # of flag values.
      FLAGS[new_name].present = True
    else:
      logging.warning('Ignoring legacy flag %s because new flag %s is present.',
                      old_name, new_name)
  # We keep the old flag around so that providers that haven't been
  # updated yet will continue to work.


DISK_FLAGS_TO_TRANSLATE = {
    'scratch_disk_type': 'data_disk_type',
    'scratch_disk_iops': 'aws_provisioned_iops',
    'scratch_disk_size': 'data_disk_size'
}


def WarnAndTranslateDiskFlags():
  """Translate old disk-related flags to new disk-related flags.
  """

  for old, new in DISK_FLAGS_TO_TRANSLATE.iteritems():
    WarnAndCopyFlag(old, new)


class BaseDiskSpec(object):
  """Stores the information needed to create a disk."""

  __metaclass__ = AutoRegisterDiskSpecMeta
  CLOUD = None

  def __init__(self, disk_size=None, disk_type=None,
               mount_point=None, num_striped_disks=1,
               disk_number=None, device_path=None):
    """Initializes the DiskSpec object.

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
    self.num_striped_disks = num_striped_disks
    self.disk_number = disk_number
    self.device_path = device_path

  def ApplyFlags(self, flags):
    """Applies flags to the DiskSpec."""
    self.disk_size = flags.data_disk_size or self.disk_size
    self.disk_type = flags.data_disk_type or self.disk_type
    self.num_striped_disks = flags.num_striped_disks or self.num_striped_disks
    self.mount_point = flags.scratch_dir or self.mount_point


class BaseDisk(resource.BaseResource):
  """Object representing a Base Disk."""

  is_striped = False

  def __init__(self, disk_spec):
    super(BaseDisk, self).__init__()
    self.disk_size = disk_spec.disk_size
    self.disk_type = disk_spec.disk_type
    self.mount_point = disk_spec.mount_point
    self.num_striped_disks = disk_spec.num_striped_disks

    # Linux related attributes.
    self.device_path = disk_spec.device_path

    # Windows related attributes.

    # The disk number corresponds to the order in which disks were attached to
    # the instance. The System Disk has a disk number of 0. Any local disks
    # have disk numbers ranging from 1 to the number of local disks on the
    # system. Any additional disks that were attached after boot will have
    # disk numbers starting at the number of local disks + 1. These disk
    # numbers are used in diskpart scripts in order to identify the disks
    # that we want to operate on.
    self.disk_number = disk_spec.disk_number

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

  def GetDevicePath(self):
    """Returns the path to the device inside a Linux VM."""
    return self.device_path


class StripedDisk(BaseDisk):
  """Object representing several disks striped together."""

  is_striped = True

  def __init__(self, disk_spec, disks):
    """Initializes a StripedDisk object.

    Args:
      disk_spec: A BaseDiskSpec containing the desired mount point.
      disks: A list of BaseDisk objects that constitute the StripedDisk.
      device_path: The path of the striped device in a Linux VM.
    """
    super(StripedDisk, self).__init__(disk_spec)
    self.disks = disks

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
