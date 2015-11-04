# Copyright 2015 Google Inc. All rights reserved.
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

"""Module containing classes related to AliCloud disks.
"""

import json
import string
import threading
import logging

from perfkitbenchmarker import disk
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.alicloud import util

DISK_TYPE = {
    disk.STANDARD: 'cloud',
    disk.REMOTE_SSD: 'cloud_ssd',
    disk.PIOPS: 'cloud_efficiency',
    disk.LOCAL: 'ephemeral_ssd'
}

class AliDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create an AliDisk."""

  def __init__(self, **kwargs):
    """Initializes the Disk Spec.

    Args:
      iops: The number of provisioned IOPS for a PIOPS disk type.
      kwargs: The key word arguments to disk.BaseDiskSpec's __init__ method.
    """
    super(AliDiskSpec, self).__init__(**kwargs)

  def ApplyFlags(self, flags):
    """Apply flags to the DiskSpec."""
    super(AliDiskSpec, self).ApplyFlags(flags)


class AliDisk(disk.BaseDisk):
  """Object representing an Ali Disk."""

  _lock = threading.Lock()
  vm_devices = {}

  def __init__(self, disk_spec, zone):
    super(AliDisk, self).__init__(disk_spec)
    self.id = None
    self.zone = zone
    self.region = util.GetRegionByZone(self.zone)
    self.attached_vm_id = None

  def _Create(self):
    """Creates the disk."""
    create_cmd = util.ALI_PREFIX + [
                  'ecs',
                  'CreateDisk',
                  '--RegionId %s' % self.region,
                  '--ZoneId %s' % self.zone,
                  '--Size %s' % self.disk_size,
                  '--DiskCategory %s' % DISK_TYPE[self.disk_type]]
    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['DiskId']

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = util.ALI_PREFIX + [
                  'ecs',
                  'DeleteDisk',
                  '--RegionId %s' % self.region,
                  '--DiskId %s' % self.id]
    logging.info('Deleting AliCloud disk %s. This may fail if the disk is not '
             'yet detached, but will be retried.', self.id)
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueCommand(delete_cmd)

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AliVirtualMachine instance to which the disk will be attached.
    """
    with self._lock:
      self.attached_vm_id = vm.id
      if self.attached_vm_id not in AliDisk.vm_devices:
        AliDisk.vm_devices[self.attached_vm_id] = set(
            string.ascii_lowercase[1:])
      self.device_letter = min(AliDisk.vm_devices[self.attached_vm_id])
      AliDisk.vm_devices[self.attached_vm_id].remove(self.device_letter)

    attach_cmd = util.ALI_PREFIX + [
                  'ecs',
                  'AttachDisk',
                  '--RegionId %s' % self.region,
                  '--InstanceId %s' % self.attached_vm_id,
                  '--DiskId %s' % self.id,
                  '--Device %s' % self.GetDevicePath()]
    attach_cmd = util.GetEncodedCmd(attach_cmd)
    vm_util.IssueRetryableCommand(attach_cmd)

  def Detach(self):
    """Detaches the disk from a VM."""
    detach_cmd = util.ALI_PREFIX + [
                  'ecs',
                  'DetachDisk',
                  '--RegionId %s' % self.region,
                  '--InstanceId %s' % self.attached_vm_id,
                  '--DiskId %s' % self.id]
    detach_cmd = util.GetEncodedCmd(detach_cmd)
    vm_util.IssueRetryableCommand(detach_cmd)

    with self._lock:
      assert self.attached_vm_id in AliDisk.vm_devices
      AliDisk.vm_devices[self.attached_vm_id].add(self.device_letter)
      self.attached_vm_id = None
      self.device_letter = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    return '/dev/xvd%s' % self.device_letter #TODO  IoOptimized
