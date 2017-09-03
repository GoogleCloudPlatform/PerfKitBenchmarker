# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.alicloud import util

FLAGS = flags.FLAGS

DISK_TYPE = {
    disk.STANDARD: 'cloud',
    disk.REMOTE_SSD: 'cloud_ssd',
    disk.PIOPS: 'cloud_efficiency',
    disk.LOCAL: 'ephemeral_ssd'
}


class AliDisk(disk.BaseDisk):
  """Object representing an AliCloud Disk."""

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
        '--Device %s' % self.GetVirtualDevicePath()]
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
    return '/dev/vd%s' % self.device_letter

  def GetVirtualDevicePath(self):
    """Returns the path to the device visible to console users."""
    return '/dev/xvd%s' % self.device_letter

  @vm_util.Retry(poll_interval=5, max_retries=30, log_errors=False)
  def WaitForDiskStatus(self, status_list):
    """Waits until disk is attach to the instance"""
    logging.info('Waits until the disk\'s status is one of statuses: %s',
                 status_list)
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeDisks',
        '--RegionId %s' % self.region,
        '--ZoneId %s' % self.zone,
        '--DiskIds \'["%s"]\'' % self.id,
        '--Device %s' % self.GetVirtualDevicePath()]
    attach_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(attach_cmd)
    response = json.loads(stdout)
    disk = response['Disks']['Disk']
    assert len(disk) == 1
    status = disk[0]['Status']
    assert status in status_list
