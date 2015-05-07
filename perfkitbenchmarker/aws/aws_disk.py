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

"""Module containing classes related to AWS disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html to
determine valid disk types.
See http://aws.amazon.com/ebs/details/ for more information about AWS (EBS)
disks.
"""

import json
import logging
import string
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.aws import util

VOLUME_EXISTS_STATUSES = frozenset(['creating', 'available', 'in-use', 'error'])
VOLUME_DELETED_STATUSES = frozenset(['deleting', 'deleted'])
VOLUME_KNOWN_STATUSES = VOLUME_EXISTS_STATUSES | VOLUME_DELETED_STATUSES
DISK_TYPE = {
    disk.STANDARD: 'standard',
    disk.REMOTE_SSD: 'gp2',
    disk.PIOPS: 'io1'
}


class AwsDisk(disk.BaseDisk):
  """Object representing an Aws Disk."""

  _lock = threading.Lock()
  vm_devices = {}

  def __init__(self, disk_spec, zone):
    super(AwsDisk, self).__init__(disk_spec)
    self.id = None
    self.zone = zone
    self.region = zone[:-1]
    self.device_letter = None
    self.attached_vm_id = None

  def _Create(self):
    """Creates the disk."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-volume',
        '--region=%s' % self.region,
        '--size=%s' % self.disk_size,
        '--availability-zone=%s' % self.zone,
        '--volume-type=%s' % DISK_TYPE[self.disk_type]]
    if self.disk_type == 'io1':
      create_cmd.append('--iops=%s' % self.iops)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['VolumeId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-volume',
        '--region=%s' % self.region,
        '--volume-id=%s' % self.id]
    logging.info('Deleting AWS volume %s. This may fail if the disk is not '
                 'yet detached, but will be retried.', self.id)
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the disk exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-volumes',
        '--region=%s' % self.region,
        '--filter=Name=volume-id,Values=%s' % self.id]
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    volumes = response['Volumes']
    assert len(volumes) < 2, 'Too many volumes.'
    if not volumes:
      return False
    status = volumes[0]['State']
    assert status in VOLUME_KNOWN_STATUSES, status
    return status in VOLUME_EXISTS_STATUSES

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AwsVirtualMachine instance to which the disk will be attached.
    """
    with self._lock:
      self.attached_vm_id = vm.id
      if self.attached_vm_id not in AwsDisk.vm_devices:
        AwsDisk.vm_devices[self.attached_vm_id] = set(
            string.ascii_lowercase)
      self.device_letter = min(AwsDisk.vm_devices[self.attached_vm_id])
      AwsDisk.vm_devices[self.attached_vm_id].remove(self.device_letter)

    attach_cmd = util.AWS_PREFIX + [
        'ec2',
        'attach-volume',
        '--region=%s' % self.region,
        '--instance-id=%s' % self.attached_vm_id,
        '--volume-id=%s' % self.id,
        '--device=%s' % self.GetDevicePath()]
    logging.info('Attaching AWS volume %s. This may fail if the disk is not '
                 'ready, but will be retried.', self.id)
    vm_util.IssueRetryableCommand(attach_cmd)

  def Detach(self):
    """Detaches the disk from a VM."""
    detach_cmd = util.AWS_PREFIX + [
        'ec2',
        'detach-volume',
        '--region=%s' % self.region,
        '--instance-id=%s' % self.attached_vm_id,
        '--volume-id=%s' % self.id]
    vm_util.IssueRetryableCommand(detach_cmd)

    with self._lock:
      assert self.attached_vm_id in AwsDisk.vm_devices
      AwsDisk.vm_devices[self.attached_vm_id].add(self.device_letter)
      self.attached_vm_id = None
      self.device_letter = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      return '/dev/xvd%s' % self.device_letter
    else:
      return '/dev/xvdb%s' % self.device_letter
