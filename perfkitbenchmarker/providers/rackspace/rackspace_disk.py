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
"""Module containing classes related to Rackspace volumes.

Volumes can be created, deleted, *attached to VMs, and *detached from VMs. Also
disks could be either remote, or ephemeral. Remote disks are storage volumes
provided by Rackspace Cloud Block Storage, and supports two disk types, SATA,
and SSD. Ephemeral disks can be local data disks if they are provided by the
flavor, otherwise it will default to the system's boot disk.
"""

import json
import os
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.rackspace import util

FLAGS = flags.FLAGS

DISK_TYPE = {
    disk.STANDARD: 'SATA',
    disk.REMOTE_SSD: 'SSD'
}


class RackspaceEphemeralDisk(disk.BaseDisk):
  def __init__(self, disk_spec, device_name, is_root_disk=False):
    super(RackspaceEphemeralDisk, self).__init__(disk_spec)
    self.device_name = device_name
    self.is_root_disk = is_root_disk

    if self.is_root_disk:
      self.mount_point = ''

    self.exists = False

  def __str__(self):
    return ("%s(device_path=%s, is_root_disk=%s)"
            % (self.__class__.__name__,
               self.GetDevicePath(), self.is_root_disk))

  def __repr__(self):
    return self.__str__()

  def _Create(self):
    """Boot disk must exists, so pass."""
    self.exists = True

  def _Delete(self):
    """Boot disk cannot be deleted"""
    self.exists = False

  def _Exists(self):
    """Boot disk always exists"""
    return self.exists

  def Attach(self, vm):
    """Boot disk is always attached."""
    pass

  def Detach(self):
    """No need to detach boot disk."""
    pass

  def GetDevicePath(self):
    return '/dev/%s' % self.device_name


class RackspaceRemoteDisk(disk.BaseDisk):
  """Object representing a Rackspace Volume."""

  def __init__(self, disk_spec, name, zone, image=None):
      super(RackspaceRemoteDisk, self).__init__(disk_spec)
      self.attached_vm_name = None
      self.image = image
      self.name = name
      self.zone = zone
      self.id = ''

  def __str__(self):
    return ("%s(device_path=%s, id=%s, attached_vm=%s, disk_type=%s)"
            % (self.__class__.__name__, self.GetDevicePath(),
               self.id, self.attached_vm_name, self.disk_type))

  def __repr__(self):
    return self.__str__()

  def _Create(self):
    """Creates the volume."""
    nova_env = os.environ.copy()
    nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    create_cmd = [
        FLAGS.nova_path,
        'volume-create',
        '--display-name', self.name,
        '--volume-type', DISK_TYPE[self.disk_type],
        str(self.disk_size)]
    stdout, _, _ = vm_util.IssueCommand(create_cmd, env=nova_env)
    volume = util.ParseNovaTable(stdout)
    if 'id' in volume and volume['id']:
      self.id = volume['id']
    else:
      raise errors.Error('There was a problem when creating a volume.')

  def _Delete(self):
    """Deletes the volume."""
    nova_env = os.environ.copy()
    nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    delete_cmd = [FLAGS.nova_path, 'volume-delete', self.name]
    vm_util.IssueCommand(delete_cmd, env=nova_env)

  def _Exists(self):
    """Returns true if the volume exists."""
    nova_env = os.environ.copy()
    nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    getdisk_cmd = [FLAGS.nova_path, 'volume-show', self.name]
    stdout, _, _ = vm_util.IssueCommand(getdisk_cmd, env=nova_env)
    if stdout.strip() == '':
      return False
    volume = util.ParseNovaTable(stdout)
    return 'status' in volume and volume['status'] == 'available'

  def Attach(self, vm):
    """Attaches the volume to a VM.

    Args:
      vm: The RackspaceVirtualMachine instance to which the volume will be
          attached.
    """
    self.attached_vm_name = vm.name
    nova_env = os.environ.copy()
    nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    attach_cmd = [
        FLAGS.nova_path,
        'volume-attach',
        self.attached_vm_name,
        self.id, ""]
    vm_util.IssueRetryableCommand(attach_cmd, env=nova_env)

    if not self._WaitForDiskUntilAttached(vm):
      raise errors.Resource.RetryableCreationError(
          'Failed to attach all scratch disks to vms. Exiting...')

  def _WaitForDiskUntilAttached(self, vm, max_retries=30, poll_interval_secs=5):
    """Wait until volume is attached to the instance."""
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    volume_show_cmd = [FLAGS.nova_path, 'volume-show', self.id]

    for _ in xrange(max_retries):
      stdout, _, _ = vm_util.IssueCommand(volume_show_cmd, env=env)
      volume = util.ParseNovaTable(stdout)
      if 'attachments' in volume and vm.id in volume['attachments']:
        return True
      time.sleep(poll_interval_secs)

    return False

  def Detach(self):
      """Detaches the disk from a VM."""
      nova_env = os.environ.copy()
      nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
      detach_cmd = [
          FLAGS.nova_path,
          'volume-detach',
          self.attached_vm_name,
          self.id]
      vm_util.IssueRetryableCommand(detach_cmd, env=nova_env)
      self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    nova_env = os.environ.copy()
    nova_env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    getdisk_cmd = [FLAGS.nova_path, 'volume-show', self.name]
    stdout, _, _ = vm_util.IssueCommand(getdisk_cmd, env=nova_env)
    volume = util.ParseNovaTable(stdout)

    attachment_list = []
    if 'attachments' in volume and volume['attachments']:
      attachment_list = json.loads(volume['attachments'])

    if not attachment_list:
      raise errors.Error('Cannot determine volume %s attachments' % self.name)

    for attachment in attachment_list:
      if 'volume_id' in attachment and attachment['volume_id'] == self.id:
        return attachment['device']
    else:
      raise errors.Error(
          'Could not find device path in the volume %s attachments')
