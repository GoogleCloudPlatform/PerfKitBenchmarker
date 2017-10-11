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

Volumes can be created, deleted, attached to VMs, and detached from VMs. Also
disks could be either remote, or local. Remote disks are storage volumes
provided by Rackspace Cloud Block Storage, and supports two disk types, cbs-ssd,
and cbs-sata. Local disks can be local data disks if they are provided by the
flavor.
"""

import json
import logging

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.rackspace import util

FLAGS = flags.FLAGS

DURABILITY = 'durability'
EPHEMERAL = 'ephemeral'
PERSISTENT = 'persistent'
RAID10 = 'RAID10'
BOOT = 'boot'
LOCAL = 'local'
CBS_SATA = 'cbs-sata'
CBS_SSD = 'cbs-ssd'
REMOTE_TYPES = (CBS_SSD, CBS_SATA,)
REMOTE_TYPES_TRANSLATION = {
    CBS_SATA: 'SATA',
    CBS_SSD: 'SSD'
}

DISK_TYPE = {
    disk.STANDARD: BOOT,
    disk.REMOTE_SSD: CBS_SSD,
    disk.LOCAL: LOCAL
}

DISK_METADATA = {
    BOOT: {
        disk.REPLICATION: RAID10,
        DURABILITY: EPHEMERAL
    },
    LOCAL: {
        disk.REPLICATION: RAID10,
        DURABILITY: EPHEMERAL
    },
    CBS_SSD: {
        disk.REPLICATION: disk.REGION,
        DURABILITY: PERSISTENT,
        disk.MEDIA: disk.SSD
    },
    CBS_SATA: {
        disk.REPLICATION: disk.REGION,
        DURABILITY: PERSISTENT,
        disk.MEDIA: disk.HDD
    }
}

disk.RegisterDiskTypeMap(providers.RACKSPACE, DISK_TYPE)


class RackspaceDiskSpec(disk.BaseDiskSpec):
  """Object containing the information needed to create a
  RackspaceDisk.

  Attributes:
    rackspace_region: None or string. Rackspace region to build VM resources.
    rack_profile: None or string. Rack CLI profile configuration.
  """
  CLOUD = providers.RACKSPACE

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super(RackspaceDiskSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['rackspace_region'].present:
      config_values['rackspace_region'] = flag_values.rackspace_region
    if flag_values['rack_profile'].present:
      config_values['rack_profile'] = flag_values.rack_profile

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(RackspaceDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'rackspace_region': (option_decoders.StringDecoder, {'default': 'IAD'}),
        'rack_profile': (option_decoders.StringDecoder, {'default': None})})
    return result


class RackspaceDisk(disk.BaseDisk):
  """Base class for Rackspace disks."""

  def __init__(self, disk_spec, name, region, project, image=None):
    super(RackspaceDisk, self).__init__(disk_spec)
    self.name = name
    self.zone = region
    self.region = disk_spec.rackspace_region
    self.profile = disk_spec.rack_profile
    self.project = project
    self.image = image
    self.attached_vm_id = None
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])

  def _Create(self):
    """Creates the disk."""
    raise NotImplementedError()

  def _Delete(self):
    """Deletes the disk."""
    raise NotImplementedError()

  def Attach(self, vm):
    """Attaches disk, if needed, to the VM."""
    self.attached_vm_id = vm.id

  def Detach(self):
    """Detaches disk, if needed, from the VM."""
    self.attached_vm_id = None


class RackspaceLocalDisk(RackspaceDisk):
  """RackspaceLocalDisk is a disk object to represent an ephemeral storage disk
  locally attached to an instance.
  """

  def __init__(self, disk_spec, name, region, project, device_path, image=None):
    super(RackspaceLocalDisk, self).__init__(disk_spec, name, region, project,
                                             image)
    self.exists = False
    self.device_path = device_path
    self.name = name

  def _Create(self):
    self.exists = True

  def _Delete(self):
    self.exists = False

  def _Exists(self):
    return self.exists


class RackspaceBootDisk(RackspaceLocalDisk):
  """RackspaceBootDisk is a disk object to represent the root (boot) disk of an
  instance. Boot disk provides a directory path as a scratch disk space for a
  benchmark, but does not allow its backing block device to be formatted, or
  its mount point to be changed.
  """

  def __init__(self, disk_spec, zone, project, device_path, image):
    super(RackspaceBootDisk, self).__init__(disk_spec, 'boot-disk', zone,
                                            project, device_path, image)
    self.mount_point = disk_spec.mount_point


class RackspaceRemoteDisk(RackspaceDisk):
  """RackspaceRemoteDisk is a RackspaceDisk object to represent a remotely
  attached Cloud Block Storage Volume.
  """

  def __init__(self, disk_spec, name, region, project, image=None, media=None):
    super(RackspaceRemoteDisk, self).__init__(disk_spec, name, region, project,
                                              image)
    self.media = media
    self.id = None

  def _Create(self):
    cmd = util.RackCLICommand(self, 'block-storage', 'volume', 'create')
    cmd.flags['size'] = self.disk_size
    cmd.flags['name'] = self.name
    cmd.flags['volume-type'] = REMOTE_TYPES_TRANSLATION[self.media]
    stdout, stderr, _ = cmd.Issue()
    resp = json.loads(stdout)
    self.id = resp['ID']

  def _Delete(self):
    if self.id is None:
      return
    cmd = util.RackCLICommand(self, 'block-storage', 'volume', 'delete')
    cmd.flags['id'] = self.id
    cmd.Issue()
    self._WaitForRemoteDiskDeletion()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = util.RackCLICommand(self, 'block-storage', 'volume', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    if stdout and stdout.strip():
      return stdout
    return not stderr

  def Attach(self, vm):
    self._AttachRemoteDisk(vm)
    self._WaitForRemoteDiskAttached(vm)
    self.attached_vm_id = vm.id

  def Detach(self):
    self._DetachRemoteDisk()
    self.attached_vm_id = None

  def _AttachRemoteDisk(self, vm):
    if self.id is None:
      raise errors.Error('Cannot attach remote disk %s' % self.name)
    if vm.id is None:
      raise errors.VirtualMachine.VmStateError(
          'Cannot attach remote disk %s to non-existing %s VM' % (self.name,
                                                                  vm.name))
    cmd = util.RackCLICommand(self, 'servers', 'volume-attachment', 'create')
    cmd.flags['volume-id'] = self.id
    cmd.flags['server-id'] = vm.id
    stdout, stderr, _ = cmd.Issue()
    if stderr:
      raise errors.Error(
          'Failed to attach remote disk %s to %s' % (self.name, vm.name))
    resp = json.loads(stdout)
    self.device_path = resp['Device']

  @vm_util.Retry(poll_interval=1, max_retries=-1, timeout=300, log_errors=False,
                 retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def _WaitForRemoteDiskAttached(self, vm):
    cmd = util.RackCLICommand(self, 'block-storage', 'volume', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    if stdout:
      resp = json.loads(stdout)
      attachments = resp['Attachments']
      if attachments:
        logging.info('Disk: %s has been attached to %s.' % (self.name, vm.id))
        return
    raise errors.Resource.RetryableCreationError(
        'Disk: %s is not yet attached. Retrying to check status.' % self.name)

  def _DetachRemoteDisk(self):
    if self.id is None:
      raise errors.Error('Cannot detach remote disk %s' % self.name)
    if self.attached_vm_id is None:
      raise errors.VirtualMachine.VmStateError(
          'Cannot detach remote disk %s from a non-existing VM' % self.name)
    cmd = util.RackCLICommand(self, 'servers', 'volume-attachment', 'delete')
    cmd.flags['id'] = self.id
    cmd.flags['server-id'] = self.attached_vm_id
    stdout, stderr, _ = cmd.Issue()
    if stdout:
      resp = json.loads(stdout)
      if 'Successfully deleted' in resp['result']:
        return
    raise errors.Resource.RetryableDeletionError(stderr)

  @vm_util.Retry(poll_interval=1, max_retries=-1, timeout=300, log_errors=False,
                 retryable_exceptions=(errors.Resource.RetryableDeletionError,))
  def _WaitForRemoteDiskDeletion(self):
    cmd = util.RackCLICommand(self, 'block-storage', 'volume', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    if stderr:
      logging.info('Disk: %s has been successfully deleted.' % self.name)
      return
    raise errors.Resource.RetryableDeletionError(
        'Disk: %s has not been deleted. Retrying to check status.' % self.name)
