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
"""Module containing classes related to DigitalOcean disks.

At this time, DigitalOcean does not implement any standalone disk objects,
the instances come with directly integrated storage.
"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers

from perfkitbenchmarker.providers.digitalocean import util

FLAGS = flags.FLAGS

BLOCK_STORAGE = 'block-storage'

LOCAL_DISK_METADATA = {
    disk.MEDIA: disk.SSD,
    disk.REPLICATION: disk.NONE,
}

BLOCK_STORAGE_METADATA = {
    disk.MEDIA: disk.SSD,
    disk.REPLICATION: disk.ZONE,
}

# Map legacy disk types to DigitalOcean disk types.
DISK_TYPE_MAP = {
    disk.REMOTE_SSD: BLOCK_STORAGE
}
disk.RegisterDiskTypeMap(providers.DIGITALOCEAN, DISK_TYPE_MAP)


class DigitalOceanLocalDisk(disk.BaseDisk):
  """Dummy Object representing a DigitalOcean Disk."""

  def __init__(self, disk_spec):
    super(DigitalOceanLocalDisk, self).__init__(disk_spec)
    self.metadata.update(LOCAL_DISK_METADATA)

  def Attach(self, vm):
    pass

  def Detach(self):
    pass

  def GetDevicePath(self):
    # The local disk is always the boot disk, and it cannot be
    # partitioned or reformatted, so we don't support GetDevicePath().
    raise errors.Error(
        'GetDevicePath not supported for DigitalOcean local disks.')

  def _Create(self):
    pass

  def _Delete(self):
    pass


class DigitalOceanBlockStorageDisk(disk.BaseDisk):
  """Interface to DigitalOcean Block Storage."""

  def __init__(self, disk_spec, zone):
    super(DigitalOceanBlockStorageDisk, self).__init__(disk_spec)
    self.zone = zone
    if self.disk_type != BLOCK_STORAGE:
      raise ValueError('DigitalOcean data disks must have type block-storage.')
    self.metadata.update(BLOCK_STORAGE_METADATA)

  def _Create(self):
    self.volume_name = 'pkb-%s-%s' % (FLAGS.run_uri, self.disk_number)

    response, retcode = util.DoctlAndParse(
        ['compute', 'volume', 'create',
         self.volume_name,
         '--region', self.zone,
         '--size', str(self.disk_size) + 'gb'])
    if retcode:
      raise errors.Resource.RetryableCreationError(
          'Error creating disk: %s' % (response,))

    self.volume_id = response[0]['id']

  def _Delete(self):
    response, retcode = util.DoctlAndParse(
        ['compute', 'volume', 'delete',
         self.volume_id, '--force'])
    if retcode:
      raise errors.Resource.RetryableDeletionError(
          'Error deleting disk: %s' % (response,))

  def Attach(self, vm):
    response, retcode = util.DoctlAndParse(
        ['compute', 'volume-action', 'attach',
         self.volume_id, vm.droplet_id])
    if retcode:
      raise errors.VmUtil.CalledProcessException(
          'Error attaching disk: %s' % (response,))

    action_id = response[0]['id']
    util.WaitForAction(action_id)

  def Detach(self):
    response, retcode = util.DoctlAndParse(
        ['compute', 'volume-action', 'detach',
         self.volume_id])
    if retcode:
      raise errors.VmUtil.CalledProcessException(
          'Error detaching disk: %s' % (response,))

    action_id = response[0]['id']
    util.WaitForAction(action_id)

  def GetDevicePath(self):
    return '/dev/disk/by-id/scsi-0DO_Volume_%s' % self.volume_name
