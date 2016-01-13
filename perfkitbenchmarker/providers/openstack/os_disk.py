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

import logging
import time


from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker.providers.openstack import utils as os_utils
from perfkitbenchmarker.providers.openstack.utils import retry_authorization

FLAGS = flags.FLAGS

ROOT = 'root'
LOCAL = 'local'
VOLUME = 'volume'

DISK_TYPE = {disk.STANDARD: VOLUME, disk.REMOTE_SSD: VOLUME, disk.LOCAL: LOCAL}

DISK_METADATA = {
    VOLUME: {
        disk.LEGACY_DISK_TYPE: disk.STANDARD,
        disk.REPLICATION: disk.REGION
    },
    ROOT: {
        disk.REPLICATION: None
    },
    LOCAL: {
        disk.LEGACY_DISK_TYPE: disk.LOCAL
    }
}

OPENSTACK = 'OpenStack'
disk.RegisterDiskTypeMap(OPENSTACK, DISK_TYPE)


class OpenStackDisk(disk.BaseDisk):
  """
  Base class for OpenStack disks.
  """
  def __init__(self, disk_spec, name):
    super(OpenStackDisk, self).__init__(disk_spec)
    self.name = name

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def Attach(self, vm):
    pass

  def Detach(self):
    pass


class OpenStackBlockStorageDisk(OpenStackDisk):
  """
  OpenStackBlockStorageDisk is an OpenStackDisk object to represent a
  remote block storage volume that can be attached to an instance.
  """

  def __init__(self, disk_spec, name, zone):
    super(OpenStackBlockStorageDisk, self).__init__(disk_spec, name)
    self.__nclient = os_utils.NovaClient()
    self.attached_vm_name = None
    self.attached_vm_id = -1
    self.name = 'cinder-volume-%s' % name
    self.zone = zone
    self.device = ""
    self._disk = None

  def _Create(self):
    self._disk = self.__nclient.volumes.create(self.disk_size,
                                               display_name=self.name,
                                               availability_zone=self.zone,)
    is_unavailable = True
    while is_unavailable:
      time.sleep(1)
      volume = self.__nclient.volumes.get(self._disk.id)
      if volume:
        is_unavailable = not (volume.status == "available")
        self._disk = volume

  @retry_authorization(max_retries=4)
  def _Delete(self):
    from novaclient.exceptions import NotFound

    if self._disk is None:
      logging.info('Volume %s was not created. Skipping deletion.'
                   % self.name)
      return

    sleep = 1
    sleep_count = 0
    try:
      self.__nclient.volumes.delete(self._disk)
      is_deleted = False
      while not is_deleted:
        volume = self.__nclient.volumes.get(self._disk.id)
        is_deleted = volume is None
        time.sleep(sleep)
        sleep_count += 1
        if sleep_count == 10:
            sleep = 5
    except NotFound:
      logging.info('Volume %s not found, might have been already deleted'
                   % self._disk.id)

  def _Exists(self):
    from novaclient.exceptions import NotFound
    try:
      volume = self.__nclient.volumes.get(self._disk.id)
      return volume and volume.status in ('available', 'in-use',
                                          'deleting',)
    except NotFound:
      return False

  def Attach(self, vm):
    self.attached_vm_name = vm.name
    self.attached_vm_id = vm.id

    result = self.__nclient.volumes.create_server_volume(vm.id,
                                                         self._disk.id,)
    self.attach_id = result.id

    volume = None
    is_unattached = True
    while is_unattached:
      time.sleep(1)
      volume = self.__nclient.volumes.get(result.id)
      if volume:
          is_unattached = not(volume.status == "in-use"
                              and volume.attachments)

    for attachment in volume.attachments:
      if self.attach_id == attachment.get('volume_id'):
        self.device = attachment.get('device')
        return

    raise errors.Error("Couldn't not attach volume to %s" % vm.name)

  def GetDevicePath(self):
    return self.device

  def Detach(self):
    self.__nclient.volumes.delete_server_volume(self.attached_vm_id,
                                                self.attach_id)


class OpenStackLocalDisk(OpenStackDisk):
  """
  OpenStackLocalDisk is a disk object to represent ephemeral storage disk that
  is locally attached to an instance.
  """

  def __init__(self, disk_spec, device_path, name):
    """Initialize OpenStack Local Disk object.

    Args:
      disk_spec: DiskSpec object contains information needed of desired disk.
      device_path: string Linux device path of the local disk. e.g. '/dev/vdb'
    """
    super(OpenStackLocalDisk, self).__init__(disk_spec, name)
    self.exists = False
    self.device_path = device_path
    self.name = 'local-disk-%s' % name

  def _Create(self):
    self.exists = True

  def _Exists(self):
    return self.exists

  def _Delete(self):
    self.exists = False

  def Attach(self, vm):
    pass

  def Detach(self):
    pass


class OpenStackRootDisk(OpenStackLocalDisk):
  """
  OpenStackRootDisk is a disk object to represent the root disk of an instance.
  Root disk provide a directory path as a scratch disk space for a benchmark,
  but does not allows its backing block device to be formatted, or its mount
  point to be changed.
  """

  def __init__(self, disk_spec, device_path):
    """Initialize OpenStack Local Disk object.

    Args:
      disk_spec: DiskSpec object contains information needed of desired disk.
      device_path: string Linux device path of the local disk. e.g. '/dev/vdb'
    """
    super(OpenStackRootDisk, self).__init__(disk_spec, device_path, 'root-disk')
    self.mount_point = disk_spec.mount_point

  def GetDevicePath(self):
    """
    Root disk is already mounted. Formatting of of root disk is not permitted.
    """
    raise errors.Error('Formatting of root disk block device is not supported.')
