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

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags

FLAGS = flags.FLAGS

# TODO: define a common flag for Mesos and K8S
flags.DEFINE_string('mesos_rbd_pool', 'rbd',
                    'Name of RBD pool for Ceph volumes.')


class MesosDisk(disk.BaseDisk):
  """
  Base class for Mesos Disks.
  """

  def __init__(self, disk_spec):
    super(MesosDisk, self).__init__(disk_spec)
    self.mount_point = disk_spec.mount_point

  def _Create(self):
    return

  def _Delete(self):
    return

  def Attach(self, vm):
    return

  def Detach(self):
    return

  def GetPrepareCmd(self):
    return ""

  def GetDockerVolumeParam(self):
    return ""

  def GetCleanupCmd(self):
    return ""

  def SetDevicePath(self, vm):
    return

  def GetDevicePath(self):
    return None


class LocalDisk(MesosDisk):

  def __init__(self, disk_num, disk_spec, name):
    super(LocalDisk, self).__init__(disk_spec)
    self.name = 'local-disk-%s-%s' % (name, disk_num)

  def GetDockerVolumeParam(self):
    volume_cmd = '-v %s ' % (self.mount_point)
    return volume_cmd


class CephDisk(MesosDisk):

  def __init__(self, disk_num, disk_spec, name):
    super(CephDisk, self).__init__(disk_spec)
    self.name = 'rbd-%s-%s' % (name, disk_num)
    self.device_path = None

  def SetDevicePath(self, vm):
    # TODO: implement the method
    return

  def GetDevicePath(self):
    return self.device_path

  def GetPrepareCmd(self):
    disk_size = str(1024 * self.disk_size)

    # TODO:
    # - test different rbd pool
    # - move to SH file (will be easier to read)
    cmd = 'rbd create -p {1} --size={2} {0}; ' \
          'rbd map {0}; ' \
          '/sbin/mkfs.ext4 /dev/rbd/rbd/{0};' \
          'mkdir -p /mnt/{0}; ' \
          'mount /dev/rbd/rbd/{0} /mnt/{0};'.format(self.name,
                                                    FLAGS.mesos_rbd_pool,
                                                    disk_size)
    return cmd

  def GetDockerVolumeParam(self):
    host_path = '/mnt/%s' % self.name
    return '-v %s:%s ' % (host_path, self.mount_point)

  def GetCleanupCmd(self):
    # TODO:
    # - embed in a loop and retry until the image is finally removed
    # - make sure that 'umount' and 'rbd unmap' are not needed in any case
    return 'rbd rm %s/%s; ' % (FLAGS.rbd_pool, self.name)
