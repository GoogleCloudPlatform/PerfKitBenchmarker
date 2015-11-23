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
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import errors
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE

FLAGS = flags.FLAGS


def CreateDisks(disk_specs, vm_name):
  """
  Creates instances of KubernetesDisk child classes depending on
  scratch disk type.
  """
  scratch_disks = []
  for disk_num, disk_spec in enumerate(disk_specs):
    if disk_spec.disk_type == disk.LOCAL:
      scratch_disk = LocalDisk(disk_num, disk_spec, vm_name)
    else:
      scratch_disk = CephDisk(disk_num, disk_spec, vm_name)
    scratch_disk._Create()
    scratch_disks.append(scratch_disk)
  return scratch_disks


class KubernetesDisk(disk.BaseDisk):
  """
  Base class for Kubernetes Disks.
  """

  def __init__(self, disk_spec):
    super(KubernetesDisk, self).__init__(disk_spec)
    self.mount_point = disk_spec.mount_point

  def _Create(self):
    return

  def _Delete(self):
    return

  def Attach(self, vm):
    return

  def Detach(self):
    return

  def SetDevicePath(self, vm):
    return

  def AttachVolumeMountInfo(self, volume_mounts):
    volume_mount = {
        "mountPath": self.mount_point,
        "name": self.name
    }
    volume_mounts.append(volume_mount)


class LocalDisk(KubernetesDisk):
  """
  Implementation of Kubernetes 'emptyDir' type of volume.
  """

  def __init__(self, disk_num, disk_spec, name):
    super(LocalDisk, self).__init__(disk_spec)
    self.name = 'local-disk-%s-%s' % (name, disk_num)

  def GetDevicePath(self):
    """
    In case of LocalDisk, host's disk is mounted (empty directory from the
    host is mounted to the docker instance) and we intentionally
    prevent from formatting the device.
    """
    raise errors.Error('GetDevicePath not supported for Kubernetes local disk')

  def AttachVolumeInfo(self, volumes):
    local_volume = {
        "name": self.name,
        "emptyDir": {}
    }
    volumes.append(local_volume)


class CephDisk(KubernetesDisk):
  """
  Implementation of Kubernetes 'rbd' type of volume.
  """

  def __init__(self, disk_num, disk_spec, name):
    super(CephDisk, self).__init__(disk_spec)
    self.name = 'rbd-%s-%s' % (name, disk_num)
    self.ceph_secret = FLAGS.ceph_secret

  def _Create(self):
    """
    Creates Rados Block Device volumes and installs filesystem on them.
    """
    cmd = ['rbd', '-p', FLAGS.rbd_pool, 'create', self.name, '--size',
           str(1024 * self.disk_size)]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Creating RBD image failed: %s" % output[STDERR])

    cmd = ['rbd', 'map', FLAGS.rbd_pool + '/' + self.name]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Mapping RBD image failed: %s" % output[STDERR])
    rbd_device = output[STDOUT].rstrip()
    if '/dev/rbd' not in rbd_device:
      # Sometimes 'rbd map' command doesn't return any output.
      # Trying to find device location another way.
      cmd = ['rbd', 'showmapped']
      output = vm_util.IssueCommand(cmd)
      for image_device in output[STDOUT].split('\n'):
        if self.name in image_device:
          pattern = re.compile("/dev/rbd.*")
          output = pattern.findall(image_device)
          rbd_device = output[STDOUT].rstrip()
          break

    cmd = ['/sbin/mkfs.ext4', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Formatting partition failed: %s" % output[STDERR])

    cmd = ['rbd', 'unmap', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Unmapping block device failed: %s" % output[STDERR])

  def _Delete(self):
    """
    Deletes RBD image.
    """
    cmd = ['rbd', 'rm', FLAGS.rbd_pool + '/' + self.name]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      msg = "Removing RBD image failed. Reattempting."
      logging.warning(msg)
      raise Exception(msg)

  def AttachVolumeInfo(self, volumes):
    ceph_volume = {
        "name": self.name,
        "rbd": {
            "monitors": FLAGS.ceph_monitors,
            "pool": FLAGS.rbd_pool,
            "image": self.name,
            "keyring": FLAGS.ceph_keyring,
            "user": FLAGS.rbd_user,
            "fsType": "ext4",
            "readOnly": False
        }
    }
    if FLAGS.ceph_secret:
      ceph_volume["rbd"]["secretRef"] = {"name": FLAGS.ceph_secret}
    volumes.append(ceph_volume)

  def SetDevicePath(self, vm):
    """
    Retrieves the path to scratch disk device.
    """
    cmd = "mount | grep %s | tr -s ' ' | cut -f 1 -d ' '" % self.mount_point
    device, _ = vm.RemoteCommand(cmd)
    self.device_path = device.rstrip()

  def GetDevicePath(self):
    return self.device_path
