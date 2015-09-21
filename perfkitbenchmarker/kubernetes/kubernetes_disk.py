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

import logging
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE

FLAGS = flags.FLAGS


flags.DEFINE_boolean('use_ceph_volumes', True,
                     'Use Ceph volumes for scratch disks')

flags.DEFINE_string('ceph_secret', None,
                    'Name of the Ceph Secret which Kubernetes uses to '
                    'authenticate with Ceph.')

flags.DEFINE_list('ceph_monitors', [],
                  'IP addresses and ports of Ceph Monitors. '
                  'Must be provided when scratch disk is required. '
                  'Example: "127.0.0.1:6789,192.168.1.1:6789"')


class CephDisk(disk.BaseDisk):

  def __init__(self, disk_num, disk_spec, name):
    super(CephDisk, self).__init__(disk_spec)
    self.disk_num = disk_num
    self.image_name = 'rbd-%s-%s' % (name, self.disk_num)
    self.ceph_secret = FLAGS.ceph_secret

  def _Create(self):
    """
    Creates Rados Block Device volumes and installs filesystem on them.
    """
    cmd = ['rbd', 'create', self.image_name, '--size',
           str(1024 * self.disk_size)]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Creating RBD image failed: %s" % output[STDERR])

    cmd = ['rbd', 'map', self.image_name]
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
        if self.image_name in image_device:
          pattern = re.compile("/dev/rbd.*")
          output = pattern.findall(image_device)
          rbd_device = output[STDOUT].rstrip()
          break

    cmd = ['mkfs.ext4', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Formatting partition failed: %s" % output[STDERR])

    cmd = ['rbd', 'unmap', rbd_device]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      raise Exception("Unmapping block device failed: %s" % output[STDERR])

  def _Delete(self):
    cmd = ['rbd', 'rm', self.image_name]
    output = vm_util.IssueCommand(cmd)
    if output[EXIT_CODE] != 0:
      msg = "Removing RBD image failed. Reattempting."
      logging.warning(msg)
      raise Exception(msg)

  def BuildVolumeBody(self):
    ceph_volume = {
        "name": self.image_name,
        "rbd": {
            "monitors": FLAGS.ceph_monitors,
            "pool": "rbd",
            "image": self.image_name,
            "secretRef": {
                "name": FLAGS.ceph_secret
            },
            "fsType": "ext4",
            "readOnly": False
        }
    }
    return ceph_volume

  def GetDevicePath(self):
    return self.mount_point

  def Attach(self):
    pass

  def Detach(self):
    pass
