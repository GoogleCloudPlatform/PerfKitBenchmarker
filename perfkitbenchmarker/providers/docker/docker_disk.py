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
"""Module containing classes related to DigitalOcean disks.
At this time, DigitalOcean does not implement any standalone disk objects,
the instances come with directly integrated storage.
"""

import json
import logging
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker.vm_util import OUTPUT_STDOUT as STDOUT,\
    OUTPUT_STDERR as STDERR, OUTPUT_EXIT_CODE as EXIT_CODE
from perfkitbenchmarker.configs import option_decoders

FLAGS = flags.FLAGS


def CreateDisks(disk_specs, vm_name):
  """
  Creates instances of KubernetesDisk child classes depending on
  scratch disk type.
  """
  scratch_disks = []
  for disk_num, disk_spec in enumerate(disk_specs):

    #disk_class = GetKubernetesDiskClass(disk_spec.disk_type)
    #scratch_disk = disk_class(disk_num, disk_spec, vm_name)
    #scratch_disk.Create()
    logging.info("Creating Disk number: " + str(disk_num))

    volume_disk = DockerDisk(disk_spec, disk_num, vm_name)
    volume_disk.Create()

    scratch_disks.append(volume_disk)
  return scratch_disks


class DockerDisk(disk.BaseDisk):
  """Dummy Object representing a Docker Disk."""
  # Will support additional disk functionalality later

  def __init__(self, disk_spec, disk_num, vm_name):
    super(DockerDisk, self).__init__(disk_spec)
    self.vm_name = vm_name
    self.disk_num = disk_num
    self.volume_name = self.vm_name + '-volume' + str(self.disk_num)

  def Attach(self, vm):
    pass

  def Detach(self):
    pass

  def GetDevicePath(self):
    raise errors.Error('GetDevicePath not supported for Docker.')

  def _Create(self):
    #volume_name = self.vm_name + '-volume' + str(self.disk_num)
    logging.info("Creating a new Docker Volume: " + self.volume_name)

    #docker volume create volume_name
    cmd = ['docker', 'volume', 'create', self.volume_name]
    output = vm_util.IssueCommand(cmd)

  def _Delete(self):
  	cmd = ['docker', 'volume', 'rm', self.volume_name]
  	output = vm_util.IssueCommand(cmd)

  def AttachVolumeInfo(self, volume_mounts):
    vol_string = (scratch_disk.volume_name + ":" + scratch_disk.mount_point)

    return vol_string
    