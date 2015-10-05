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
"""Module containing classes related to GCE disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
Use 'gcloud compute disk-types list' to determine valid disk types.
"""

import json

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'image_project', None, 'The project against which all image references will'
    ' be resolved. See: '
    'https://cloud.google.com/sdk/gcloud/reference/compute/disks/create')

DISK_TYPE = {disk.STANDARD: 'pd-standard', disk.REMOTE_SSD: 'pd-ssd'}


class GceDisk(disk.BaseDisk):
  """Object representing an GCE Disk."""

  def __init__(self, disk_spec, name, zone, project, image=None):
    super(GceDisk, self).__init__(disk_spec)
    self.attached_vm_name = None
    self.image = image
    self.name = name
    self.zone = zone
    self.project = project

  def _Create(self):
    """Creates the disk."""
    create_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'disks',
                  'create', self.name,
                  '--size', str(self.disk_size),
                  '--type', DISK_TYPE[self.disk_type]]
    create_cmd.extend(util.GetDefaultGcloudFlags(self))
    if self.image:
      create_cmd.extend(['--image', self.image])
      if FLAGS.image_project:
        create_cmd.extend(['--image-project', FLAGS.image_project])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = [FLAGS.gcloud_path,
                  'compute', 'disks',
                  'delete', self.name]
    delete_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the disk exists."""
    getdisk_cmd = [FLAGS.gcloud_path,
                   'compute', 'disks',
                   'describe', self.name]
    getdisk_cmd.extend(util.GetDefaultGcloudFlags(self))
    stdout, _, _ = vm_util.IssueCommand(getdisk_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The GceVirtualMachine instance to which the disk will be attached.
    """
    self.attached_vm_name = vm.name
    attach_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'instances',
                  'attach-disk',
                  self.attached_vm_name,
                  '--device-name', self.name,
                  '--disk', self.name]
    attach_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueRetryableCommand(attach_cmd)

  def Detach(self):
    """Detaches the disk from a VM."""
    detach_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'instances',
                  'detach-disk',
                  self.attached_vm_name,
                  '--device-name', self.name]
    detach_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueRetryableCommand(detach_cmd)
    self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    return '/dev/disk/by-id/google-%s' % self.name
