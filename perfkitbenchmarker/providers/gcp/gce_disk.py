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
"""Module containing classes related to GCE disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
Use 'gcloud compute disk-types list' to determine valid disk types.
"""

import json

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.providers import GCP

FLAGS = flags.FLAGS

PD_STANDARD = 'pd-standard'
PD_SSD = 'pd-ssd'

DISK_TYPE = {disk.STANDARD: PD_STANDARD, disk.REMOTE_SSD: PD_SSD}

DISK_METADATA = {
    PD_STANDARD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_SSD: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    disk.LOCAL: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.NONE,
    }
}

SCSI = "SCSI"
NVME = "NVME"

disk.RegisterDiskTypeMap(GCP, DISK_TYPE)


class GceDisk(disk.BaseDisk):
  """Object representing an GCE Disk."""

  def __init__(self, disk_spec, name, zone, project,
               image=None, image_project=None):
    super(GceDisk, self).__init__(disk_spec)
    self.attached_vm_name = None
    self.image = image
    self.image_project = image_project
    self.name = name
    self.zone = zone
    self.project = project
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])
    if self.disk_type == disk.LOCAL:
      self.metadata['interface'] = FLAGS.gce_ssd_interface

  def _Create(self):
    """Creates the disk."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'create', self.name)
    cmd.flags['size'] = self.disk_size
    cmd.flags['type'] = self.disk_type
    if self.image:
      cmd.flags['image'] = self.image
    if self.image_project:
      cmd.flags['image-project'] = self.image_project
    cmd.Issue()

  def _Delete(self):
    """Deletes the disk."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns true if the disk exists."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'describe', self.name)
    stdout, _, _ = cmd.Issue(suppress_warning=True)
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
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'attach-disk',
                             self.attached_vm_name)
    cmd.flags['device-name'] = self.name
    cmd.flags['disk'] = self.name
    cmd.IssueRetryable()

  def Detach(self):
    """Detaches the disk from a VM."""
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'detach-disk',
                             self.attached_vm_name)
    cmd.flags['device-name'] = self.name
    cmd.IssueRetryable()
    self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if FLAGS.gce_ssd_interface == SCSI:
      return '/dev/disk/by-id/google-%s' % self.name
    elif FLAGS.gce_ssd_interface == NVME:
      return '/dev/%s' % self.name
