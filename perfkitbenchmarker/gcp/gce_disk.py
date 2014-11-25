"""Module containing classes related to GCE disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
Use 'gcutil listdisktypes' to determine valid disk types.
"""

import json

import gflags as flags

from perfkitbenchmarker import disk
from perfkitbenchmarker import perfkitbenchmarker_lib
from perfkitbenchmarker.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'image_project', None, 'The project against which all image references will'
    ' be resolved. See: '
    'https://cloud.google.com/sdk/gcloud/reference/compute/disks/create')


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
                  '--type', self.disk_type]
    create_cmd.extend(util.GetDefaultGcloudFlags(self))
    if self.image:
      create_cmd.extend(['--image', self.image])
      if FLAGS.image_project:
        create_cmd.extend(['--image-project', FLAGS.image_project])
    perfkitbenchmarker_lib.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = [FLAGS.gcloud_path,
                  'compute', 'disks',
                  'delete', self.name]
    delete_cmd.extend(util.GetDefaultGcloudFlags(self))
    perfkitbenchmarker_lib.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the disk exists."""
    getdisk_cmd = [FLAGS.gcloud_path,
                   'compute', 'disks',
                   'describe', self.name]
    getdisk_cmd.extend(util.GetDefaultGcloudFlags(self))
    stdout, _, _ = perfkitbenchmarker_lib.IssueCommand(getdisk_cmd)
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
    perfkitbenchmarker_lib.IssueRetryableCommand(attach_cmd)

  def Detach(self):
    """Detaches the disk from a VM."""
    detach_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'instances',
                  'detach-disk',
                  self.attached_vm_name,
                  '--device-name', self.name]
    detach_cmd.extend(util.GetDefaultGcloudFlags(self))
    perfkitbenchmarker_lib.IssueRetryableCommand(detach_cmd)
    self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    return '/dev/disk/by-id/google-%s' % self.name
