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
"""Class to represent a GCE Virtual Machine object.

Zones:
run 'gcloud compute zones list'
Machine Types:
run 'gcloud compute machine-types list'
Images:
run 'gcloud compute images list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

NVME = 'nvme'
SCSI = 'SCSI'
UBUNTU_IMAGE = 'ubuntu-14-04'
RHEL_IMAGE = 'rhel-7'
WINDOWS_IMAGE = 'windows-2012-r2'


class GceVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a GceVirtualMachine."""

  CLOUD = 'GCP'

  def __init__(self, project=None, num_local_ssds=0,
               preemptible=False, **kwargs):
    """Initializes the VmSpec.

    Args:
      num_local_ssds: The number of local ssds to attach to the instance.
      project: The project to create the VM in.
      preemptible: True if the VM should be preemptible and False otherwise.
      kwargs: The key word arguments to virtual_machine.BaseVmSpec's __init__
        method.
    """
    super(GceVmSpec, self).__init__(**kwargs)
    self.project = project
    self.num_local_ssds = num_local_ssds
    self.preemptible = preemptible

  def ApplyFlags(self, flags):
    """Apply flags to the VmSpec."""
    super(GceVmSpec, self).ApplyFlags(flags)
    self.project = flags.project or self.project
    if flags['gce_num_local_ssds'].present:
      self.num_local_ssds = flags.gce_num_local_ssds
    if flags['gce_preemptible_vms'].present:
      self.preemptible = flags.gce_preemptible_vms


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = 'GCP'
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None
  BOOT_DISK_SIZE_GB = 10
  BOOT_DISK_TYPE = gce_disk.PD_STANDARD

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(GceVirtualMachine, self).__init__(vm_spec)
    self.network = gce_network.GceNetwork.GetNetwork(self)
    self.firewall = gce_network.GceFirewall.GetFirewall()
    self.image = self.image or self.DEFAULT_IMAGE
    self.project = vm_spec.project
    self.max_local_disks = vm_spec.num_local_ssds
    self.boot_metadata = {}

    self.preemptible = vm_spec.preemptible

    events.sample_created.connect(self.AnnotateSample, weak=False)

  def _GenerateCreateCommand(self, ssh_keys_path):
    """Generates a command to create the VM instance.

    Args:
      ssh_keys_path: string. Path to a file containing the sshKeys metadata.

    Returns:
      GcloudCommand. gcloud command to issue in order to create the VM instance.
    """
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'create', self.name)
    cmd.flags['image'] = self.image
    cmd.flags['boot-disk-size'] = self.BOOT_DISK_SIZE_GB
    cmd.flags['boot-disk-type'] = self.BOOT_DISK_TYPE
    cmd.flags['machine-type'] = self.machine_type
    cmd.flags['tags'] = 'perfkitbenchmarker'
    cmd.flags['no-restart-on-failure'] = True
    cmd.flags['metadata-from-file'] = 'sshKeys=%s' % ssh_keys_path
    metadata = ['owner=%s' % FLAGS.owner]
    for key, value in self.boot_metadata.iteritems():
      metadata.append('%s=%s' % (key, value))
    cmd.flags['metadata'] = ','.join(metadata)
    if not FLAGS.gce_migrate_on_maintenance:
      cmd.flags['maintenance-policy'] = 'TERMINATE'
    ssd_interface_option = NVME if NVME in self.image else SCSI
    cmd.flags['local-ssd'] = (['interface={0}'.format(ssd_interface_option)] *
                              self.max_local_disks)
    if FLAGS.gcloud_scopes:
      cmd.flags['scopes'] = ','.join(re.split(r'[,; ]', FLAGS.gcloud_scopes))
    if self.preemptible:
      cmd.flags['preemptible'] = True
    return cmd

  def _GenerateDescribeCommand(self):
    """Generates a command to describe the VM instance.

    Returns:
      GcloudCommand. gcloud command to issue in order to describe the VM
      instance.
    """
    return util.GcloudCommand(self, 'compute', 'instances', 'describe',
                              self.name)

  def _GenerateDeleteCommand(self):
    """Generates a command to delete the VM instance.

    Returns:
      GcloudCommand. gcloud command to issue in order to delete the VM instance.
    """
    return util.GcloudCommand(self, 'compute', 'instances', 'delete', self.name)

  def _GenerateAddMetadataCommand(self, **kwargs):
    """Generates a command to add metadata to the VM instance.

    Args:
      **kwargs: dict mapping metadata name string to metadata value string.

    Returns:
      GcloudCommand. gcloud command to issue in order to add the provided
      metadata to the VM instance.
    """
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'add-metadata',
                             self.name)
    cmd.flags['metadata'] = ','.join('{0}={1}'.format(key, value)
                                     for key, value in kwargs.iteritems())
    return cmd

  def _Create(self):
    """Create a GCE VM instance."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                    prefix='key-metadata') as tf:
      tf.write('%s:%s\n' % (self.user_name, public_key))
      tf.close()
      create_cmd = self._GenerateCreateCommand(tf.name)
      create_cmd.Issue()

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    getinstance_cmd = self._GenerateDescribeCommand()
    stdout, _, _ = getinstance_cmd.Issue()
    response = json.loads(stdout)
    network_interface = response['networkInterfaces'][0]
    self.internal_ip = network_interface['networkIP']
    self.ip_address = network_interface['accessConfigs'][0]['natIP']

  def _Delete(self):
    """Delete a GCE VM instance."""
    delete_cmd = self._GenerateDeleteCommand()
    delete_cmd.Issue()

  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = self._GenerateDescribeCommand()
    stdout, _, _ = getinstance_cmd.Issue(suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []

    for i in xrange(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.LOCAL:
        name = 'local-ssd-%d' % self.local_disk_counter
        data_disk = gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
        # Local disk numbers start at 1 (0 is the system disk).
        data_disk.disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      else:
        name = '%s-data-%d-%d' % (self.name, len(self.scratch_disks), i)
        data_disk = gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
        # Remote disk numbers start at 1+max_local_disks (0 is the system disk
        # and local disks occupy 1-max_local_disks).
        data_disk.disk_number = (self.remote_disk_counter +
                                 1 + self.max_local_disks)
        self.remote_disk_counter += 1
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/disk/by-id/google-local-ssd-%d' % i
            for i in range(self.max_local_disks)]

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM via 'gcloud compute instances add-metadata'."""
    if not kwargs:
      return
    cmd = self._GenerateAddMetadataCommand(**kwargs)
    cmd.Issue()

  def AnnotateSample(self, unused_sender, benchmark_spec, sample):
    sample['metadata']['preemptible'] = self.preemptible


class ContainerizedGceVirtualMachine(GceVirtualMachine,
                                     linux_vm.ContainerizedDebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class DebianBasedGceVirtualMachine(GceVirtualMachine,
                                   linux_vm.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedGceVirtualMachine(GceVirtualMachine,
                                 linux_vm.RhelMixin):
  DEFAULT_IMAGE = RHEL_IMAGE


class WindowsGceVirtualMachine(GceVirtualMachine,
                               windows_virtual_machine.WindowsMixin):

  DEFAULT_IMAGE = WINDOWS_IMAGE
  BOOT_DISK_SIZE_GB = 50
  BOOT_DISK_TYPE = disk.REMOTE_SSD

  def __init__(self, vm_spec):
    """Initialize a Windows GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(WindowsGceVirtualMachine, self).__init__(vm_spec)
    self.boot_metadata[
        'windows-startup-script-ps1'] = windows_virtual_machine.STARTUP_SCRIPT

  def _GenerateResetPasswordCommand(self):
    """Generates a command to reset a VM user's password.

    Returns:
      GcloudCommand. gcloud command to issue in order to reset the VM user's
      password.
    """
    cmd = util.GcloudCommand(self, 'compute', 'reset-windows-password',
                             self.name)
    cmd.flags['user'] = self.user_name
    return cmd

  def _PostCreate(self):
    super(WindowsGceVirtualMachine, self)._PostCreate()
    reset_password_cmd = self._GenerateResetPasswordCommand()
    stdout, _ = reset_password_cmd.IssueRetryable()
    response = json.loads(stdout)
    self.password = response['password']
