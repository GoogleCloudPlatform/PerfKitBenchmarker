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

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

PD_STANDARD = 'pd-standard'
PD_SSD = 'pd-ssd'
PD_BALANCED = 'pd-balanced'
PD_EXTREME = 'pd-extreme'
GCE_REMOTE_DISK_TYPES = [
    PD_STANDARD,
    PD_SSD,
    PD_BALANCED,
    PD_EXTREME,
]
GCE_REMOTE_EXTREME_DISK_TYPES = [
    PD_EXTREME,
]

DISK_TYPE = {disk.STANDARD: PD_STANDARD, disk.REMOTE_SSD: PD_SSD}

REGIONAL_DISK_SCOPE = 'regional'

DISK_METADATA = {
    PD_STANDARD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_BALANCED: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_SSD: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_EXTREME: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    disk.LOCAL: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.NONE,
    },
}

SCSI = 'SCSI'
NVME = 'NVME'

disk.RegisterDiskTypeMap(provider_info.GCP, DISK_TYPE)


NVME_PD_MACHINE_FAMILIES = [
    'm3'
]


def PdDriveIsNvme(vm):
  """Check if the machine uses NVMe for PD."""
  machine_type = vm.machine_type
  family = machine_type.split('-')[0].lower()
  if family in NVME_PD_MACHINE_FAMILIES:
    return True
  # In other cases, only a sub-category of a family uses nvme,
  # such as confidential VMs on Milan.
  # this is not robust, but can get refactored when
  # there is more clarity on what groups of VMs are NVMe.
  if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
    return True
  return False


def AddLabels(gcp_resource: resource.BaseResource, disk_name: str):
  """Add labels to a disk created by a service that fails to label a disk.

  Disks created by PKB (and most services) are labeled at create time.

  Args:
    gcp_resource: a resource with the project and zone of the disk.
    disk_name: the name of the disk
  """
  cmd = util.GcloudCommand(
      gcp_resource, 'compute', 'disks', 'add-labels', disk_name)
  cmd.flags['labels'] = util.MakeFormattedDefaultTags()
  cmd.Issue()


class GceDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create an GCPDisk."""

  CLOUD = provider_info.GCP

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super(GceDiskSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['gce_ssd_interface'].present:
      config_values['interface'] = flag_values.gce_ssd_interface

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(GceDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'interface': (option_decoders.StringDecoder, {
            'default': 'SCSI'
        }),
    })
    return result


class GceDisk(disk.BaseDisk):
  """Object representing an GCE Disk."""

  def __init__(self,
               disk_spec,
               name,
               zone,
               project,
               image=None,
               image_project=None,
               replica_zones=None):
    super(GceDisk, self).__init__(disk_spec)
    self.interface = disk_spec.interface
    self.attached_vm_name = None
    self.image = image
    self.image_project = image_project
    self.name = name
    self.zone = zone
    self.project = project
    self.replica_zones = replica_zones
    self.region = util.GetRegionFromZone(self.zone)
    self.provisioned_iops = None
    if self.disk_type in GCE_REMOTE_EXTREME_DISK_TYPES:
      self.provisioned_iops = FLAGS.gcp_provisioned_iops

    disk_metadata = DISK_METADATA[disk_spec.disk_type]
    if self.replica_zones:
      disk_metadata[disk.REPLICATION] = disk.REGION
      self.metadata['replica_zones'] = replica_zones
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])
    if self.disk_type == disk.LOCAL:
      self.metadata['interface'] = self.interface
    if (
        self.provisioned_iops
        and self.disk_type in GCE_REMOTE_EXTREME_DISK_TYPES
    ):
      self.metadata['provisioned_iops'] = self.provisioned_iops

  def _Create(self):
    """Creates the disk."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'create', self.name)
    cmd.flags['size'] = self.disk_size
    cmd.flags['type'] = self.disk_type
    if (
        self.provisioned_iops
        and self.disk_type in GCE_REMOTE_EXTREME_DISK_TYPES
    ):
      cmd.flags['provisioned-iops'] = self.provisioned_iops
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    if self.image:
      cmd.flags['image'] = self.image
    if self.image_project:
      cmd.flags['image-project'] = self.image_project

    if self.replica_zones:
      cmd.flags['region'] = self.region
      cmd.flags['replica-zones'] = ','.join(self.replica_zones)
      del cmd.flags['zone']

    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

  def _Delete(self):
    """Deletes the disk."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'delete', self.name)
    if self.replica_zones:
      cmd.flags['region'] = self.region
      del cmd.flags['zone']
    cmd.Issue(raise_on_failure=False)

  def _Describe(self):
    """Returns json describing the disk or None on failure."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'describe', self.name)

    if self.replica_zones:
      cmd.flags['region'] = self.region
      del cmd.flags['zone']

    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    try:
      result = json.loads(stdout)
    except ValueError:
      return None
    return result

  def _IsReady(self):
    """Returns true if the disk is ready."""
    result = self._Describe()
    if not result:
      return False
    return result.get('status') == 'READY'

  def _Exists(self):
    """Returns true if the disk exists."""
    result = self._Describe()
    return bool(result)

  @vm_util.Retry()
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

    if self.replica_zones:
      cmd.flags['disk-scope'] = REGIONAL_DISK_SCOPE

    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    # Gcloud attach-disk commands may still attach disks despite being rate
    # limited.
    if retcode:
      if (cmd.rate_limited and 'is already being used' in stderr and
          FLAGS.retry_on_rate_limited):
        return
      debug_text = ('Ran: {%s}\nReturnCode:%s\nSTDOUT: %s\nSTDERR: %s' %
                    (' '.join(cmd.GetCommand()), retcode, stdout, stderr))
      raise errors.VmUtil.CalledProcessException(
          'Command returned a non-zero exit code:\n{}'.format(debug_text))

  def Detach(self):
    """Detaches the disk from a VM."""
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'detach-disk',
                             self.attached_vm_name)
    cmd.flags['device-name'] = self.name

    if self.replica_zones:
      cmd.flags['disk-scope'] = REGIONAL_DISK_SCOPE
    cmd.IssueRetryable()
    self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type in GCE_REMOTE_DISK_TYPES and self.interface == NVME:
      return self.name
    # by default, returns this name id.
    return f'/dev/disk/by-id/google-{self.name}'
