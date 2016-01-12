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
import yaml

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

NVME = 'nvme'
SCSI = 'SCSI'
UBUNTU_IMAGE = 'ubuntu-14-04'
RHEL_IMAGE = 'rhel-7'
WINDOWS_IMAGE = 'windows-2012-r2'


class MemoryDecoder(option_decoders.StringDecoder):
  """Verifies and decodes a config option value specifying a memory size."""

  _CONFIG_MEMORY_PATTERN = re.compile(r'([0-9.]+)([GM]iB)')

  def Decode(self, value, component_full_name, flag_values):
    """Decodes memory size in MiB from a string.

    The value specified in the config must be a string representation of the
    memory size expressed in MiB or GiB. It must be an integer number of MiB
    Examples: "1280MiB", "7.5GiB".

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      int. Memory size in MiB.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    string = super(MemoryDecoder, self).Decode(value, component_full_name,
                                               flag_values)
    match = self._CONFIG_MEMORY_PATTERN.match(string)
    if not match:
      raise errors.Config.InvalidValue(
          'Invalid {0}.{1} value: "{2}". Examples of valid values: "1280MiB", '
          '"7.5GiB".'.format(component_full_name, self.option, string))
    try:
      memory_value = float(match.group(1))
    except ValueError:
      raise errors.Config.InvalidValue(
          'Invalid {0}.{1} value: "{2}". "{3}" is not a valid float.'.format(
              component_full_name, self.option, string, match.group(1)))
    memory_units = match.group(2)
    if memory_units == 'GiB':
      memory_value *= 1024
    memory_mib_int = int(memory_value)
    if memory_value != memory_mib_int:
      raise errors.Config.InvalidValue(
          'Invalid {0}.{1} value: "{2}". The specified size must be an integer '
          'number of MiB.'.format(component_full_name, self.option, string))
    return memory_mib_int


class CustomMachineTypeSpec(spec.BaseSpec):
  """Properties of a GCE custom machine type.

  Attributes:
    cpus: int. Number of vCPUs.
    memory: string. Representation of the size of memory, expressed in MiB or
        GiB. Must be an integer number of MiB (e.g. "1280MiB", "7.5GiB").
  """

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(CustomMachineTypeSpec, cls)._GetOptionDecoderConstructions()
    result.update({'cpus': (option_decoders.IntDecoder, {'min': 1}),
                   'memory': (MemoryDecoder, {})})
    return result


class MachineTypeDecoder(option_decoders.TypeVerifier):
  """Decodes the machine_type option of a GCE VM config."""

  def __init__(self, option, **kwargs):
    super(MachineTypeDecoder, self).__init__(option, (basestring, dict),
                                             **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the machine_type option of a GCE VM config.

    Args:
      value: Either a string name of a GCE machine type or a dict containing
          'cpu' and 'memory' keys describing a custom VM.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      If value is a string, returns it unmodified. Otherwise, returns the
      decoded CustomMachineTypeSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super(MachineTypeDecoder, self).Decode(value, component_full_name,
                                           flag_values)
    if isinstance(value, basestring):
      return value
    return CustomMachineTypeSpec('.'.join((component_full_name, self.option)),
                                 flag_values=flag_values, **value)


class GceVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a GceVirtualMachine.

  Attributes:
    cpus: None or int. Number of vCPUs for custom VMs.
    memory: None or string. For custom VMs, a string representation of the size
        of memory, expressed in MiB or GiB. Must be an integer number of MiB
        (e.g. "1280MiB", "7.5GiB").
    num_local_ssds: int. The number of local SSDs to attach to the instance.
    preemptible: boolean. True if the VM should be preemptible, False otherwise.
    project: string or None. The project to create the VM in.
  """

  CLOUD = providers.GCP

  def __init__(self, *args, **kwargs):
    super(GceVmSpec, self).__init__(*args, **kwargs)
    if isinstance(self.machine_type, CustomMachineTypeSpec):
      self.cpus = self.machine_type.cpus
      self.memory = self.machine_type.memory
      self.machine_type = None
    else:
      self.cpus = None
      self.memory = None

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(GceVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['gce_num_local_ssds'].present:
      config_values['num_local_ssds'] = flag_values.gce_num_local_ssds
    if flag_values['gce_preemptible_vms'].present:
      config_values['preemptible'] = flag_values.gce_preemptible_vms
    if flag_values['machine_type'].present:
      config_values['machine_type'] = yaml.load(flag_values.machine_type)
    if flag_values['project'].present:
      config_values['project'] = flag_values.project

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(GceVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'machine_type': (MachineTypeDecoder, {}),
        'num_local_ssds': (option_decoders.IntDecoder, {'default': 0,
                                                        'min': 0}),
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
        'project': (option_decoders.StringDecoder, {'default': None})})
    return result


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = providers.GCP
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None
  BOOT_DISK_SIZE_GB = 10
  BOOT_DISK_TYPE = gce_disk.PD_STANDARD

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.

    Raises:
      errors.Config.MissingOption: If the spec does not include a "machine_type"
          or both "cpus" and "memory".
      errors.Config.InvalidValue: If the spec contains both "machine_type" and
          at least one of "cpus" or "memory".
    """
    super(GceVirtualMachine, self).__init__(vm_spec)
    self.boot_metadata = {}
    self.cpus = vm_spec.cpus
    self.image = self.image or self.DEFAULT_IMAGE
    self.max_local_disks = vm_spec.num_local_ssds
    self.memory_mib = vm_spec.memory
    self.preemptible = vm_spec.preemptible
    self.project = vm_spec.project
    self.network = gce_network.GceNetwork.GetNetwork(self)
    self.firewall = gce_network.GceFirewall.GetFirewall()
    events.sample_created.connect(self.AnnotateSample, weak=False)

  def _GenerateCreateCommand(self, ssh_keys_path):
    """Generates a command to create the VM instance.

    Args:
      ssh_keys_path: string. Path to a file containing the sshKeys metadata.

    Returns:
      GcloudCommand. gcloud command to issue in order to create the VM instance.
    """
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'create', self.name)
    cmd.flags['network'] = self.network.network_resource.name
    cmd.flags['image'] = self.image
    cmd.flags['boot-disk-auto-delete'] = True
    if FLAGS.image_project:
      cmd.flags['image-project'] = FLAGS.image_project
    cmd.flags['boot-disk-size'] = self.BOOT_DISK_SIZE_GB
    cmd.flags['boot-disk-type'] = self.BOOT_DISK_TYPE
    if self.machine_type is None:
      cmd.flags['custom-cpu'] = self.cpus
      cmd.flags['custom-memory'] = '{0}MiB'.format(self.memory_mib)
    else:
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

  def _CreateDependencies(self):
    super(GceVirtualMachine, self)._CreateDependencies()
    # GCE firewall rules are created for all instances in a network.
    # Create necessary VM access rules *prior* to creating the VM, such that it
    # doesn't affect boot time.
    self.AllowRemoteAccessPorts()

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    getinstance_cmd = util.GcloudCommand(self, 'compute', 'instances',
                                         'describe', self.name)
    stdout, _, _ = getinstance_cmd.Issue()
    response = json.loads(stdout)
    network_interface = response['networkInterfaces'][0]
    self.internal_ip = network_interface['networkIP']
    self.ip_address = network_interface['accessConfigs'][0]['natIP']

  def _Delete(self):
    """Delete a GCE VM instance."""
    delete_cmd = util.GcloudCommand(self, 'compute', 'instances', 'delete',
                                    self.name)
    delete_cmd.Issue()

  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = util.GcloudCommand(self, 'compute', 'instances',
                                         'describe', self.name)
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
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'add-metadata',
                             self.name)
    cmd.flags['metadata'] = ','.join('{0}={1}'.format(key, value)
                                     for key, value in kwargs.iteritems())
    cmd.Issue()

  def AnnotateSample(self, unused_sender, benchmark_spec, sample):
    sample['metadata']['preemptible'] = self.preemptible

  def GetMachineTypeDict(self):
    """Returns a dict containing properties that specify the machine type.

    Returns:
      dict mapping string property key to value.
    """
    result = super(GceVirtualMachine, self).GetMachineTypeDict()
    for attr_name in 'cpus', 'memory_mib', 'preemptible':
      attr_value = getattr(self, attr_name)
      if attr_value:
        result[attr_name] = attr_value
    return result


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
  BOOT_DISK_TYPE = gce_disk.PD_SSD

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
