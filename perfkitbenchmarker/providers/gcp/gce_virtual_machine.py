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

import collections
import itertools
import json
import logging
import re
import threading
import yaml

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

NVME = 'NVME'
SCSI = 'SCSI'
UBUNTU_IMAGE = 'ubuntu-14-04'
RHEL_IMAGE = 'rhel-7'
WINDOWS_IMAGE = 'windows-2012-r2'
_INSUFFICIENT_HOST_CAPACITY = ('does not have enough resources available '
                               'to fulfill the request.')
GPU_TYPE_K80 = 'k80'
GPU_TYPE_P100 = 'p100'
GPU_TYPE_TO_INTERAL_NAME_MAP = {
    GPU_TYPE_K80: 'nvidia-tesla-k80',
    GPU_TYPE_P100: 'nvidia-tesla-p100',
}


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
          'Invalid {0} value: "{1}". Examples of valid values: "1280MiB", '
          '"7.5GiB".'.format(self._GetOptionFullName(component_full_name),
                             string))
    try:
      memory_value = float(match.group(1))
    except ValueError:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". "{2}" is not a valid float.'.format(
              self._GetOptionFullName(component_full_name), string,
              match.group(1)))
    memory_units = match.group(2)
    if memory_units == 'GiB':
      memory_value *= 1024
    memory_mib_int = int(memory_value)
    if memory_value != memory_mib_int:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". The specified size must be an integer '
          'number of MiB.'.format(self._GetOptionFullName(component_full_name),
                                  string))
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

  def __init__(self, **kwargs):
    super(MachineTypeDecoder, self).__init__((basestring, dict), **kwargs)

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
    return CustomMachineTypeSpec(self._GetOptionFullName(component_full_name),
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
    image_project: string or None. The image project used to locate the
        specifed image.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
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
    if flag_values['gce_boot_disk_size'].present:
      config_values['boot_disk_size'] = flag_values.gce_boot_disk_size
    if flag_values['gce_boot_disk_type'].present:
      config_values['boot_disk_type'] = flag_values.gce_boot_disk_type
    if flag_values['machine_type'].present:
      config_values['machine_type'] = yaml.load(flag_values.machine_type)
    if flag_values['project'].present:
      config_values['project'] = flag_values.project
    if flag_values['image_project'].present:
      config_values['image_project'] = flag_values.image_project
    if flag_values['gcp_host_type'].present:
      config_values['host_type'] = flag_values.gcp_host_type
    if flag_values['gcp_num_vms_per_host'].present:
      config_values['num_vms_per_host'] = flag_values.gcp_num_vms_per_host
    if flag_values['gcp_min_cpu_platform'].present:
      config_values['min_cpu_platform'] = flag_values.gcp_min_cpu_platform

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
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_type': (option_decoders.StringDecoder, {'default': None}),
        'project': (option_decoders.StringDecoder, {'default': None}),
        'image_project': (option_decoders.StringDecoder, {'default': None}),
        'host_type': (option_decoders.StringDecoder,
                      {'default': 'n1-host-64-416'}),
        'num_vms_per_host': (option_decoders.IntDecoder, {'default': None}),
        'min_cpu_platform': (option_decoders.StringDecoder, {'default': None}),
    })
    return result


class GceSoleTenantHost(resource.BaseResource):
  """Object representing a GCE sole tenant host.

  Attributes:
    name: string. The name of the sole tenant host.
    host_type: string. The host type of the host.
    zone: string. The zone of the host.
  """

  _counter = itertools.count()

  def __init__(self, host_type, zone, project):
    super(GceSoleTenantHost, self).__init__()
    self.name = 'pkb-host-%s-%s' % (FLAGS.run_uri, self._counter.next())
    self.host_type = host_type
    self.zone = zone
    self.project = project
    self.fill_fraction = 0.0

  def _Create(self):
    """Creates the host."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy', 'hosts',
                             'create', self.name)
    cmd.flags['host-type'] = self.host_type
    cmd.Issue()

  def _Exists(self):
    """Returns True if the host exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy', 'hosts',
                             'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    """Deletes the host."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy', 'hosts',
                             'delete', self.name)
    cmd.Issue()


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = providers.GCP
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None
  BOOT_DISK_SIZE_GB = 10
  BOOT_DISK_TYPE = gce_disk.PD_STANDARD

  _host_lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

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
    self.project = vm_spec.project or util.GetDefaultProject()
    self.image_project = vm_spec.image_project
    self.network = gce_network.GceNetwork.GetNetwork(self)
    self.firewall = gce_network.GceFirewall.GetFirewall()
    self.boot_disk_size = vm_spec.boot_disk_size
    self.boot_disk_type = vm_spec.boot_disk_type
    self.id = None
    self.host = None
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.host_type = vm_spec.host_type
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.min_cpu_platform = vm_spec.min_cpu_platform
    self.gce_remote_access_firewall_rule = FLAGS.gce_remote_access_firewall_rule

  @property
  def host_list(self):
    """Returns the list of hosts that are compatible with this VM."""
    return self.host_map[(self.project, self.zone)]

  def _GenerateCreateCommand(self, ssh_keys_path):
    """Generates a command to create the VM instance.

    Args:
      ssh_keys_path: string. Path to a file containing the sshKeys metadata.

    Returns:
      GcloudCommand. gcloud command to issue in order to create the VM instance.
    """
    args = []
    # TODO: gcloud supports GPUs in the beta version, but we are using alpha
    # here so that we can specify min_cpu_platform in addition to attaching
    # gpus.
    if self.host or self.min_cpu_platform or self.gpu_count:
      args = ['alpha']
    args.extend(['compute', 'instances', 'create', self.name])

    cmd = util.GcloudCommand(self, *args)
    if self.network.subnet_resource is not None:
      cmd.flags['subnet'] = self.network.subnet_resource.name
    else:
      cmd.flags['network'] = self.network.network_resource.name
    cmd.flags['image'] = self.image
    cmd.flags['boot-disk-auto-delete'] = True
    if self.image_project is not None:
      cmd.flags['image-project'] = self.image_project
    cmd.flags['boot-disk-size'] = self.boot_disk_size or self.BOOT_DISK_SIZE_GB
    cmd.flags['boot-disk-type'] = self.boot_disk_type or self.BOOT_DISK_TYPE
    if self.machine_type is None:
      cmd.flags['custom-cpu'] = self.cpus
      cmd.flags['custom-memory'] = '{0}MiB'.format(self.memory_mib)
    else:
      cmd.flags['machine-type'] = self.machine_type
    if self.gpu_count:
      cmd.flags['accelerator'] = 'type={0},count={1}'.format(
          GPU_TYPE_TO_INTERAL_NAME_MAP[self.gpu_type],
          self.gpu_count)
    cmd.flags['tags'] = 'perfkitbenchmarker'
    cmd.flags['no-restart-on-failure'] = True
    if self.host:
      cmd.flags['sole-tenancy-host'] = self.host.name
    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    metadata_from_file = {'sshKeys': ssh_keys_path}
    parsed_metadata_from_file = flag_util.ParseKeyValuePairs(
        FLAGS.gcp_instance_metadata_from_file)
    for key, value in parsed_metadata_from_file.iteritems():
      if key in metadata_from_file:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.' % key)
        continue
      metadata_from_file[key] = value
    cmd.flags['metadata-from-file'] = ','.join([
        '%s=%s' % (k, v) for k, v in metadata_from_file.iteritems()
    ])

    metadata = {'owner': FLAGS.owner} if FLAGS.owner else {}
    metadata.update(self.boot_metadata)
    parsed_metadata = flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata)
    for key, value in parsed_metadata.iteritems():
      if key in metadata:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.' % key)
        continue
      metadata[key] = value
    cmd.flags['metadata'] = ','.join(
        ['%s=%s' % (k, v) for k, v in metadata.iteritems()])

    # TODO: If GCE one day supports live migration on GPUs this can be revised.
    if (FLAGS['gce_migrate_on_maintenance'].present and
        FLAGS.gce_migrate_on_maintenance and self.gpu_count):
      raise errors.Config.InvalidValue(
          'Cannot set flag gce_migrate_on_maintenance on instances with GPUs, '
          'as it is not supported by GCP.')
    if not FLAGS.gce_migrate_on_maintenance or self.gpu_count:
      cmd.flags['maintenance-policy'] = 'TERMINATE'
    ssd_interface_option = FLAGS.gce_ssd_interface
    cmd.flags['local-ssd'] = (['interface={0}'.format(ssd_interface_option)] *
                              self.max_local_disks)
    if FLAGS.gcloud_scopes:
      cmd.flags['scopes'] = ','.join(re.split(r'[,; ]', FLAGS.gcloud_scopes))
    if self.preemptible:
      cmd.flags['preemptible'] = True
    return cmd

  def _Create(self):
    """Create a GCE VM instance."""
    num_hosts = len(self.host_list)
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                    prefix='key-metadata') as tf:
      tf.write('%s:%s\n' % (self.user_name, public_key))
      tf.close()
      create_cmd = self._GenerateCreateCommand(tf.name)
      _, stderr, retcode = create_cmd.Issue()

    if (self.use_dedicated_host and retcode and
        _INSUFFICIENT_HOST_CAPACITY in stderr and not self.num_vms_per_host):
      logging.warning(
          'Creation failed due to insufficient host capacity. A new host will '
          'be created and instance creation will be retried.')
      with self._host_lock:
        if num_hosts == len(self.host_list):
          host = GceSoleTenantHost(self.host_type, self.zone, self.project)
          self.host_list.append(host)
          host.Create()
        self.host = self.host_list[-1]
      raise errors.Resource.RetryableCreationError()

  def _CreateDependencies(self):
    super(GceVirtualMachine, self)._CreateDependencies()
    # Create necessary VM access rules *prior* to creating the VM, such that it
    # doesn't affect boot time.
    self.AllowRemoteAccessPorts()

    if self.use_dedicated_host:
      with self._host_lock:
        if (not self.host_list or (self.num_vms_per_host and
                                   self.host_list[-1].fill_fraction +
                                   1.0 / self.num_vms_per_host > 1.0)):
          host = GceSoleTenantHost(self.host_type, self.zone, self.project)
          self.host_list.append(host)
          host.Create()
        self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

  def _DeleteDependencies(self):
    if self.host:
      with self._host_lock:
        if self.host in self.host_list:
          self.host_list.remove(self.host)
        if self.host not in self.deleted_hosts:
          self.host.Delete()
          self.deleted_hosts.add(self.host)

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    getinstance_cmd = util.GcloudCommand(self, 'compute', 'instances',
                                         'describe', self.name)
    stdout, _, _ = getinstance_cmd.Issue()
    response = json.loads(stdout)
    self.id = response['id']
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
        name = ''
        if FLAGS.gce_ssd_interface == SCSI:
          name = 'local-ssd-%d' % self.local_disk_counter
        elif FLAGS.gce_ssd_interface == NVME:
          name = 'nvme0n%d' % (self.local_disk_counter + 1)
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

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM via 'gcloud compute instances add-metadata'."""
    if not kwargs:
      return
    cmd = util.GcloudCommand(self, 'compute', 'instances', 'add-metadata',
                             self.name)
    cmd.flags['metadata'] = ','.join('{0}={1}'.format(key, value)
                                     for key, value in kwargs.iteritems())
    cmd.Issue()

  def AllowRemoteAccessPorts(self):
    """Creates firewall rules for remote access if required. """

    # If gce_remote_access_firewall_rule is specified, access is already
    # granted by that rule.
    # If not, GCE firewall rules are created for all instances in a
    # network.
    if not self.gce_remote_access_firewall_rule:
      super(GceVirtualMachine, self).AllowRemoteAccessPorts()

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping string property key to value.
    """
    result = super(GceVirtualMachine, self).GetResourceMetadata()
    for attr_name in 'cpus', 'memory_mib', 'preemptible', 'project':
      attr_value = getattr(self, attr_name)
      if attr_value:
        result[attr_name] = attr_value
    if self.use_dedicated_host:
      result['host_type'] = self.host_type
      result['num_vms_per_host'] = self.num_vms_per_host
    if self.gpu_count:
      result['gpu_type'] = self.gpu_type
      result['gpu_count'] = self.gpu_count
    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'instances',
                             'simulate-maintenance-event', self.name)
    _, _, retcode = cmd.Issue()
    if retcode:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.')


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
