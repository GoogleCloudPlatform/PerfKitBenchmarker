# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
import posixpath
import re
import threading

from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import util

import yaml

FLAGS = flags.FLAGS

NVME = 'NVME'
SCSI = 'SCSI'
UBUNTU_IMAGE = 'ubuntu-14-04'
RHEL_IMAGE = 'rhel-7'
WINDOWS_IMAGE = 'windows-2012-r2'
_INSUFFICIENT_HOST_CAPACITY = ('does not have enough resources available '
                               'to fulfill the request.')
STOCKOUT_MESSAGE = ('Creation failed due to insufficient capacity indicating a '
                    'potential stockout scenario.')
_GCE_VM_CREATE_TIMEOUT = 600
_GPU_TYPE_TO_INTERAL_NAME_MAP = {
    'k80': 'nvidia-tesla-k80',
    'p100': 'nvidia-tesla-p100',
    'v100': 'nvidia-tesla-v100',
}


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
    image_family: string or None. The image family used to locate the image.
    image_project: string or None. The image project used to locate the
        specifed image.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
  """

  CLOUD = providers.GCP

  def __init__(self, *args, **kwargs):
    super(GceVmSpec, self).__init__(*args, **kwargs)
    if isinstance(self.machine_type,
                  custom_virtual_machine_spec.CustomMachineTypeSpec):
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
    if flag_values['image_family'].present:
      config_values['image_family'] = flag_values.image_family
    if flag_values['image_project'].present:
      config_values['image_project'] = flag_values.image_project
    if flag_values['gcp_node_type'].present:
      config_values['node_type'] = flag_values.gcp_node_type
    if flag_values['gcp_num_vms_per_host'].present:
      config_values['num_vms_per_host'] = flag_values.gcp_num_vms_per_host
    if flag_values['gcp_min_cpu_platform'].present:
      if (flag_values.gcp_min_cpu_platform !=
          gcp_flags.GCP_MIN_CPU_PLATFORM_NONE):
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
        'machine_type': (custom_virtual_machine_spec.MachineTypeDecoder, {}),
        'num_local_ssds': (option_decoders.IntDecoder, {'default': 0,
                                                        'min': 0}),
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_type': (option_decoders.StringDecoder, {'default': None}),
        'project': (option_decoders.StringDecoder, {'default': None}),
        'image_family': (option_decoders.StringDecoder, {'default': None}),
        'image_project': (option_decoders.StringDecoder, {'default': None}),
        'node_type': (option_decoders.StringDecoder,
                      {'default': 'n1-node-96-624'}),
        'num_vms_per_host': (option_decoders.IntDecoder, {'default': None}),
        'min_cpu_platform': (option_decoders.StringDecoder, {'default': None}),
    })
    return result


class GceSoleTenantNodeTemplate(resource.BaseResource):
  """Object representing a GCE sole tenant node template.

  Attributes:
    name: string. The name of the node template.
    node_type: string. The node type of the node template.
    zone: string. The zone of the node template, converted to region.
  """

  def __init__(self, name, node_type, zone, project):
    super(GceSoleTenantNodeTemplate, self).__init__()
    self.name = name
    self.node_type = node_type
    self.region = util.GetRegionFromZone(zone)
    self.project = project

  def _Create(self):
    """Creates the node template."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-templates', 'create', self.name)
    cmd.flags['node-type'] = self.node_type
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the node template exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-templates', 'describe', self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    """Deletes the node template."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-templates', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()


class GceSoleTenantNodeGroup(resource.BaseResource):
  """Object representing a GCE sole tenant node group.

  Attributes:
    name: string. The name of the node group.
    node_template: string. The note template of the node group.
    zone: string. The zone of the node group.
  """

  _counter_lock = threading.Lock()
  _counter = itertools.count()

  def __init__(self, node_type, zone, project):
    super(GceSoleTenantNodeGroup, self).__init__()
    with self._counter_lock:
      self.instance_number = self._counter.next()
    self.name = 'pkb-node-group-%s-%s' % (FLAGS.run_uri, self.instance_number)
    self.node_type = node_type
    self.node_template = None
    self.zone = zone
    self.project = project
    self.fill_fraction = 0.0

  def _Create(self):
    """Creates the host."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-groups', 'create', self.name)
    cmd.flags['node-template'] = self.node_template.name
    cmd.flags['target-size'] = 1
    cmd.Issue()

  def _CreateDependencies(self):
    super(GceSoleTenantNodeGroup, self)._CreateDependencies()
    node_template_name = self.name.replace('group', 'template')
    node_template = GceSoleTenantNodeTemplate(
        node_template_name, self.node_type, self.zone, self.project)
    node_template.Create()
    self.node_template = node_template

  def _DeleteDependencies(self):
    if self.node_template:
      self.node_template.Delete()

  def _Exists(self):
    """Returns True if the host exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-groups', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    """Deletes the host."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'sole-tenancy',
                             'node-groups', 'delete', self.name)
    cmd.Issue()


def GenerateAcceleratorSpecString(accelerator_type, accelerator_count):
  """Generates a string to be used to attach accelerators to a VM using gcloud.

  This function takes a cloud-agnostic accelerator type (k80, p100, etc.) and
  returns a gce-specific accelerator name (nvidia-tesla-k80, etc).

  If FLAGS.gce_accelerator_type_override is specified, the value of said flag
  will be used as the name of the accelerator.

  Args:
    accelerator_type: cloud-agnostic accelerator type (p100, k80, etc.)
    accelerator_count: number of accelerators to attach to the VM

  Returns:
    String to be used by gcloud to attach accelerators to a VM.
    Must be prepended by the flag '--accelerator'.
  """
  gce_accelerator_type = (FLAGS.gce_accelerator_type_override or
                          _GPU_TYPE_TO_INTERAL_NAME_MAP[accelerator_type])
  return 'type={0},count={1}'.format(
      gce_accelerator_type,
      accelerator_count)


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = providers.GCP
  # Subclasses should override the default image OR
  # both the image family and image_project.
  DEFAULT_IMAGE = None
  DEFAULT_IMAGE_FAMILY = None
  DEFAULT_IMAGE_PROJECT = None
  BOOT_DISK_SIZE_GB = 10
  BOOT_DISK_TYPE = gce_disk.PD_STANDARD

  _host_lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.

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
    self.image_family = vm_spec.image_family or self.DEFAULT_IMAGE_FAMILY
    self.image_project = vm_spec.image_project or self.DEFAULT_IMAGE_PROJECT
    self.backfill_image = False
    self.network = gce_network.GceNetwork.GetNetwork(self)
    self.firewall = gce_network.GceFirewall.GetFirewall()
    self.boot_disk_size = vm_spec.boot_disk_size
    self.boot_disk_type = vm_spec.boot_disk_type
    self.id = None
    self.node_type = vm_spec.node_type
    self.node_group = None
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.min_cpu_platform = vm_spec.min_cpu_platform
    self.gce_remote_access_firewall_rule = FLAGS.gce_remote_access_firewall_rule
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override

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
    if self.node_group:
      args = ['alpha']
    args.extend(['compute', 'instances', 'create', self.name])

    cmd = util.GcloudCommand(self, *args)
    if self.network.subnet_resource is not None:
      cmd.flags['subnet'] = self.network.subnet_resource.name
    else:
      cmd.flags['network'] = self.network.network_resource.name
    if self.image:
      cmd.flags['image'] = self.image
    elif self.image_family:
      cmd.flags['image-family'] = self.image_family
    if self.image_project is not None:
      cmd.flags['image-project'] = self.image_project
    cmd.flags['boot-disk-auto-delete'] = True
    cmd.flags['boot-disk-size'] = self.boot_disk_size or self.BOOT_DISK_SIZE_GB
    cmd.flags['boot-disk-type'] = self.boot_disk_type or self.BOOT_DISK_TYPE
    if self.machine_type is None:
      cmd.flags['custom-cpu'] = self.cpus
      cmd.flags['custom-memory'] = '{0}MiB'.format(self.memory_mib)
    else:
      cmd.flags['machine-type'] = self.machine_type
    if self.gpu_count:
      cmd.flags['accelerator'] = GenerateAcceleratorSpecString(self.gpu_type,
                                                               self.gpu_count)
    cmd.flags['tags'] = 'perfkitbenchmarker'
    cmd.flags['no-restart-on-failure'] = True
    if self.node_group:
      cmd.flags['node-group'] = self.node_group.name
    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    metadata_from_file = {'sshKeys': ssh_keys_path}
    parsed_metadata_from_file = flag_util.ParseKeyValuePairs(
        FLAGS.gcp_instance_metadata_from_file)
    for key, value in parsed_metadata_from_file.iteritems():
      if key in metadata_from_file:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.', key)
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
                        'from command line.', key)
        continue
      metadata[key] = value
    cmd.flags['metadata'] = ','.join(
        ['%s=%s' % (k, v) for k, v in metadata.iteritems()])

    # TODO(gareth-ferneyhough): If GCE one day supports live migration on GPUs
    #                           this can be revised.
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
      _, stderr, retcode = create_cmd.Issue(timeout=_GCE_VM_CREATE_TIMEOUT)

    if (self.use_dedicated_host and retcode and
        _INSUFFICIENT_HOST_CAPACITY in stderr and not self.num_vms_per_host):
      logging.warning(
          'Creation failed due to insufficient host capacity. A new host will '
          'be created and instance creation will be retried.')
      with self._host_lock:
        if num_hosts == len(self.host_list):
          host = GceSoleTenantNodeGroup(self.node_template,
                                        self.zone, self.project)
          self.host_list.append(host)
          host.Create()
        self.node_group = self.host_list[-1]
      raise errors.Resource.RetryableCreationError()
    if (not self.use_dedicated_host and retcode and
        _INSUFFICIENT_HOST_CAPACITY in stderr):
      logging.error(STOCKOUT_MESSAGE)
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(STOCKOUT_MESSAGE)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

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
          host = GceSoleTenantNodeGroup(self.node_type,
                                        self.zone, self.project)
          self.host_list.append(host)
          host.Create()
        self.node_group = self.host_list[-1]
        if self.num_vms_per_host:
          self.node_group.fill_fraction += 1.0 / self.num_vms_per_host

  def _DeleteDependencies(self):
    if self.node_group:
      with self._host_lock:
        if self.node_group in self.host_list:
          self.host_list.remove(self.node_group)
        if self.node_group not in self.deleted_hosts:
          self.node_group.Delete()
          self.deleted_hosts.add(self.node_group)

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
    if not self.image:
      getdisk_cmd = util.GcloudCommand(
          self, 'compute', 'disks', 'describe', self.name)
      stdout, _, _ = getdisk_cmd.Issue()
      response = json.loads(stdout)
      self.image = response['sourceImage'].split('/')[-1]
      self.backfill_image = True

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
    """Creates firewall rules for remote access if required."""

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
    # Only record image_family flag when it is used in vm creation command.
    # Note, when using non-debian/ubuntu based custom images, user will need
    # to use --os_type flag. In that case, we do not want to
    # record image_family in metadata.
    if self.backfill_image and self.image_family:
      result['image_family'] = self.image_family
    if self.image_project:
      result['image_project'] = self.image_project
    if self.use_dedicated_host:
      result['node_type'] = self.node_type
      result['num_vms_per_host'] = self.num_vms_per_host
    if self.gpu_count:
      result['gpu_type'] = self.gpu_type
      result['gpu_count'] = self.gpu_count
    if self.gce_accelerator_type_override:
      result['accelerator_type_override'] = self.gce_accelerator_type_override
    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'instances',
                             'simulate-maintenance-event', self.name)
    _, _, retcode = cmd.Issue()
    if retcode:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.')

  def DownloadPreprovisionedBenchmarkData(self, install_path, benchmark_name,
                                          filename):
    """Downloads a data file from a GCS bucket with pre-provisioned data.

    Use --gce_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      benchmark_name: Name of the benchmark associated with this data file.
      filename: The name of the file that was downloaded.
    """
    # TODO(deitz): Add retry logic.
    self.RemoteCommand(GenerateDownloadPreprovisionedBenchmarkDataCommand(
        install_path, benchmark_name, filename))


class ContainerizedGceVirtualMachine(GceVirtualMachine,
                                     linux_vm.ContainerizedDebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class DebianBasedGceVirtualMachine(GceVirtualMachine,
                                   linux_vm.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class Debian9BasedGceVirtualMachine(GceVirtualMachine,
                                    linux_vm.Debian9Mixin):
  DEFAULT_IMAGE_FAMILY = 'debian-9'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class RhelBasedGceVirtualMachine(GceVirtualMachine,
                                 linux_vm.RhelMixin):
  DEFAULT_IMAGE = RHEL_IMAGE

  def __init__(self, vm_spec):
    super(RhelBasedGceVirtualMachine, self).__init__(vm_spec)
    self.python_package_config = 'python'
    self.python_dev_package_config = 'python-devel'
    self.python_pip_package_config = 'python2-pip'


class Centos7BasedGceVirtualMachine(GceVirtualMachine,
                                    linux_vm.Centos7Mixin):
  DEFAULT_IMAGE_FAMILY = 'centos-7'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'

  def __init__(self, vm_spec):
    super(Centos7BasedGceVirtualMachine, self).__init__(vm_spec)
    self.python_package_config = 'python'
    self.python_dev_package_config = 'python-devel'
    self.python_pip_package_config = 'python2-pip'


class Ubuntu1404BasedGceVirtualMachine(GceVirtualMachine,
                                       linux_vm.Ubuntu1404Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1404-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu1604BasedGceVirtualMachine(GceVirtualMachine,
                                       linux_vm.Ubuntu1604Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1604-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu1710BasedGceVirtualMachine(GceVirtualMachine,
                                       linux_vm.Ubuntu1710Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1710'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class WindowsGceVirtualMachine(GceVirtualMachine,
                               windows_virtual_machine.WindowsMixin):
  """Class supporting Windows GCE virtual machines."""

  DEFAULT_IMAGE = WINDOWS_IMAGE
  BOOT_DISK_SIZE_GB = 50
  BOOT_DISK_TYPE = gce_disk.PD_SSD

  def __init__(self, vm_spec):
    """Initialize a Windows GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.
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


def GenerateDownloadPreprovisionedBenchmarkDataCommand(install_path,
                                                       benchmark_name,
                                                       filename):
  """Returns a string used to download preprovisioned data."""
  return 'gsutil -q cp gs://%s/%s/%s %s' % (
      FLAGS.gcp_preprovisioned_data_bucket, benchmark_name, filename,
      posixpath.join(install_path, filename))
