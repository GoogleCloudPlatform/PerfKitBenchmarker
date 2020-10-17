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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections
import itertools
import json
import logging
import posixpath
import re
import threading
import time
from absl import flags
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

import six
from six.moves import range
import yaml

FLAGS = flags.FLAGS

NVME = 'NVME'
SCSI = 'SCSI'
_INSUFFICIENT_HOST_CAPACITY = ('does not have enough resources available '
                               'to fulfill the request.')
_FAILED_TO_START_DUE_TO_PREEMPTION = (
    'Instance failed to start due to preemption.')
_GCE_VM_CREATE_TIMEOUT = 600
_GCE_NVIDIA_GPU_PREFIX = 'nvidia-tesla-'
_SHUTDOWN_SCRIPT = 'su "{user}" -c "echo | gsutil cp - {preempt_marker}"'
_WINDOWS_SHUTDOWN_SCRIPT_PS1 = 'Write-Host | gsutil cp - {preempt_marker}'
_PREEMPT_DURATION = 30


class GceUnexpectedWindowsAdapterOutputError(Exception):
  """Raised when querying the status of a windows adapter failed."""


class GceDriverDoesntSupportFeatureError(Exception):
  """Raised if there is an attempt to set a feature not supported."""


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

    # The A2 machine family, unlike other GCP offerings has a preset number of
    # GPUs, so we set them directly from the machine_type
    # https://cloud.google.com/blog/products/compute/announcing-google-cloud-a2-vm-family-based-on-nvidia-a100-gpu
    if self.machine_type and self.machine_type.startswith('a2-'):
      a2_lookup = {
          'a2-highgpu-1g': 1,
          'a2-highgpu-2g': 2,
          'a2-highgpu-4g': 4,
          'a2-highgpu-8g': 8,
          'a2-megagpu-16g': 16
      }
      self.gpu_count = a2_lookup[self.machine_type]
      self.gpu_type = virtual_machine.GPU_A100

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
      config_values['machine_type'] = yaml.safe_load(flag_values.machine_type)
    if flag_values['project'].present:
      config_values['project'] = flag_values.project
    if flag_values['image_family'].present:
      config_values['image_family'] = flag_values.image_family
    if flag_values['image_project'].present:
      config_values['image_project'] = flag_values.image_project
    if flag_values['gcp_node_type'].present:
      config_values['node_type'] = flag_values.gcp_node_type
    if flag_values['gcp_min_cpu_platform'].present:
      if (flag_values.gcp_min_cpu_platform !=
          gcp_flags.GCP_MIN_CPU_PLATFORM_NONE):
        config_values['min_cpu_platform'] = flag_values.gcp_min_cpu_platform
      else:
        # Specifying gcp_min_cpu_platform explicitly removes any config.
        config_values.pop('min_cpu_platform', None)
    if flag_values['gce_tags'].present:
      config_values['gce_tags'] = flag_values.gce_tags

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
        'machine_type': (custom_virtual_machine_spec.MachineTypeDecoder, {
            'default': None
        }),
        'num_local_ssds': (option_decoders.IntDecoder, {
            'default': 0,
            'min': 0
        }),
        'preemptible': (option_decoders.BooleanDecoder, {
            'default': False
        }),
        'boot_disk_size': (option_decoders.IntDecoder, {
            'default': None
        }),
        'boot_disk_type': (option_decoders.StringDecoder, {
            'default': None
        }),
        'project': (option_decoders.StringDecoder, {
            'default': None
        }),
        'image_family': (option_decoders.StringDecoder, {
            'default': None
        }),
        'image_project': (option_decoders.StringDecoder, {
            'default': None
        }),
        'node_type': (option_decoders.StringDecoder, {
            'default': 'n1-node-96-624'
        }),
        'min_cpu_platform': (option_decoders.StringDecoder, {
            'default': None
        }),
        'gce_tags': (option_decoders.ListDecoder, {
            'item_decoder': option_decoders.StringDecoder(),
            'default': None
        }),
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
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-templates', 'create', self.name)
    cmd.flags['node-type'] = self.node_type
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the node template exists."""
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-templates', 'describe', self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _Delete(self):
    """Deletes the node template."""
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-templates', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)


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
      self.instance_number = next(self._counter)
    self.name = 'pkb-node-group-%s-%s' % (FLAGS.run_uri, self.instance_number)
    self.node_type = node_type
    self.node_template = None
    self.zone = zone
    self.project = project
    self.fill_fraction = 0.0

  def _Create(self):
    """Creates the host."""
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-groups', 'create', self.name)
    cmd.flags['node-template'] = self.node_template.name
    cmd.flags['target-size'] = 1
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

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
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-groups', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _Delete(self):
    """Deletes the host."""
    cmd = util.GcloudCommand(self, 'compute', 'sole-tenancy',
                             'node-groups', 'delete', self.name)
    cmd.Issue(raise_on_failure=False)


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
                          _GCE_NVIDIA_GPU_PREFIX + accelerator_type)
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

  # Subclasses may override these, but are recommended to leave them up to GCE.
  BOOT_DISK_SIZE_GB = None
  BOOT_DISK_TYPE = None

  NVME_START_INDEX = 1

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
    self.spot_early_termination = False
    self.preemptible_status_code = None
    self.project = vm_spec.project or util.GetDefaultProject()
    self.image_family = vm_spec.image_family or self.DEFAULT_IMAGE_FAMILY
    self.image_project = vm_spec.image_project or self.DEFAULT_IMAGE_PROJECT
    self.backfill_image = False
    self.network = self._GetNetwork()
    self.firewall = gce_network.GceFirewall.GetFirewall()
    self.boot_disk_size = vm_spec.boot_disk_size or self.BOOT_DISK_SIZE_GB
    self.boot_disk_type = vm_spec.boot_disk_type or self.BOOT_DISK_TYPE
    self.id = None
    self.node_type = vm_spec.node_type
    self.node_group = None
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.min_cpu_platform = vm_spec.min_cpu_platform
    self.gce_remote_access_firewall_rule = FLAGS.gce_remote_access_firewall_rule
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override
    self.gce_tags = vm_spec.gce_tags
    self.gce_network_tier = FLAGS.gce_network_tier
    self.gce_shielded_secure_boot = FLAGS.gce_shielded_secure_boot

  def _GetNetwork(self):
    """Returns the GceNetwork to use."""
    return gce_network.GceNetwork.GetNetwork(self)

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
    args = ['compute', 'instances', 'create', self.name]

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
    if self.boot_disk_size:
      cmd.flags['boot-disk-size'] = self.boot_disk_size
    if self.boot_disk_type:
      cmd.flags['boot-disk-type'] = self.boot_disk_type
    if self.machine_type is None:
      cmd.flags['custom-cpu'] = self.cpus
      cmd.flags['custom-memory'] = '{0}MiB'.format(self.memory_mib)
    else:
      cmd.flags['machine-type'] = self.machine_type
    if self.gpu_count and 'a2-' not in self.machine_type:
      # A2 machine type already has predefined GPU type and count.
      cmd.flags['accelerator'] = GenerateAcceleratorSpecString(self.gpu_type,
                                                               self.gpu_count)
    cmd.flags['tags'] = ','.join(['perfkitbenchmarker'] + (self.gce_tags or []))
    cmd.flags['no-restart-on-failure'] = True
    if self.node_group:
      cmd.flags['node-group'] = self.node_group.name
    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform
    if self.gce_shielded_secure_boot:
      cmd.flags['shielded-secure-boot'] = True

    if self.network.placement_group:
      self.metadata.update(self.network.placement_group.GetResourceMetadata())
      cmd.flags['resource-policies'] = self.network.placement_group.name
      cmd.flags['maintenance-policy'] = 'TERMINATE'
    else:
      self.metadata[
          'placement_group_style'] = placement_group.PLACEMENT_GROUP_NONE

    metadata_from_file = {'sshKeys': ssh_keys_path}
    parsed_metadata_from_file = flag_util.ParseKeyValuePairs(
        FLAGS.gcp_instance_metadata_from_file)
    for key, value in six.iteritems(parsed_metadata_from_file):
      if key in metadata_from_file:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.', key)
        continue
      metadata_from_file[key] = value
    cmd.flags['metadata-from-file'] = ','.join([
        '%s=%s' % (k, v) for k, v in six.iteritems(metadata_from_file)
    ])

    metadata = {}
    metadata.update(self.boot_metadata)
    metadata.update(util.GetDefaultTags())

    additional_metadata = {}
    additional_metadata.update(self.vm_metadata)
    additional_metadata.update(
        flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata))

    for key, value in six.iteritems(additional_metadata):
      if key in metadata:
        logging.warning('Metadata "%s" is set internally. Cannot be overridden '
                        'from command line.', key)
        continue
      metadata[key] = value

    if self.preemptible:
      cmd.flags['preemptible'] = True
      preemptible_status_bucket = (
          f'gs://{FLAGS.gcp_preemptible_status_bucket}/{FLAGS.run_uri}/')
      self.preempt_marker = f'{preemptible_status_bucket}{self.name}'
      metadata.update([self._PreemptibleMetadataKeyValue()])

    cmd.flags['metadata'] = util.FormatTags(metadata)

    # TODO(user): If GCE one day supports live migration on GPUs
    #                           this can be revised.
    if (FLAGS['gce_migrate_on_maintenance'].present and
        FLAGS.gce_migrate_on_maintenance and self.gpu_count):
      raise errors.Config.InvalidValue(
          'Cannot set flag gce_migrate_on_maintenance on instances with GPUs, '
          'as it is not supported by GCP.')
    if not FLAGS.gce_migrate_on_maintenance or self.gpu_count:
      cmd.flags['maintenance-policy'] = 'TERMINATE'
    cmd.flags['local-ssd'] = (['interface={0}'.format(
        FLAGS.gce_ssd_interface)] * self.max_local_disks)
    if FLAGS.gcloud_scopes:
      cmd.flags['scopes'] = ','.join(re.split(r'[,; ]', FLAGS.gcloud_scopes))
    cmd.flags['network-tier'] = self.gce_network_tier.upper()

    return cmd

  @abc.abstractmethod
  def _PreemptibleMetadataKeyValue(self):
    """Returns the metadata (key, value) tuple for use with preemptible instance creation."""

  def _RemoveShutdownScript(self):
    # Remove shutdown script which copies status when it is interrupted
    cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'remove-metadata', self.name)
    key, _ = self._PreemptibleMetadataKeyValue()
    cmd.flags['keys'] = key
    cmd.Issue(raise_on_failure=False)

  def Reboot(self):
    if self.preemptible:
      self._UpdateInterruptibleVmStatus()
    super(GceVirtualMachine, self).Reboot()
    if self.preemptible:
      # Reboot occurred without interruption, remove preempt marker
      vm_util.IssueCommand([FLAGS.gsutil_path, 'rm', self.preempt_marker],
                           raise_on_failure=False)

  def _PreDelete(self):
    super(GceVirtualMachine, self)._PreDelete()
    if self.preemptible:
      self._RemoveShutdownScript()

  def _Create(self):
    """Create a GCE VM instance."""
    num_hosts = len(self.host_list)
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(mode='w', dir=vm_util.GetTempDir(),
                                    prefix='key-metadata') as tf:
      tf.write('%s:%s\n' % (self.user_name, public_key))
      tf.close()
      create_cmd = self._GenerateCreateCommand(tf.name)
      _, stderr, retcode = create_cmd.Issue(timeout=_GCE_VM_CREATE_TIMEOUT,
                                            raise_on_failure=False)

    if (self.use_dedicated_host and retcode and
        _INSUFFICIENT_HOST_CAPACITY in stderr):
      if self.num_vms_per_host:
        raise errors.Resource.CreationError(
            'Failed to create host: %d vms of type %s per host exceeds '
            'memory capacity limits of the host' %
            (self.num_vms_per_host, self.machine_type))
      else:
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
      logging.error(util.STOCKOUT_MESSAGE)
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(
          util.STOCKOUT_MESSAGE)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)
    if retcode:
      if (create_cmd.rate_limited and 'already exists' in stderr and
          FLAGS.retry_on_rate_limited):
        # Gcloud create commands may still create VMs despite being rate
        # limited.
        return
      if util.RATE_LIMITED_MESSAGE in stderr:
        raise errors.Benchmarks.QuotaFailure.RateLimitExceededError(stderr)
      if self.preemptible and _FAILED_TO_START_DUE_TO_PREEMPTION in stderr:
        self.spot_early_termination = True
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            'Interrupted before VM started')
      raise errors.Resource.CreationError(
          'Failed to create VM: %s return code: %s' % (stderr, retcode))

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

  def _ParseDescribeResponse(self, describe_response):
    """Sets the ID and IP addresses from a response to the describe command.

    Args:
      describe_response: JSON-loaded response to the describe gcloud command.

    Raises:
      KeyError, IndexError: If the ID and IP addresses cannot be parsed.
    """
    self.id = describe_response['id']
    network_interface = describe_response['networkInterfaces'][0]
    self.internal_ip = network_interface['networkIP']
    self.ip_address = network_interface['accessConfigs'][0]['natIP']

  @property
  def HasIpAddress(self):
    """Returns True when the IP has been retrieved from a describe response."""
    return not self._NeedsToParseDescribeResponse()

  def _NeedsToParseDescribeResponse(self):
    """Returns whether the ID and IP addresses still need to be set."""
    return not self.id or not self.internal_ip or not self.ip_address

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    if self._NeedsToParseDescribeResponse():
      getinstance_cmd = util.GcloudCommand(self, 'compute', 'instances',
                                           'describe', self.name)
      stdout, _, _ = getinstance_cmd.Issue()
      response = json.loads(stdout)
      self._ParseDescribeResponse(response)
    if not all((self.image, self.boot_disk_size, self.boot_disk_type)):
      getdisk_cmd = util.GcloudCommand(
          self, 'compute', 'disks', 'describe', self.name)
      stdout, _, _ = getdisk_cmd.Issue()
      response = json.loads(stdout)
      if not self.image:
        self.image = response['sourceImage'].split('/')[-1]
        self.backfill_image = True
      if not self.boot_disk_size:
        self.boot_disk_size = response['sizeGb']
      if not self.boot_disk_type:
        self.boot_disk_type = response['type'].split('/')[-1]

  def _Delete(self):
    """Delete a GCE VM instance."""
    delete_cmd = util.GcloudCommand(self, 'compute', 'instances', 'delete',
                                    self.name)
    delete_cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = util.GcloudCommand(self, 'compute', 'instances',
                                         'describe', self.name)
    stdout, _, _ = getinstance_cmd.Issue(suppress_warning=True,
                                         raise_on_failure=False)
    try:
      response = json.loads(stdout)
    except ValueError:
      return False
    try:
      # The VM may exist before we can fully parse the describe response for the
      # IP address or ID of the VM. For example, if the VM has a status of
      # provisioning, we can't yet parse the IP address. If this is the case, we
      # will continue to invoke the describe command in _PostCreate above.
      # However, if we do have this information now, it's better to stash it and
      # avoid invoking the describe command again.
      self._ParseDescribeResponse(response)
    except (KeyError, IndexError):
      pass
    return True

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []

    for i in range(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.LOCAL:
        name = ''
        if FLAGS.gce_ssd_interface == SCSI:
          name = 'local-ssd-%d' % self.local_disk_counter
          disk_number = self.local_disk_counter + 1
        elif FLAGS.gce_ssd_interface == NVME:
          # Device can either be /dev/nvme0n1 or /dev/nvme1n1. Find out which.
          name, _ = self.RemoteCommand('find /dev/nvme*n%d' %
                                       (self.local_disk_counter + 1))
          name = name.strip().split('/')[-1]
          disk_number = self.local_disk_counter + self.NVME_START_INDEX
        else:
          raise errors.Error('Unknown Local SSD Interface.')
        data_disk = gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
        data_disk.disk_number = disk_number
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      elif disk_spec.disk_type == disk.NFS:
        data_disk = self._GetNfsService().CreateNfsDisk()
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
    """Adds metadata to disk."""
    # vm metadata added to vm on creation.
    cmd = util.GcloudCommand(
        self, 'compute', 'disks', 'add-labels', self.name)
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
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
    if self.gce_tags:
      result['gce_tags'] = ','.join(self.gce_tags)
    if self.max_local_disks:
      result['gce_local_ssd_count'] = self.max_local_disks
      result['gce_local_ssd_interface'] = FLAGS.gce_ssd_interface
    result['gce_network_tier'] = self.gce_network_tier
    result[
        'gce_shielded_secure_boot'] = self.gce_shielded_secure_boot
    result['boot_disk_type'] = self.boot_disk_type
    result['boot_disk_size'] = self.boot_disk_size
    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.GcloudCommand(self, 'alpha', 'compute', 'instances',
                             'simulate-maintenance-event', self.name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.')

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Downloads a data file from a GCS bucket with pre-provisioned data.

    Use --gce_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
    """
    # TODO(deitz): Add retry logic.
    self.RemoteCommand(GenerateDownloadPreprovisionedDataCommand(
        install_path, module_name, filename))

  def InstallCli(self):
    """Installs the gcloud cli on this GCP vm."""
    self.Install('google_cloud_sdk')

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return FLAGS.gcp_preprovisioned_data_bucket and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename))

  def _UpdateInterruptibleVmStatus(self):
    """Updates the interruptible status if the VM was preempted."""
    # Check to see if the VM's preempt_marker file exists in GCS.
    _, _, retcode = vm_util.IssueCommand(
        [FLAGS.gsutil_path, 'stat', self.preempt_marker],
        raise_on_failure=False, suppress_warning=True)
    # The VM is preempted if the command exits without an error
    self.spot_early_termination = not bool(retcode)
    if self.spot_early_termination:
      logging.info('VM %s interrupted', self.name)

  def UpdateInterruptibleVmStatus(self):
    """Updates the interruptible status if the VM was preempted.

    Compute Engine sends a preemption notice to the instance in the form of an
    ACPI G2 Soft Off signal.  If the instance does not stop after 30 seconds,
    Compute Engine sends an ACPI G3 Mechanical Off signal to the operating
    system.

    See details at
    https://cloud.google.com/compute/docs/instances/preemptible#preemption-process


    The sequence of events:
    1. The machine starts to stop some service. At the same time, Connection
       refused and PKB can not run remote command.
    2. The machine starts to run shutdown script, the GCS bucket is updated.
    3. This machine is preempted.
    4. This machine is deleted.
    """
    if not self.preemptible:  # Only do checks on preemptible VMs
      return
    if self.spot_early_termination:  # VM already marked as preemptedor
      return
    if not self.is_failed_run:  # GCE only detects interruption in failed run.
      return
    logging.info('Failed run detected for %s', self.name)
    # https://cloud.google.com/compute/docs/instances/preemptible#preemption-process
    # Compute Engine sends a preemption notice to the instance in the form of
    # an ACPI G2 Soft Off signal.
    # If the instance does not stop after 30 seconds, Compute Engine sends an
    # ACPI G3 Mechanical Off signal to the operating system.
    end_time = time.time() + _PREEMPT_DURATION
    while time.time() <= end_time:
      self._UpdateInterruptibleVmStatus()
      if self.spot_early_termination:
        # VM marked as interrupted, stop checking
        break
      else:
        time.sleep(1)

  def IsInterruptible(self):
    """Returns whether this vm is an interruptible vm (spot vm).

    Returns: True if this vm is an interruptible vm (spot vm).
    """
    return self.preemptible

  def WasInterrupted(self):
    """Returns whether this spot vm was terminated early by GCP.

    Returns: True if this vm was terminated early by GCP.
    """
    return self.spot_early_termination

  def GetVmStatusCode(self):
    """Returns the early termination code if any.

    Returns: Early termination code.
    """
    return self.preemptible_status_code

  def GetPreemptibleStatusPollSeconds(self):
    """Get seconds between preemptible status polls.

    Returns:
      Seconds between polls
    """
    return 30


class BaseLinuxGceVirtualMachine(GceVirtualMachine, linux_vm.BaseLinuxMixin):
  """Class supporting Linux GCE virtual machines."""

  def _PreemptibleMetadataKeyValue(self):
    """See base class."""
    return 'shutdown-script', _SHUTDOWN_SCRIPT.format(
        preempt_marker=self.preempt_marker, user=self.user_name)

  def OnStartup(self):
    super(BaseLinuxGceVirtualMachine, self).OnStartup()
    if self.preemptible:
      # Prepare VM to use GCS. When an instance is interrupt, the shutdown
      # script will copy the status a GCS bucket.
      gcs.GoogleCloudStorageService.AcquireWritePermissionsLinux(self)


class Debian9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian9Mixin):
  DEFAULT_IMAGE_FAMILY = 'debian-9'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class Debian10BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian10Mixin):
  DEFAULT_IMAGE_FAMILY = 'debian-10'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class Rhel7BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel7Mixin):
  DEFAULT_IMAGE_FAMILY = 'rhel-7'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Rhel8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel8Mixin):
  DEFAULT_IMAGE_FAMILY = 'rhel-8'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class CentOs7BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOs7Mixin):
  DEFAULT_IMAGE_FAMILY = 'centos-7'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class CentOs8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOs8Mixin):
  DEFAULT_IMAGE_FAMILY = 'centos-8'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class ContainerOptimizedOsBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.ContainerOptimizedOsMixin):
  DEFAULT_IMAGE_FAMILY = 'cos-stable'
  DEFAULT_IMAGE_PROJECT = 'cos-cloud'


class CoreOsBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CoreOsMixin):
  DEFAULT_IMAGE_FAMILY = 'fedora-coreos-stable'
  DEFAULT_IMAGE_PROJECT = 'fedora-coreos-cloud'

  def __init__(self, vm_spec):
    super(CoreOsBasedGceVirtualMachine, self).__init__(vm_spec)
    # Fedora CoreOS only creates the core user
    self.user_name = 'core'


class Ubuntu1604BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu1604Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1604-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu1804BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu1804Mixin):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1804-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class BaseWindowsGceVirtualMachine(GceVirtualMachine,
                                   windows_virtual_machine.BaseWindowsMixin):
  """Class supporting Windows GCE virtual machines."""

  DEFAULT_IMAGE_PROJECT = 'windows-cloud'

  NVME_START_INDEX = 0

  def __init__(self, vm_spec):
    """Initialize a Windows GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.
    """
    super(BaseWindowsGceVirtualMachine, self).__init__(vm_spec)
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
    super(BaseWindowsGceVirtualMachine, self)._PostCreate()
    reset_password_cmd = self._GenerateResetPasswordCommand()
    stdout, _ = reset_password_cmd.IssueRetryable()
    response = json.loads(stdout)
    self.password = response['password']

  def _PreemptibleMetadataKeyValue(self):
    """See base class."""
    return 'windows-shutdown-script-ps1', _WINDOWS_SHUTDOWN_SCRIPT_PS1.format(
        preempt_marker=self.preempt_marker)

  @vm_util.Retry(
      max_retries=10,
      retryable_exceptions=(GceUnexpectedWindowsAdapterOutputError,
                            errors.VirtualMachine.RemoteCommandError))
  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping metadata key to value.
    """
    result = super(BaseWindowsGceVirtualMachine, self).GetResourceMetadata()
    result['disable_rss'] = self.disable_rss
    return result

  def DisableRSS(self):
    """Disables RSS on the GCE VM.

    Raises:
      GceDriverDoesntSupportFeatureError: If RSS is not supported.
      GceUnexpectedWindowsAdapterOutputError: If querying the RSS state
        returns unexpected output.
    """
    # First ensure that the driver supports interrupt moderation
    net_adapters, _ = self.RemoteCommand('Get-NetAdapter')
    if 'Red Hat VirtIO Ethernet Adapter' not in net_adapters:
      raise GceDriverDoesntSupportFeatureError(
          'Driver not tested with RSS disabled in PKB.')

    command = 'netsh int tcp set global rss=disabled'
    self.RemoteCommand(command)
    try:
      self.RemoteCommand('Restart-NetAdapter -Name "Ethernet"')
    except IOError:
      # Restarting the network adapter will always fail because
      # the winrm connection used to issue the command will be
      # broken.
      pass

    # Verify the setting went through
    stdout, _ = self.RemoteCommand('netsh int tcp show global')
    if 'Receive-Side Scaling State          : enabled' in stdout:
      raise GceUnexpectedWindowsAdapterOutputError('RSS failed to disable.')

  def OnStartup(self):
    super(BaseWindowsGceVirtualMachine, self).OnStartup()
    if self.preemptible:
      # Prepare VM to use GCS. When an instance is interrupt, the shutdown
      # script will copy the status a GCS bucket.
      gcs.GoogleCloudStorageService.AcquireWritePermissionsWindows(self)


class Windows2012CoreGceVirtualMachine(
    BaseWindowsGceVirtualMachine, windows_virtual_machine.Windows2012CoreMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2012-r2-core'


class Windows2016CoreGceVirtualMachine(
    BaseWindowsGceVirtualMachine, windows_virtual_machine.Windows2016CoreMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2016-core'


class Windows2019CoreGceVirtualMachine(
    BaseWindowsGceVirtualMachine, windows_virtual_machine.Windows2019CoreMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2019-core'


class Windows2012DesktopGceVirtualMachine(
    BaseWindowsGceVirtualMachine,
    windows_virtual_machine.Windows2012DesktopMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2012-r2'


class Windows2016DesktopGceVirtualMachine(
    BaseWindowsGceVirtualMachine,
    windows_virtual_machine.Windows2016DesktopMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2016'


class Windows2019DesktopGceVirtualMachine(
    BaseWindowsGceVirtualMachine,
    windows_virtual_machine.Windows2019DesktopMixin):
  DEFAULT_IMAGE_FAMILY = 'windows-2019'


def GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                              filename):
  """Returns a string used to download preprovisioned data."""
  return 'gsutil -q cp gs://%s/%s/%s %s' % (
      FLAGS.gcp_preprovisioned_data_bucket, module_name, filename,
      posixpath.join(install_path, filename))


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  return 'gsutil stat gs://%s/%s/%s' % (
      FLAGS.gcp_preprovisioned_data_bucket, module_name, filename)
