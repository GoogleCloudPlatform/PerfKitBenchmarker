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
import copy
import datetime
import itertools
import json
import logging
import posixpath
import re
import threading
import time
from typing import Dict, List, Optional, Tuple

from absl import flags
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import gcsfuse_disk
from perfkitbenchmarker.providers.gcp import util
import six
from six.moves import range
import yaml

FLAGS = flags.FLAGS

# GCE Instance life cycle:
# https://cloud.google.com/compute/docs/instances/instance-life-cycle
RUNNING = 'RUNNING'
TERMINATED = 'TERMINATED'
INSTANCE_DELETED_STATUSES = frozenset([TERMINATED])
INSTANCE_TRANSITIONAL_STATUSES = frozenset([
    'PROVISIONING',
    'STAGING',
    'STOPPING',
    'REPAIRING',
    'SUSPENDING',
])
INSTANCE_EXISTS_STATUSES = INSTANCE_TRANSITIONAL_STATUSES | frozenset(
    [RUNNING, 'SUSPENDED', 'STOPPED']
)
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES

# Gcloud operations are complete when their 'status' is 'DONE'.
OPERATION_DONE = 'DONE'

# 2h timeout for LM notificaiton
LM_NOTIFICATION_TIMEOUT_SECONDS = 60 * 60 * 2

NVME = 'NVME'
SCSI = 'SCSI'
_INSUFFICIENT_HOST_CAPACITY = (
    'does not have enough resources available to fulfill the request.'
)
_FAILED_TO_START_DUE_TO_PREEMPTION = (
    'Instance failed to start due to preemption.'
)
_GCE_VM_CREATE_TIMEOUT = 1200
_GCE_NVIDIA_GPU_PREFIX = 'nvidia-tesla-'
_SHUTDOWN_SCRIPT = 'su "{user}" -c "echo | gsutil cp - {preempt_marker}"'
METADATA_PREEMPT_URI = (
    'http://metadata.google.internal/computeMetadata/v1/instance/preempted'
)
_METADATA_PREEMPT_CMD = (
    f'curl {METADATA_PREEMPT_URI} -H "Metadata-Flavor: Google"'
)
# Machine type to ARM architecture.
_MACHINE_TYPE_PREFIX_TO_ARM_ARCH = {
    't2a': 'neoverse-n1',
}

FIVE_MINUTE_TIMEOUT = 300


class GceRetryDescribeOperationsError(Exception):
  """Exception for retrying Exists().

  When there is an internal error with 'describe operations' or the
  'instances create' operation has not yet completed.
  """


class GceWaitUntilRunningTimeoutError(Exception):
  """Exception that is raised when WaitUntilRunning times out."""


class GceServiceUnavailableError(Exception):
  """Error for retrying _Exists when the describe output indicates that 'The service is currently unavailable'."""


class GceVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a GceVirtualMachine.

  Attributes:
    cpus: None or int. Number of vCPUs for custom VMs.
    memory: None or string. For custom VMs, a string representation of the size
      of memory, expressed in MiB or GiB. Must be an integer number of MiB (e.g.
      "1280MiB", "7.5GiB").
    num_local_ssds: int. The number of local SSDs to attach to the instance.
    preemptible: boolean. True if the VM should be preemptible, False otherwise.
    project: string or None. The project to create the VM in.
    image_family: string or None. The image family used to locate the image.
    image_project: string or None. The image project used to locate the specifed
      image.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
  """

  CLOUD = provider_info.GCP

  def __init__(self, *args, **kwargs):
    self.num_local_ssds: int = None
    self.preemptible: bool = None
    self.boot_disk_size: int = None
    self.boot_disk_type: str = None
    self.project: str = None
    self.image_family: str = None
    self.image_project: str = None
    self.node_type: str = None
    self.min_cpu_platform: str = None
    self.threads_per_core: int = None
    self.gce_tags: List[str] = None
    self.min_node_cpus: int = None
    self.subnet_name: str = None
    super(GceVmSpec, self).__init__(*args, **kwargs)

    if isinstance(
        self.machine_type, custom_virtual_machine_spec.CustomMachineTypeSpec
    ):
      logging.warning(
          'Specifying a custom machine in the format of '
          '{cpus: [NUMBER_OF_CPUS], memory: [GB_OF_MEMORY]} '
          'creates a custom machine in the n1 machine family. '
          'To create custom machines in other machine families, '
          'use [MACHINE_FAMILY]-custom-[NUMBER_CPUS]-[NUMBER_MiB] '
          'nomaclature. e.g. n2-custom-2-4096.'
      )
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
          'a2-megagpu-16g': 16,
          'a2-ultragpu-1g': 1,
          'a2-ultragpu-2g': 2,
          'a2-ultragpu-4g': 4,
          'a2-ultragpu-8g': 8,
      }
      self.gpu_count = a2_lookup[self.machine_type]
      self.gpu_type = virtual_machine.GPU_A100

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super(GceVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['gce_num_local_ssds'].present:
      config_values['num_local_ssds'] = flag_values.gce_num_local_ssds
    if flag_values['gce_ssd_interface'].present:
      config_values['ssd_interface'] = flag_values.gce_ssd_interface
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
      if (
          flag_values.gcp_min_cpu_platform
          != gcp_flags.GCP_MIN_CPU_PLATFORM_NONE
      ):
        config_values['min_cpu_platform'] = flag_values.gcp_min_cpu_platform
      else:
        # Specifying gcp_min_cpu_platform explicitly removes any config.
        config_values.pop('min_cpu_platform', None)
    if flag_values['disable_smt'].present:
      config_values['threads_per_core'] = 1
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
        'machine_type': (
            custom_virtual_machine_spec.MachineTypeDecoder,
            {'default': None},
        ),
        'ssd_interface': (
            option_decoders.StringDecoder,
            {'default': 'SCSI'},
        ),
        'num_local_ssds': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_type': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'project': (option_decoders.StringDecoder, {'default': None}),
        'image_family': (option_decoders.StringDecoder, {'default': None}),
        'image_project': (option_decoders.StringDecoder, {'default': None}),
        'node_type': (
            option_decoders.StringDecoder,
            {'default': 'n1-node-96-624'},
        ),
        'min_cpu_platform': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'threads_per_core': (option_decoders.IntDecoder, {'default': None}),
        'gce_tags': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': None,
            },
        ),
        'subnet_name': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
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
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-templates', 'create', self.name
    )
    cmd.flags['node-type'] = self.node_type
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the node template exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-templates', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _Delete(self):
    """Deletes the node template."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-templates', 'delete', self.name
    )
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
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-groups', 'create', self.name
    )
    cmd.flags['node-template'] = self.node_template.name
    cmd.flags['target-size'] = 1
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

  def _CreateDependencies(self):
    super(GceSoleTenantNodeGroup, self)._CreateDependencies()
    node_template_name = self.name.replace('group', 'template')
    node_template = GceSoleTenantNodeTemplate(
        node_template_name, self.node_type, self.zone, self.project
    )
    node_template.Create()
    self.node_template = node_template

  def _DeleteDependencies(self):
    if self.node_template:
      self.node_template.Delete()

  def _Exists(self):
    """Returns True if the host exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-groups', 'describe', self.name
    )
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _Delete(self):
    """Deletes the host."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-groups', 'delete', self.name
    )
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
  gce_accelerator_type = (
      FLAGS.gce_accelerator_type_override
      or _GCE_NVIDIA_GPU_PREFIX + accelerator_type
  )
  return 'type={0},count={1}'.format(gce_accelerator_type, accelerator_count)


def GetArmArchitecture(machine_type):
  """Returns the specific ARM processor architecture of the VM."""
  # t2a-standard-1 -> t2a
  if not machine_type:
    return None
  prefix = re.split(r'[dn]?\-', machine_type)[0]
  return _MACHINE_TYPE_PREFIX_TO_ARM_ARCH.get(prefix)


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = provider_info.GCP

  DEFAULT_IMAGE = None
  BOOT_DISK_SIZE_GB = None
  BOOT_DISK_TYPE = None

  NVME_START_INDEX = 1

  _host_lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  _LM_TIMES_SEMAPHORE = threading.Semaphore(0)
  _LM_NOTICE_SCRIPT = 'gce_maintenance_notice.py'
  _LM_SIGNAL_LOG = 'lm_signal.log'
  _LM_NOTICE_LOG = 'gce_maintenance_notice.log'

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
    self.boot_metadata_from_file = {}
    if self.boot_startup_script:
      self.boot_metadata_from_file['startup-script'] = self.boot_startup_script
    self.ssd_interface = vm_spec.ssd_interface
    self.cpus = vm_spec.cpus
    self.image = self.image or self.DEFAULT_IMAGE
    self.max_local_disks = vm_spec.num_local_ssds
    self.memory_mib = vm_spec.memory
    self.preemptible = vm_spec.preemptible
    self.spot_early_termination = False
    self.preemptible_status_code = None
    self.project = vm_spec.project or util.GetDefaultProject()
    self.image_project = vm_spec.image_project or self.GetDefaultImageProject()
    self.backfill_image = False
    self.mtu: Optional[int] = FLAGS.mtu
    self.subnet_name = vm_spec.subnet_name
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
    self.threads_per_core = vm_spec.threads_per_core
    self.gce_remote_access_firewall_rule = FLAGS.gce_remote_access_firewall_rule
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override
    self.gce_tags = vm_spec.gce_tags
    self.gce_network_tier = FLAGS.gce_network_tier
    self.gce_nic_type = FLAGS.gce_nic_type
    if not self.SupportGVNIC():
      logging.warning('Changing gce_nic_type to VIRTIO_NET')
      self.gce_nic_type = 'VIRTIO_NET'
    self.gce_egress_bandwidth_tier = gcp_flags.EGRESS_BANDWIDTH_TIER.value
    self.gce_shielded_secure_boot = FLAGS.gce_shielded_secure_boot
    # Default to GCE default (Live Migration)
    self.on_host_maintenance = None
    # https://cloud.google.com/compute/docs/instances/live-migration#gpusmaintenance
    # https://cloud.google.com/compute/docs/instances/define-instance-placement#restrictions
    # TODO(pclay): Update if this assertion ever changes
    if (
        FLAGS['gce_migrate_on_maintenance'].present
        and FLAGS.gce_migrate_on_maintenance
        and (self.gpu_count or self.network.placement_group)
    ):
      raise errors.Config.InvalidValue(
          'Cannot set flag gce_migrate_on_maintenance on instances with GPUs '
          'or network placement groups, as it is not supported by GCP.'
      )
    if (
        not FLAGS.gce_migrate_on_maintenance
        or self.gpu_count
        or self.network.placement_group
    ):
      self.on_host_maintenance = 'TERMINATE'
    self.automatic_restart = FLAGS.gce_automatic_restart
    if self.preemptible:
      self.preempt_marker = f'gs://{FLAGS.gcp_preemptible_status_bucket}/{FLAGS.run_uri}/{self.name}'
    arm_arch = GetArmArchitecture(self.machine_type)
    if arm_arch:
      # Assign host_arch to avoid running detect_host on ARM
      self.host_arch = arm_arch
      self.is_aarch64 = True
    self.image_family = vm_spec.image_family or self.GetDefaultImageFamily(
        self.is_aarch64
    )

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
    cmd.flags['async'] = True

    # Compute all flags requiring alpha first. Then if any flags are different
    # between alpha and GA, we can set the appropriate ones.
    if self.gce_egress_bandwidth_tier:
      network_performance_configs = (
          f'total-egress-bandwidth-tier={self.gce_egress_bandwidth_tier}'
      )
      cmd.flags['network-performance-configs'] = network_performance_configs

    if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
      # TODO(pclay): remove when on-host-maintenance gets promoted to GA
      cmd.use_alpha_gcloud = True
      if gcp_flags.GCE_CONFIDENTIAL_COMPUTE_TYPE.value == 'sev':
        cmd.flags.update({'confidential-compute': True})
      cmd.flags.update({'on-host-maintenance': 'TERMINATE'})

    elif self.on_host_maintenance:
      # TODO(pclay): remove when on-host-maintenance gets promoted to GA
      maintenance_flag = 'maintenance-policy'
      if cmd.use_alpha_gcloud:
        maintenance_flag = 'on-host-maintenance'
      cmd.flags[maintenance_flag] = self.on_host_maintenance

    # Bundle network-related arguments with --network-interface
    # This flag is mutually exclusive with any of these flags:
    # --address, --network, --network-tier, --subnet, --private-network-ip.
    # gcloud compute instances create ... --network-interface=
    common_ni_args = [
        f'network-tier={self.gce_network_tier.upper()}',
        f'nic-type={self.gce_nic_type.upper()}',
    ]
    if not self.assign_external_ip:
      common_ni_args.append('no-address')
    if self.network.subnet_resources:
      for subnet_resource in self.network.subnet_resources:
        cmd.additional_flags += [
            '--network-interface',
            ','.join([f'subnet={subnet_resource.name}'] + common_ni_args),
        ]
    else:
      for network_resource in self.network.network_resources:
        cmd.additional_flags += [
            '--network-interface',
            ','.join([f'network={network_resource.name}'] + common_ni_args),
        ]

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

    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    if self.threads_per_core:
      cmd.flags['threads-per-core'] = self.threads_per_core

    if self.gpu_count and self.machine_type and 'a2-' not in self.machine_type:
      # A2 machine type already has predefined GPU type and count.
      cmd.flags['accelerator'] = GenerateAcceleratorSpecString(
          self.gpu_type, self.gpu_count
      )
    cmd.flags['tags'] = ','.join(['perfkitbenchmarker'] + (self.gce_tags or []))
    if not self.automatic_restart:
      cmd.flags['no-restart-on-failure'] = True
    self.metadata['automatic_restart'] = self.automatic_restart
    if self.node_group:
      cmd.flags['node-group'] = self.node_group.name
    if self.gce_shielded_secure_boot:
      cmd.flags['shielded-secure-boot'] = True

    if self.network.placement_group:
      self.metadata.update(self.network.placement_group.GetResourceMetadata())
      cmd.flags['resource-policies'] = self.network.placement_group.name
    else:
      self.metadata['placement_group_style'] = (
          placement_group.PLACEMENT_GROUP_NONE
      )

    metadata_from_file = {'sshKeys': ssh_keys_path}
    if self.boot_metadata_from_file:
      metadata_from_file.update(self.boot_metadata_from_file)
    parsed_metadata_from_file = flag_util.ParseKeyValuePairs(
        FLAGS.gcp_instance_metadata_from_file
    )
    for key, value in six.iteritems(parsed_metadata_from_file):
      if key in metadata_from_file:
        logging.warning(
            (
                'Metadata "%s" is set internally. Cannot be overridden '
                'from command line.'
            ),
            key,
        )
        continue
      metadata_from_file[key] = value
    cmd.flags['metadata-from-file'] = ','.join(
        ['%s=%s' % (k, v) for k, v in six.iteritems(metadata_from_file)]
    )

    # passing sshKeys does not work with OS Login
    metadata = {'enable-oslogin': 'FALSE'}
    metadata.update(self.boot_metadata)
    metadata.update(util.GetDefaultTags())

    additional_metadata = {}
    additional_metadata.update(self.vm_metadata)
    additional_metadata.update(
        flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata)
    )

    for key, value in six.iteritems(additional_metadata):
      if key in metadata:
        logging.warning(
            (
                'Metadata "%s" is set internally. Cannot be overridden '
                'from command line.'
            ),
            key,
        )
        continue
      metadata[key] = value

    if self.preemptible:
      cmd.flags['preemptible'] = True
      metadata.update([self._PreemptibleMetadataKeyValue()])

    cmd.flags['metadata'] = util.FormatTags(metadata)

    cmd.flags['local-ssd'] = [
        'interface={0}'.format(self.ssd_interface)
    ] * self.max_local_disks

    create_disks = []
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      if not self.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
        continue
      # local disks are handled above in a separate gcloud flag
      if disk_spec.disk_type == disk.LOCAL:
        continue
      for i in range(disk_spec.num_striped_disks):
        name = self._GenerateDiskNamePrefix(disk_spec_id, i)
        pd_args = [
            f'name={name}',
            f'device-name={name}',
            f'size={disk_spec.disk_size}',
            f'type={disk_spec.disk_type}',
            'auto-delete=yes',
            'boot=no',
            'mode=rw',
        ]
        if disk_spec.disk_type in [
            gce_disk.PD_EXTREME,
        ]:
          pd_args += [f'provisioned-iops={FLAGS.gcp_provisioned_iops}']
        create_disks.append(','.join(pd_args))
    if create_disks:
      cmd.flags['create-disk'] = create_disks

    if FLAGS.gcloud_scopes:
      cmd.flags['scopes'] = ','.join(re.split(r'[,; ]', FLAGS.gcloud_scopes))
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    return cmd

  def _AddShutdownScript(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'add-metadata', self.name
    )
    key, value = self._PreemptibleMetadataKeyValue()
    cmd.flags['metadata'] = f'{key}={value}'
    cmd.Issue()

  def _RemoveShutdownScript(self):
    # Removes shutdown script which copies status when it is interrupted
    cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'remove-metadata', self.name
    )
    key, _ = self._PreemptibleMetadataKeyValue()
    cmd.flags['keys'] = key
    cmd.Issue(raise_on_failure=False)

  def Reboot(self):
    if self.preemptible:
      self._RemoveShutdownScript()
    super().Reboot()
    if self.preemptible:
      self._AddShutdownScript()

  def _Start(self):
    """Starts the VM."""
    start_cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'start', self.name
    )
    # After start, IP address is changed
    stdout, _, _ = start_cmd.Issue()
    response = json.loads(stdout)
    # Response is a list of size one
    self._ParseDescribeResponse(response[0])

  def _Stop(self):
    """Stops the VM."""
    stop_cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'stop', self.name
    )
    stop_cmd.Issue()

  def _PreDelete(self):
    super()._PreDelete()
    if self.preemptible:
      self._RemoveShutdownScript()

  def _Create(self):
    """Create a GCE VM instance."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(
        mode='w', dir=vm_util.GetTempDir(), prefix='key-metadata'
    ) as tf:
      tf.write('%s:%s\n' % (self.user_name, public_key))
      tf.close()
      create_cmd = self._GenerateCreateCommand(tf.name)
      stdout, stderr, retcode = create_cmd.Issue(
          timeout=_GCE_VM_CREATE_TIMEOUT, raise_on_failure=False
      )
      # Save the create operation name for use in _WaitUntilRunning
      if 'name' in stdout:
        response = json.loads(stdout)
        self.create_operation_name = response[0]['name']
    self._ParseCreateErrors(create_cmd.rate_limited, stderr, retcode)
    if not self.create_return_time:
      self.create_return_time = time.time()

  def _ParseCreateErrors(
      self, cmd_rate_limited: bool, stderr: str, retcode: int):
    """Parse error messages from a command in order to classify a failure."""
    num_hosts = len(self.host_list)
    if (
        self.use_dedicated_host
        and retcode
        and _INSUFFICIENT_HOST_CAPACITY in stderr
    ):
      if self.num_vms_per_host:
        raise errors.Resource.CreationError(
            'Failed to create host: %d vms of type %s per host exceeds '
            'memory capacity limits of the host'
            % (self.num_vms_per_host, self.machine_type)
        )
      else:
        logging.warning(
            'Creation failed due to insufficient host capacity. A new host will'
            ' be created and instance creation will be retried.'
        )
        with self._host_lock:
          if num_hosts == len(self.host_list):
            host = GceSoleTenantNodeGroup(
                self.node_type, self.zone, self.project
            )
            self.host_list.append(host)
            host.Create()
          self.node_group = self.host_list[-1]
        raise errors.Resource.RetryableCreationError()
    if (
        not self.use_dedicated_host
        and retcode
        and _INSUFFICIENT_HOST_CAPACITY in stderr
    ):
      logging.error(util.STOCKOUT_MESSAGE)
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(
          util.STOCKOUT_MESSAGE
      )
    util.CheckGcloudResponseKnownFailures(stderr, retcode)
    if retcode:
      if (
          cmd_rate_limited
          and 'already exists' in stderr
          and FLAGS.retry_on_rate_limited
      ):
        # Gcloud create commands may still create VMs despite being rate
        # limited.
        return
      if util.RATE_LIMITED_MESSAGE in stderr:
        raise errors.Benchmarks.QuotaFailure.RateLimitExceededError(stderr)
      if self.preemptible and _FAILED_TO_START_DUE_TO_PREEMPTION in stderr:
        self.spot_early_termination = True
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            'Interrupted before VM started'
        )
      if (
          re.search(r"subnetworks/\S+' is not ready", stderr)
          and gcp_flags.RETRY_GCE_SUBNETWORK_NOT_READY.value
      ):
        # Commonly occurs when simultaneously creating GKE clusters
        raise errors.Resource.RetryableCreationError(
            f'subnet is currently being updated:\n{stderr}'
        )
      if "Invalid value for field 'resource.machineType'" in stderr:
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      if re.search("The resource '.*' was not found", stderr):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      if 'features are not compatible for creating instance' in stderr:
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      if 'Internal error.' in stderr:
        raise errors.Resource.CreationInternalError(stderr)
      if re.search(
          "CPU platform type with name '.*' does not exist in zone", stderr
      ):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      if 'The service is currently unavailable' in stderr:
        raise errors.Benchmarks.KnownIntermittentError(stderr)
      raise errors.Resource.CreationError(
          f'Failed to create VM {self.name}:\n{stderr}\nreturn code: {retcode}'
      )

  def _CreateDependencies(self):
    super(GceVirtualMachine, self)._CreateDependencies()
    # Create necessary VM access rules *prior* to creating the VM, such that it
    # doesn't affect boot time.
    self.AllowRemoteAccessPorts()

    if self.use_dedicated_host:
      with self._host_lock:
        if not self.host_list or (
            self.num_vms_per_host
            and self.host_list[-1].fill_fraction + 1.0 / self.num_vms_per_host
            > 1.0
        ):
          host = GceSoleTenantNodeGroup(self.node_type, self.zone, self.project)
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
    if 'accessConfigs' in network_interface:
      self.ip_address = network_interface['accessConfigs'][0]['natIP']

    for network_interface in describe_response['networkInterfaces']:
      self.internal_ips.append(network_interface['networkIP'])

  @property
  def HasIpAddress(self):
    """Returns True when the IP has been retrieved from a describe response."""
    return not self._NeedsToParseDescribeResponse()

  def _NeedsToParseDescribeResponse(self):
    """Returns whether the ID and IP addresses still need to be set."""
    return (
        not self.id
        or not self.GetInternalIPs()
        or (self.assign_external_ip and not self.ip_address)
    )

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    if self._NeedsToParseDescribeResponse():
      getinstance_cmd = util.GcloudCommand(
          self, 'compute', 'instances', 'describe', self.name
      )
      stdout, _, _ = getinstance_cmd.Issue()
      response = json.loads(stdout)
      self._ParseDescribeResponse(response)
    if not all((self.image, self.boot_disk_size, self.boot_disk_type)):
      getdisk_cmd = util.GcloudCommand(
          self, 'compute', 'disks', 'describe', self.name
      )
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
    delete_cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'delete', self.name
    )
    delete_cmd.Issue(raise_on_failure=False)

  def _Suspend(self):
    """Suspend a GCE VM instance."""
    util.GcloudCommand(
        self, 'beta', 'compute', 'instances', 'suspend', self.name
    ).Issue()

  def _Resume(self):
    """Resume a GCE VM instance."""
    resume_cmd = util.GcloudCommand(
        self, 'beta', 'compute', 'instances', 'resume', self.name
    )

    # After resume, IP address is refreshed
    stdout, _, _ = resume_cmd.Issue()
    response = json.loads(stdout)
    # Response is a list of size one
    self._ParseDescribeResponse(response[0])

  @vm_util.Retry(
      poll_interval=1,
      log_errors=False,
      retryable_exceptions=(GceServiceUnavailableError,)
  )
  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'describe', self.name
    )
    stdout, stderr, retcode = getinstance_cmd.Issue(raise_on_failure=False)
    if 'The service is currently unavailable' in stderr:
      logging.info('instances describe command failed, retrying.')
      raise GceServiceUnavailableError()
    elif retcode and re.search(r"The resource \'.*'\ was not found", stderr):
      return False
    response = json.loads(stdout)
    status = response['status']
    return status in INSTANCE_EXISTS_STATUSES

  @vm_util.Retry(
      poll_interval=1,
      log_errors=False,
      retryable_exceptions=(GceRetryDescribeOperationsError,),
      timeout_exception=GceWaitUntilRunningTimeoutError
  )
  def _WaitUntilRunning(self):
    """Waits until the VM instances create command completes."""
    getoperation_cmd = util.GcloudCommand(
        self, 'compute', 'operations', 'describe', self.create_operation_name
    )
    stdout, _, retcode = getoperation_cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('operations describe command failed, retrying.')
      raise GceRetryDescribeOperationsError()
    response = json.loads(stdout)
    status = response['status']
    # Classify errors once the operation is complete.
    if 'error' in response:
      create_stderr = json.dumps(response['error'])
      create_retcode = 1
      self._ParseCreateErrors(getoperation_cmd.rate_limited,
                              create_stderr, create_retcode)
    # Retry if the operation is not yet DONE.
    elif status != OPERATION_DONE:
      logging.info('VM create operation has status %s; retrying operations '
                   'describe command.', status)
      raise GceRetryDescribeOperationsError()
    # Collect the time-to-running timestamp once the operation completes.
    elif not self.is_running_time:
      self.is_running_time = time.time()

  def _GenerateDiskNamePrefix(self, disk_spec_id, index):
    """Generates a deterministic disk name given disk_spec_id and index."""
    return f'{self.name}-data-{disk_spec_id}-{index}'

  def CreateScratchDisk(self, disk_spec_id, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec_id: Deterministic order of this disk_spec in the VM's list of
        disk_specs.
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []
    replica_zones = FLAGS.data_disk_zones

    for i in range(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.LOCAL:
        name = ''
        if self.ssd_interface == SCSI:
          name = 'local-ssd-%d' % self.local_disk_counter
          disk_number = self.local_disk_counter + 1
        elif self.ssd_interface == NVME:
          name = f'local-nvme-ssd-{self.local_disk_counter}'
          disk_number = self.local_disk_counter + self.NVME_START_INDEX
        else:
          raise errors.Error('Unknown Local SSD Interface.')
        # This is a local ssd, it does not need the regional pd flag.
        data_disk = gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
        data_disk.disk_number = disk_number
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      elif disk_spec.disk_type == disk.NFS:
        data_disk = self._GetNfsService().CreateNfsDisk()
      elif disk_spec.disk_type == disk.OBJECT_STORAGE:
        data_disk = gcsfuse_disk.GcsFuseDisk(disk_spec)
      else:  # disk_type is PD
        name = self._GenerateDiskNamePrefix(disk_spec_id, i)
        data_disk = gce_disk.GceDisk(
            disk_spec,
            name,
            self.zone,
            self.project,
            replica_zones=replica_zones,
        )
        if gce_disk.PdDriveIsNvme(self):
          data_disk.interface = gce_disk.NVME
        else:
          data_disk.interface = gce_disk.SCSI
        # Remote disk numbers start at 1+max_local_disks (0 is the system disk
        # and local disks occupy 1-max_local_disks).
        data_disk.disk_number = (
            self.remote_disk_counter + 1 + self.max_local_disks
        )
        self.remote_disk_counter += 1
      disks.append(data_disk)

    scratch_disk = self._CreateScratchDiskFromDisks(disk_spec, disks)
    nvme_devices = self.GetNVMEDeviceInfo()
    remote_nvme_devices = self.FindRemoteNVMEDevices(scratch_disk, nvme_devices)
    self.UpdateDevicePath(scratch_disk, remote_nvme_devices)
    self._PrepareScratchDisk(scratch_disk, disk_spec)

  def FindRemoteNVMEDevices(self, _, nvme_devices):
    """Find the paths for all remote NVME devices inside the VM."""
    local_disks = []
    if self.ssd_interface == NVME:
      for local_disk_count in range(self.max_local_disks):
        name, _ = self.RemoteCommand(f'find /dev/nvme*n{local_disk_count + 1}')
        name = name.strip().split('/')[-1]
        local_disk_name = '/dev/%s' % name
        local_disks.append(local_disk_name)
    # filter out local nvme devices from all nvme devices
    remote_nvme_devices = [
        device['DevicePath']
        for device in nvme_devices
        if device['DevicePath'] not in local_disks
    ]
    # if boot disk is nvme,
    # remove the boot disk, which is the disk with the lowest index
    if gce_disk.PdDriveIsNvme(self):
      return sorted(remote_nvme_devices)[1:]
    else:
      return sorted(remote_nvme_devices)

  def UpdateDevicePath(self, scratch_disk, remote_nvme_devices):
    """Updates the paths for all remote NVME devices inside the VM."""
    disks = scratch_disk.disks if scratch_disk.is_striped else [scratch_disk]
    # round robin assignment since we cannot tell the disks apart.
    for d in disks:
      if d.disk_type in gce_disk.GCE_REMOTE_DISK_TYPES and d.interface == NVME:
        d.name = remote_nvme_devices.pop()

  def DiskCreatedOnVMCreation(self, data_disk):
    """Returns whether the disk has been created during VM creation."""
    return self.DiskTypeCreatedOnVMCreation(data_disk.disk_type)

  def DiskTypeCreatedOnVMCreation(self, disk_type):
    """Returns whether the disk type has been created during VM creation."""
    if not FLAGS.gcp_create_disks_with_vm:
      return False
    # GCE regional disks cannot use create-on-create.
    if FLAGS.data_disk_zones:
      return False
    return disk_type in gce_disk.GCE_REMOTE_DISK_TYPES + [disk.LOCAL]

  def AddMetadata(self, **kwargs):
    """Adds metadata to disk."""
    # vm metadata added to vm on creation.
    # Add metadata to boot disk
    gce_disk.AddLabels(self, self.name)

    # Add metadata to data disks
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      if not self.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
        continue
      # local disks are not tagged
      if disk_spec.disk_type == disk.LOCAL:
        continue
      for i in range(disk_spec.num_striped_disks):
        name = self._GenerateDiskNamePrefix(disk_spec_id, i)
        cmd = util.GcloudCommand(self, 'compute', 'disks', 'add-labels', name)
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
      result['gce_local_ssd_interface'] = self.ssd_interface
    # self.network.network_resources can be None when subnet_names are populated
    network_resources = (
        self.network.network_resources or self.network.subnet_resources
    )
    result['gce_network_name'] = ','.join(
        network_resource.name for network_resource in network_resources
    )
    result['gce_subnet_name'] = ','.join(
        subnet_resource.name
        for subnet_resource in self.network.subnet_resources
    )
    result['gce_network_tier'] = self.gce_network_tier
    result['gce_nic_type'] = self.gce_nic_type
    if self.gce_egress_bandwidth_tier:
      result['gce_egress_bandwidth_tier'] = self.gce_egress_bandwidth_tier
    result['gce_shielded_secure_boot'] = self.gce_shielded_secure_boot
    result['boot_disk_type'] = self.boot_disk_type
    result['boot_disk_size'] = self.boot_disk_size
    if self.threads_per_core:
      result['threads_per_core'] = self.threads_per_core
    if self.network.mtu:
      result['mtu'] = self.network.mtu
    if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
      result['confidential_compute'] = True
      result['confidential_compute_type'] = (
          gcp_flags.GCE_CONFIDENTIAL_COMPUTE_TYPE.value
      )
    return result

  def SimulateMaintenanceWithLog(self):
    """Create a json file with information related to the vm."""
    simulate_maintenance_json = {
        'current_time': datetime.datetime.now().timestamp() * 1000,
        'instance_id': self.id,
        'project': self.project,
        'instance_name': self.name,
        'zone': self.zone,
    }
    vm_path = posixpath.join(vm_util.GetTempDir(), self._LM_SIGNAL_LOG)
    with open(vm_path, 'w+') as f:
      json.dump(simulate_maintenance_json, f, indent=2, sort_keys=True)

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.GcloudCommand(
        self,
        'compute',
        'instances',
        'simulate-maintenance-event',
        self.name,
        '--async',
    )
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode or 'error' in stdout:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.'
      )

  def SetupLMNotification(self):
    """Prepare environment for /scripts/gce_maintenance_notify.py script."""
    self.Install('pip3')
    self.RemoteCommand('sudo pip3 install requests')
    self.PushDataFile(self._LM_NOTICE_SCRIPT, vm_util.VM_TMP_DIR)

  def _GetLMNotificationCommand(self):
    """Return Remote python execution command for LM notify script."""
    vm_path = posixpath.join(vm_util.VM_TMP_DIR, self._LM_NOTICE_SCRIPT)
    server_log = self._LM_NOTICE_LOG
    return (
        f'python3 {vm_path} {gcp_flags.LM_NOTIFICATION_METADATA_NAME.value} >'
        f' {server_log} 2>&1'
    )

  def _PullLMNoticeLog(self):
    """Pull the LM Notice Log onto the local VM."""
    self.PullFile(vm_util.GetTempDir(), self._LM_NOTICE_LOG)

  def StartLMNotification(self):
    """Start meta-data server notification subscription."""

    def _Subscribe():
      self.RobustRemoteCommand(
          self._GetLMNotificationCommand(),
          timeout=LM_NOTIFICATION_TIMEOUT_SECONDS,
          ignore_failure=True,
      )
      self._PullLMNoticeLog()
      logging.info('[LM Notify] Release live migration lock.')
      self._LM_TIMES_SEMAPHORE.release()

    t = threading.Thread(target=_Subscribe)
    t.daemon = True
    t.start()

  def WaitLMNotificationRelease(self):
    """Block main thread until LM ended."""
    logging.info('[LM Notify] Wait for live migration to finish.')
    self._LM_TIMES_SEMAPHORE.acquire()
    logging.info('[LM Notify] Live migration is done.')

  def _ReadLMNoticeContents(self):
    """Read the contents of the LM Notice Log into a string."""
    return self.RemoteCommand(f'cat {self._LM_NOTICE_LOG}')[0]

  def CollectLMNotificationsTime(self):
    """Extract LM notifications from log file.

    Sample Log file to parse:
      Host_maintenance_start _at_ 1656555520.78123
      Host_maintenance_end _at_ 1656557227.63631

    Returns:
      Live migration events timing info dictionary
    """
    lm_total_time_key = 'LM_total_time'
    lm_start_time_key = 'Host_maintenance_start'
    lm_end_time_key = 'Host_maintenance_end'
    events_dict = {
        'machine_instance': self.instance_number,
        lm_start_time_key: 0,
        lm_end_time_key: 0,
        lm_total_time_key: 0,
    }
    lm_times = self._ReadLMNoticeContents()
    if not lm_times:
      return events_dict

    # Result may contain errors captured, so we need to skip them
    for event_info in lm_times.splitlines():
      event_info_parts = event_info.split(' _at_ ')
      if len(event_info_parts) == 2:
        events_dict[event_info_parts[0]] = event_info_parts[1]

    events_dict[lm_total_time_key] = float(
        events_dict[lm_end_time_key]
    ) - float(events_dict[lm_start_time_key])
    return events_dict

  def DownloadPreprovisionedData(
      self, install_path, module_name, filename, timeout=FIVE_MINUTE_TIMEOUT
  ):
    """Downloads a data file from a GCS bucket with pre-provisioned data.

    Use --gce_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisionedData, Five minutes by
        default.
    """
    # TODO(deitz): Add retry logic.
    self.RobustRemoteCommand(
        GenerateDownloadPreprovisionedDataCommand(
            install_path, module_name, filename
        ),
        timeout=timeout,
    )

  def InstallCli(self):
    """Installs the gcloud cli on this GCP vm."""
    self.Install('google_cloud_sdk')

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return FLAGS.gcp_preprovisioned_data_bucket and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename)
    )

  def _UpdateInterruptibleVmStatusThroughMetadataService(self):
    _, _, retcode = vm_util.IssueCommand(
        [FLAGS.gsutil_path, 'stat', self.preempt_marker], raise_on_failure=False
    )
    # The VM is preempted if the command exits without an error
    self.spot_early_termination = not bool(retcode)
    if self.WasInterrupted():
      return
    stdout, _ = self.RemoteCommand(self._MetadataPreemptCmd)
    self.spot_early_termination = stdout.strip().lower() == 'true'

  @property
  def _MetadataPreemptCmd(self):
    return _METADATA_PREEMPT_CMD

  def _PreemptibleMetadataKeyValue(self) -> Tuple[str, str]:
    """See base class."""
    return 'shutdown-script', _SHUTDOWN_SCRIPT.format(
        preempt_marker=self.preempt_marker, user=self.user_name
    )

  def _AcquireWritePermissionsLinux(self):
    gcs.GoogleCloudStorageService.AcquireWritePermissionsLinux(self)

  def OnStartup(self):
    super().OnStartup()
    if self.preemptible:
      # Prepare VM to use GCS. When an instance is interrupt, the shutdown
      # script will copy the status a GCS bucket.
      self._AcquireWritePermissionsLinux()

  def _UpdateInterruptibleVmStatusThroughApi(self):
    # If the run has failed then do a check that could throw an exception.
    vm_without_zone = copy.copy(self)
    vm_without_zone.zone = None
    gcloud_command = util.GcloudCommand(
        vm_without_zone, 'compute', 'operations', 'list'
    )
    gcloud_command.flags['filter'] = f'targetLink.scope():{self.name}'
    gcloud_command.flags['zones'] = self.zone
    stdout, _, _ = gcloud_command.Issue()
    self.spot_early_termination = any(
        operation['operationType'] == 'compute.instances.preempted'
        for operation in json.loads(stdout)
    )

  def UpdateInterruptibleVmStatus(self, use_api=False):
    """Updates the interruptible status if the VM was preempted."""
    if not self.IsInterruptible():
      return
    if self.WasInterrupted():
      return
    try:
      self._UpdateInterruptibleVmStatusThroughMetadataService()
    except errors.VirtualMachine.RemoteCommandError as error:
      if use_api and 'connection timed out' in str(error).lower():
        self._UpdateInterruptibleVmStatusThroughApi()

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

  def GetInterruptableStatusPollSeconds(self):
    """Get seconds between preemptible status polls.

    Returns:
      Seconds between polls
    """
    return 3600

  def SupportGVNIC(self) -> bool:
    return True

  def GetDefaultImageFamily(self, is_arm: bool) -> Optional[str]:
    return None

  def GetDefaultImageProject(self) -> Optional[str]:
    return None


class BaseLinuxGceVirtualMachine(GceVirtualMachine, linux_vm.BaseLinuxMixin):
  """Class supporting Linux GCE virtual machines.

  Currently looks for gVNIC capabilities.
  TODO(pclay): Make more generic and move to BaseLinuxMixin.
  """

  # ethtool properties output should match this regex
  _ETHTOOL_RE = re.compile(r'^(?P<key>.*?):\s*(?P<value>.*)\s*')
  # the "device" value in ethtool properties for gvnic
  _GVNIC_DEVICE_NAME = 'gve'

  # Subclasses should override the default image OR
  # both the image family and image_project.
  DEFAULT_IMAGE_FAMILY = None
  DEFAULT_ARM_IMAGE_FAMILY = None
  DEFAULT_IMAGE_PROJECT = None
  SUPPORTS_GVNIC = True

  def __init__(self, vm_spec):
    super(BaseLinuxGceVirtualMachine, self).__init__(vm_spec)
    self._gvnic_version = None

  def GetResourceMetadata(self):
    """See base class."""
    metadata = (
        super(BaseLinuxGceVirtualMachine, self).GetResourceMetadata().copy()
    )
    if self._gvnic_version:
      metadata['gvnic_version'] = self._gvnic_version

    return metadata

  def OnStartup(self):
    """See base class.  Sets the _gvnic_version."""
    super(BaseLinuxGceVirtualMachine, self).OnStartup()
    self._gvnic_version = self.GetGvnicVersion()

  def GetGvnicVersion(self) -> Optional[str]:
    """Returns the gvnic network driver version."""
    if not gcp_flags.GCE_NIC_RECORD_VERSION.value:
      return

    all_device_properties = {}
    for device_name in self._get_network_device_mtus():
      device = self._GetNetworkDeviceProperties(device_name)
      all_device_properties[device_name] = device
      driver = device.get('driver')
      driver_version = device.get('version')
      if not driver:
        logging.error(
            'Network device %s lacks a driver %s', device_name, device
        )
      elif driver == self._GVNIC_DEVICE_NAME:
        logging.info('gvnic properties %s', device)
        if driver_version:
          return driver_version
        raise ValueError(f'No version in {device}')

  def _GetNetworkDeviceProperties(self, device_name: str) -> Dict[str, str]:
    """Returns a dict of the network device properties."""
    # ethtool can exist under /usr/sbin or needs to be installed (debian9)
    if self.HasPackage('ethtool'):
      self.InstallPackages('ethtool')
    try:
      stdout, _ = self.RemoteCommand(
          f'PATH="${{PATH}}":/usr/sbin ethtool -i {device_name}'
      )
    except errors.VirtualMachine.RemoteCommandError:
      logging.info('ethtool not installed', exc_info=True)
      return {}
    properties = {}
    for line in stdout.splitlines():
      m = self._ETHTOOL_RE.match(line)
      if m:
        properties[m['key']] = m['value']
    return properties

  def SupportGVNIC(self) -> bool:
    return self.SUPPORTS_GVNIC

  # Use an explicit is_arm parameter to not accidentally assume a default
  def GetDefaultImageFamily(self, is_arm: bool) -> str:
    if not self.DEFAULT_IMAGE_FAMILY:
      raise ValueError('DEFAULT_IMAGE_FAMILY can not be None')
    if is_arm:
      if self.DEFAULT_ARM_IMAGE_FAMILY:
        return self.DEFAULT_ARM_IMAGE_FAMILY
      if 'arm64' not in self.DEFAULT_IMAGE_FAMILY:
        arm_image_family = self.DEFAULT_IMAGE_FAMILY + '-arm64'
        logging.info(
            'ARM image must be used; changing image to %s',
            arm_image_family,
        )
        return arm_image_family
    return self.DEFAULT_IMAGE_FAMILY

  def GetDefaultImageProject(self) -> str:
    if not self.DEFAULT_IMAGE_PROJECT:
      raise ValueError('DEFAULT_IMAGE_PROJECT can not be None')
    return self.DEFAULT_IMAGE_PROJECT


class Debian9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian9Mixin
):
  DEFAULT_IMAGE_FAMILY = 'debian-9'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'
  SUPPORTS_GVNIC = False

  def _BeforeSuspend(self):
    self.InstallPackages('dbus')
    self.RemoteCommand('sudo systemctl restart systemd-logind.service')


class Debian10BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian10Mixin
):
  DEFAULT_IMAGE_FAMILY = 'debian-10'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'
  SUPPORTS_GVNIC = False


class Debian11BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian11Mixin
):
  DEFAULT_IMAGE_FAMILY = 'debian-11'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class Rhel7BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel7Mixin
):
  DEFAULT_IMAGE_FAMILY = 'rhel-7'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Rhel8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel8Mixin
):
  DEFAULT_IMAGE_FAMILY = 'rhel-8'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Rhel9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel9Mixin
):
  DEFAULT_IMAGE_FAMILY = 'rhel-9'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class CentOs7BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOs7Mixin
):
  DEFAULT_IMAGE_FAMILY = 'centos-7'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class CentOsStream8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOsStream8Mixin
):
  DEFAULT_IMAGE_FAMILY = 'centos-stream-8'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class RockyLinux8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.RockyLinux8Mixin
):
  DEFAULT_IMAGE_FAMILY = 'rocky-linux-8'
  DEFAULT_IMAGE_PROJECT = 'rocky-linux-cloud'


# https://cloud.google.com/blog/products/application-modernization/introducing-rocky-linux-optimized-for-google-cloud
class RockyLinux8OptimizedBasedGceVirtualMachine(
    RockyLinux8BasedGceVirtualMachine
):
  OS_TYPE = os_types.ROCKY_LINUX8_OPTIMIZED
  DEFAULT_IMAGE_FAMILY = 'rocky-linux-8-optimized-gcp'


class RockyLinux9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.RockyLinux9Mixin
):
  DEFAULT_IMAGE_FAMILY = 'rocky-linux-9'
  DEFAULT_IMAGE_PROJECT = 'rocky-linux-cloud'


class RockyLinux9OptimizedBasedGceVirtualMachine(
    RockyLinux9BasedGceVirtualMachine
):
  OS_TYPE = os_types.ROCKY_LINUX9_OPTIMIZED
  DEFAULT_IMAGE_FAMILY = 'rocky-linux-9-optimized-gcp'


class CentOsStream9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOsStream9Mixin
):
  DEFAULT_IMAGE_FAMILY = 'centos-stream-9'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class BaseCosBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.BaseContainerLinuxMixin):
  BASE_OS_TYPE = os_types.CORE_OS
  DEFAULT_IMAGE_PROJECT = 'cos-cloud'

  def PrepareVMEnvironment(self):
    super().PrepareVMEnvironment()
    # COS mounts /home and /tmp with -o noexec, which blocks running benchmark
    # binaries.
    # TODO(user): Support reboots
    self.RemoteCommand('sudo mount -o remount,exec /home')
    self.RemoteCommand('sudo mount -o remount,exec /tmp')


class CosStableBasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS
  DEFAULT_IMAGE_FAMILY = 'cos-stable'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-stable'


class CosDevBasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS_DEV
  DEFAULT_IMAGE_FAMILY = 'cos-dev'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-dev'


class Cos105BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS105
  DEFAULT_IMAGE_FAMILY = 'cos-105-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-105-lts'


class Cos101BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS101
  DEFAULT_IMAGE_FAMILY = 'cos-101-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-101-lts'


class Cos97BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS97
  DEFAULT_IMAGE_FAMILY = 'cos-97-lts'


class Cos93BasedGceVirtualMachine(
    BaseCosBasedGceVirtualMachine, virtual_machine.DeprecatedOsMixin):
  OS_TYPE = os_types.COS93
  DEFAULT_IMAGE_FAMILY = 'cos-93-lts'
  # Not sure if it's beginning or end of October
  END_OF_LIFE = '2023-10-01'
  ALTERNATIVE_OS = os_types.COS97


class CoreOsBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CoreOsMixin
):
  DEFAULT_IMAGE_FAMILY = 'fedora-coreos-stable'
  DEFAULT_IMAGE_PROJECT = 'fedora-coreos-cloud'
  SUPPORTS_GVNIC = False

  def __init__(self, vm_spec):
    super(CoreOsBasedGceVirtualMachine, self).__init__(vm_spec)
    # Fedora CoreOS only creates the core user
    self.user_name = 'core'


class Ubuntu1804BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu1804Mixin
):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-1804-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu2004BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu2004Mixin
):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-2004-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu2204BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu2204Mixin
):
  DEFAULT_IMAGE_FAMILY = 'ubuntu-2204-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


def GenerateDownloadPreprovisionedDataCommand(
    install_path, module_name, filename
):
  """Returns a string used to download preprovisioned data."""
  return 'gsutil -q cp gs://%s/%s/%s %s' % (
      FLAGS.gcp_preprovisioned_data_bucket,
      module_name,
      filename,
      posixpath.join(install_path, filename),
  )


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  return 'gsutil stat gs://%s/%s/%s' % (
      FLAGS.gcp_preprovisioned_data_bucket,
      module_name,
      filename,
  )
