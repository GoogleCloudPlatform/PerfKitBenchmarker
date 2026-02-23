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
import functools
import itertools
import json
import logging
import posixpath
import re
import threading
import time
import typing
from typing import Dict, List, Tuple

from absl import flags
from perfkitbenchmarker import boot_disk
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags as pkb_flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_disk_strategies
from perfkitbenchmarker.providers.gcp import gce_network
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util
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

NVME = 'NVME'
SCSI = 'SCSI'
_INSUFFICIENT_HOST_CAPACITY = (
    'does not have enough resources available to fulfill the request.'
)
_FAILED_TO_START_DUE_TO_PREEMPTION = (
    'Instance failed to start due to preemption.'
)
_GCE_VM_CREATE_TIMEOUT = 1200
_GCE_NVIDIA_GPU_PREFIX = 'nvidia-'
_GCE_NVIDIA_TESLA_GPU_PREFIX = 'nvidia-tesla-'
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
    'c3a': 'ampere1',
    'c4a': 'neoverse-v2',
}
# When requesting a specific GPU type independently of the machine type, choose
# the most common version. When using a machine type from the
# _FIXED_GPU_MACHINE_TYPES list below  will bypass this code.
GPU_TYPE_TO_SUFFIX = {
    'a100': '-80gb',
    'h100': '-80gb',
    'h200': '-141gb',
}
# The A2 and A3 machine families, unlike some other GCP offerings, have a
# preset type and number of GPUs, so we set those attributes directly from the
# machine_type.
_FIXED_GPU_MACHINE_TYPES = {
    # A100 GPUs
    # https://cloud.google.com/blog/products/compute/announcing-google-cloud-a2-vm-family-based-on-nvidia-a100-gpu
    'a2-highgpu-1g': (virtual_machine_spec.GPU_A100, 1),
    'a2-highgpu-2g': (virtual_machine_spec.GPU_A100, 2),
    'a2-highgpu-4g': (virtual_machine_spec.GPU_A100, 4),
    'a2-highgpu-8g': (virtual_machine_spec.GPU_A100, 8),
    'a2-megagpu-16g': (virtual_machine_spec.GPU_A100, 16),
    'a2-ultragpu-1g': (virtual_machine_spec.GPU_A100, 1),
    'a2-ultragpu-2g': (virtual_machine_spec.GPU_A100, 2),
    'a2-ultragpu-4g': (virtual_machine_spec.GPU_A100, 4),
    'a2-ultragpu-8g': (virtual_machine_spec.GPU_A100, 8),
    # H100 GPUs
    'a3-highgpu-1g': (virtual_machine_spec.GPU_H100, 1),
    'a3-highgpu-2g': (virtual_machine_spec.GPU_H100, 2),
    'a3-highgpu-4g': (virtual_machine_spec.GPU_H100, 4),
    'a3-highgpu-8g': (virtual_machine_spec.GPU_H100, 8),
    'a3-megagpu-8g': (virtual_machine_spec.GPU_H100, 8),
    # B200 GPUs
    # https://cloud.google.com/compute/docs/gpus#b200-gpus
    'a4-highgpu-8g': (virtual_machine_spec.GPU_B200, 8),
    # L4 GPUs
    # https://cloud.google.com/compute/docs/accelerator-optimized-machines#g2-vms
    'g2-standard-4': (virtual_machine_spec.GPU_L4, 1),
    'g2-standard-8': (virtual_machine_spec.GPU_L4, 1),
    'g2-standard-12': (virtual_machine_spec.GPU_L4, 1),
    'g2-standard-16': (virtual_machine_spec.GPU_L4, 1),
    'g2-standard-24': (virtual_machine_spec.GPU_L4, 2),
    'g2-standard-32': (virtual_machine_spec.GPU_L4, 1),
    'g2-standard-48': (virtual_machine_spec.GPU_L4, 4),
    'g2-standard-96': (virtual_machine_spec.GPU_L4, 8),
}

PKB_SKIPPED_TEARDOWN_METADATA_KEY = 'pkb_skipped_teardown'


class GceRetryDescribeOperationsError(Exception):
  """Exception for retrying Exists().

  When there is an internal error with 'describe operations' or the
  'instances create' operation has not yet completed.
  """


class GceServiceUnavailableError(Exception):
  """Error for retrying _Exists when the describe output indicates that 'The service is currently unavailable'."""


class GceVmSpec(virtual_machine_spec.BaseVmSpec):
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
    image_project: string or None. The image project used to locate the
      specified image.
    boot_disk_size: None or int. The size of the boot disk in GB.
    boot_disk_type: string or None. The type of the boot disk.
    boot_disk_iops: None or int. I/O operations per second
    boot_disk_throughtput: None or int. Number of throughput mb per second
  """

  CLOUD = provider_info.GCP

  def __init__(self, *args, **kwargs):
    self.num_local_ssds: int = None
    self.preemptible: bool = None
    self.boot_disk_size: int = None
    self.boot_disk_type: str = None
    self.boot_disk_iops: int = None
    self.boot_disk_throughput: int = None
    # But prefer GetProject()
    self.project: str = None
    self.image_family: str = None
    self.image_project: str = None
    self.node_type: str = None
    self.min_cpu_platform: str = None
    self.threads_per_core: int = None
    self.visible_core_count: int = None
    self.gce_tags: List[str] = None
    self.min_node_cpus: int = None
    self.subnet_names: List[str] = None
    self.ssd_interface: str
    self.mtu: int | None
    super().__init__(*args, **kwargs)
    # Copy num_local_ssds from flag values to max_local_disks.
    self.max_local_disks: int | None = self.num_local_ssds
    self.boot_disk_spec = boot_disk.BootDiskSpec(
        self.boot_disk_size,
        self.boot_disk_type,
        self.boot_disk_iops,
        self.boot_disk_throughput,
    )
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
    if (
        self.machine_type
        and self.machine_type in gce_disk.FIXED_SSD_MACHINE_TYPES
    ):
      self.max_local_disks = gce_disk.FIXED_SSD_MACHINE_TYPES[self.machine_type]
      self.ssd_interface = 'NVME'
    # For certain machine families, we need to explicitly set the GPU type
    # and counts. See the _FIXED_GPU_MACHINE_TYPES dictionary for more details.
    if self.machine_type and self.machine_type in _FIXED_GPU_MACHINE_TYPES:
      self.gpu_type = _FIXED_GPU_MACHINE_TYPES[self.machine_type][0]
      self.gpu_count = _FIXED_GPU_MACHINE_TYPES[self.machine_type][1]

  @functools.lru_cache(maxsize=1)
  def GetProject(self) -> str:
    """Returns the current project or default."""
    return self.project or util.GetDefaultProject()

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
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['mtu'].present:
      config_values['mtu'] = flag_values.mtu
    if flag_values['gce_num_local_ssds'].present:
      config_values['num_local_ssds'] = flag_values.gce_num_local_ssds
    if flag_values['gce_ssd_interface'].present:
      config_values['ssd_interface'] = flag_values.gce_ssd_interface
    if flag_values['gce_preemptible_vms'].present:
      config_values['preemptible'] = flag_values.gce_preemptible_vms
    if flag_values['gce_boot_disk_throughput'].present:
      config_values['boot_disk_throughput'] = (
          flag_values.gce_boot_disk_throughput
      )
    if flag_values['gce_boot_disk_iops'].present:
      config_values['boot_disk_iops'] = flag_values.gce_boot_disk_iops
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
    if flag_values['disable_smt'].present and flag_values.disable_smt:
      config_values['threads_per_core'] = 1
    if flag_values['visible_core_count'].present:
      config_values['visible_core_count'] = flag_values.visible_core_count
    # Convert YAML to correct type even if only one element.
    if 'gce_tags' in config_values and isinstance(
        config_values['gce_tags'], str
    ):
      config_values['gce_tags'] = [config_values['gce_tags']]
    if flag_values['gce_tags'].present:
      config_values['gce_tags'] = flag_values.gce_tags
    if flag_values['gce_subnet_name'].present:
      config_values['subnet_names'] = flag_values.gce_subnet_name

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'machine_type': (
            custom_virtual_machine_spec.MachineTypeDecoder,
            {'default': None},
        ),
        'ssd_interface': (
            option_decoders.StringDecoder,
            {'default': 'NVME'},
        ),
        'num_local_ssds': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'preemptible': (option_decoders.BooleanDecoder, {'default': False}),
        'boot_disk_type': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'boot_disk_iops': (option_decoders.IntDecoder, {'default': None}),
        'boot_disk_throughput': (
            option_decoders.IntDecoder,
            {'default': None},
        ),
        'mtu': (option_decoders.IntDecoder, {'default': None}),
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
        'visible_core_count': (option_decoders.IntDecoder, {'default': None}),
        'gce_tags': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': None,
            },
        ),
        'subnet_names': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': None,
            },
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
    super().__init__()
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
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

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
    super().__init__()
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
    assert self.node_template is not None
    cmd.flags['node-template'] = self.node_template.name
    cmd.flags['target-size'] = 1
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)

  def _CreateDependencies(self):
    super()._CreateDependencies()
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

  def _IsReady(self) -> bool:
    """Returns True if the node-group is ready."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-groups', 'describe', self.name
    )
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    return json.loads(stdout).get('status') == 'READY'

  def _Delete(self):
    """Deletes the host."""
    cmd = util.GcloudCommand(
        self, 'compute', 'sole-tenancy', 'node-groups', 'delete', self.name
    )
    cmd.Issue(raise_on_failure=False)


def GetGceAcceleratorType(accelerator_type: str) -> str:
  """Generates a GCE speicifc accelerator type string.

  This function takes a cloud-agnostic accelerator type (k80, p100, etc.) and
  returns a gce-specific accelerator name (nvidia-tesla-k80, etc).

  If FLAGS.gce_accelerator_type_override is specified, the value of said flag
  will be used as the name of the accelerator.

  Args:
    accelerator_type: cloud-agnostic accelerator type (p100, k80, etc.)

  Returns:
    GCE specific accelerator type.
  """
  gce_accelerator_type = FLAGS.gce_accelerator_type_override or (
      (
          _GCE_NVIDIA_TESLA_GPU_PREFIX
          if accelerator_type in virtual_machine_spec.TESLA_GPU_TYPES
          else _GCE_NVIDIA_GPU_PREFIX
      )
      + accelerator_type
  )
  if accelerator_type in GPU_TYPE_TO_SUFFIX:
    gce_accelerator_type += GPU_TYPE_TO_SUFFIX[accelerator_type]
  return gce_accelerator_type


def GenerateAcceleratorSpecString(
    accelerator_type: str, accelerator_count: int
) -> str:
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
  gce_accelerator_type = GetGceAcceleratorType(accelerator_type)
  return 'type={},count={}'.format(gce_accelerator_type, accelerator_count)


def GetArmArchitecture(machine_type):
  """Returns the specific ARM processor architecture of the VM."""
  # t2a-standard-1 -> t2a
  if not machine_type:
    return None
  prefix = re.split(r'[dn]?\-', machine_type)[0]
  return _MACHINE_TYPE_PREFIX_TO_ARM_ARCH.get(prefix)


def IsLiveMigratableConfidentialCompute(machine_type: str):
  """Returns True if the provided machine type is both confidential computing and can have its maintenance policy set to MIGRATE."""
  family = machine_type.split('-')[0]
  match gcp_flags.GCE_CONFIDENTIAL_COMPUTE_TYPE.value:
    case 'sev':
      return family in (
          'n2d',
      )
  return False


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  CLOUD = provider_info.GCP

  DEFAULT_IMAGE = None

  NVME_START_INDEX = 1

  _host_lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine_spec.BaseVmSpec object of the vm.

    Raises:
      errors.Config.MissingOption: If the spec does not include a "machine_type"
          or both "cpus" and "memory".
      errors.Config.InvalidValue: If the spec contains both "machine_type" and
          at least one of "cpus" or "memory".
    """
    super().__init__(vm_spec)
    self.vm_spec = typing.cast(GceVmSpec, vm_spec)
    self.create_cmd: util.GcloudCommand = None
    self.boot_metadata = {}
    self.boot_metadata_from_file = {}
    if self.boot_startup_script:
      self.boot_metadata_from_file['startup-script'] = self.boot_startup_script
    self.ssd_interface = vm_spec.ssd_interface
    self.cpus = vm_spec.cpus
    self.image = self.image or self.DEFAULT_IMAGE
    self.memory_mib = vm_spec.memory
    self.preemptible = vm_spec.preemptible
    self.spot_early_termination = False
    self.preemptible_status_code = None
    self.project = vm_spec.GetProject()
    self.image_project = vm_spec.image_project or self.GetDefaultImageProject()
    self.mtu: int | None = vm_spec.mtu
    self.subnet_names = vm_spec.subnet_names
    self.network = self._GetNetwork()
    self.firewall = gce_network.GceFirewall.GetFirewall()
    self.boot_disk = gce_disk.GceBootDisk(self, vm_spec.boot_disk_spec)
    self.disks = [self.boot_disk]
    self.id = None
    self.node_type = vm_spec.node_type
    self.host = None
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.min_cpu_platform = vm_spec.min_cpu_platform
    self.threads_per_core = vm_spec.threads_per_core
    self.visible_core_count = vm_spec.visible_core_count
    self.gce_accelerator_type_override = FLAGS.gce_accelerator_type_override
    self.gce_tags = vm_spec.gce_tags
    self.gce_network_tier = FLAGS.gce_network_tier
    self.gce_nic_types = FLAGS.gce_nic_types
    self.max_local_disks = vm_spec.max_local_disks
    self.create_cmd_info_log = None
    for idx, gce_nic_type in enumerate(self.gce_nic_types):
      if gce_nic_type == 'GVNIC' and not self.SupportGVNIC():
        logging.warning('Changing gce_nic_type to VIRTIO_NET')
        self.gce_nic_types[idx] = 'VIRTIO_NET'
    self.gce_egress_bandwidth_tier = gcp_flags.EGRESS_BANDWIDTH_TIER.value
    self.gce_shielded_secure_boot = FLAGS.gce_shielded_secure_boot
    # Default to GCE default (Live Migration)
    self.on_host_maintenance = None
    # https://cloud.google.com/compute/docs/instances/live-migration-process#limitations
    # https://cloud.google.com/compute/docs/instances/placement-policies-overview#restrictions
    # TODO(pclay): Update if this assertion ever changes
    if (
        FLAGS['gce_migrate_on_maintenance'].present
        and FLAGS.gce_migrate_on_maintenance
        and (self.gpu_count or self.network.placement_group or self.preemptible)
    ):
      raise errors.Config.InvalidValue(
          'Cannot set flag gce_migrate_on_maintenance on instances with GPUs,'
          ' network placement groups, or preemption, as it is not supported by'
          ' GCP.'
      )
    if (
        self.gpu_count
        or self.network.placement_group
        or self.preemptible
        or (
            gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value
            and not IsLiveMigratableConfidentialCompute(self.machine_type)
        )
    ):
      self.on_host_maintenance = 'TERMINATE'
    elif FLAGS['gce_migrate_on_maintenance'].present:
      if FLAGS.gce_migrate_on_maintenance:
        self.on_host_maintenance = 'MIGRATE'
      else:
        self.on_host_maintenance = 'TERMINATE'

    self.automatic_restart = FLAGS.gce_automatic_restart
    self.preempt_marker = None
    if self.preemptible:
      self.preempt_marker = f'gs://{FLAGS.gcp_preemptible_status_bucket}/{FLAGS.run_uri}/{self.name}'
    arm_arch = GetArmArchitecture(self.machine_type)
    self.is_aarch64 = bool(arm_arch)
    if arm_arch:
      # Assign host_arch to avoid running detect_host on ARM
      self.host_arch = arm_arch
    self.image_family = vm_spec.image_family or self.GetDefaultImageFamily(
        self.is_aarch64
    )
    self.create_disk_strategy = gce_disk_strategies.GetCreateDiskStrategy(
        self, None, 0
    )

  def _GetNetwork(self):
    """Returns the GceNetwork to use."""
    return gce_network.GceNetwork.GetNetwork(self.vm_spec)

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
    if gcp_flags.GCE_CREATE_LOG_HTTP.value:
      cmd.flags['log-http'] = True

    if gcp_flags.GCE_NODE_GROUP.value:
      cmd.flags['node-group'] = gcp_flags.GCE_NODE_GROUP.value

    # Compute all flags requiring alpha first. Then if any flags are different
    # between alpha and GA, we can set the appropriate ones.
    if self.gce_egress_bandwidth_tier:
      network_performance_configs = (
          f'total-egress-bandwidth-tier={self.gce_egress_bandwidth_tier}'
      )
      cmd.flags['network-performance-configs'] = network_performance_configs
      self.metadata['gce_egress_bandwidth_tier'] = (
          self.gce_egress_bandwidth_tier
      )

    # TODO(pclay): remove when on-host-maintenance gets promoted to GA
    maintenance_flag = 'maintenance-policy'
    if cmd.use_alpha_gcloud:
      maintenance_flag = 'on-host-maintenance'
    if self.on_host_maintenance:
      cmd.flags[maintenance_flag] = self.on_host_maintenance

    if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
      if gcp_flags.GCE_CONFIDENTIAL_COMPUTE_TYPE.value == 'sev':
        cmd.flags['confidential-compute-type'] = 'SEV'

    if self.network.subnet_resources:
      net_resources = self.network.subnet_resources
      ni_arg_name = 'subnet'
    else:
      net_resources = self.network.network_resources
      ni_arg_name = 'network'

    # Bundle network-related arguments with --network-interface
    # This flag is mutually exclusive with any of these flags:
    # --address, --network, --network-tier, --subnet, --private-network-ip.
    # gcloud compute instances create ... --network-interface=
    for idx, net_resource in enumerate(net_resources):
      gce_nic_type = self.gce_nic_types[idx].upper()
      gce_nic_queue_count_arg = []
      if gcp_flags.GCE_NIC_QUEUE_COUNTS.value[idx] != 'default':
        gce_nic_queue_count_arg = [
            f'queue-count={gcp_flags.GCE_NIC_QUEUE_COUNTS.value[idx]}'
        ]
      no_address_arg = []
      if not self.assign_external_ip_all_nics and (
          not self.assign_external_ip or idx > 0
      ):
        no_address_arg = ['no-address']
      cmd.additional_flags += [
          '--network-interface',
          ','.join(
              [
                  f'{ni_arg_name}={net_resource.name}',
                  f'nic-type={gce_nic_type}',
                  f'network-tier={self.gce_network_tier.upper()}',
              ]
              + gce_nic_queue_count_arg
              + no_address_arg
              + gcp_flags.GCE_EXTRA_NETWORK_INTERFACE_OPTIONS.value
          ),
      ]

    if self.image:
      cmd.flags['image'] = self.image
    elif self.image_family:
      cmd.flags['image-family'] = self.image_family
      self.metadata['image_family'] = self.image_family
    if self.image_project is not None:
      cmd.flags['image-project'] = self.image_project
      self.metadata['image_project'] = self.image_project

    for disk_ in self.disks:
      cmd.flags.update(disk_.GetCreationCommand())
    if self.machine_type is None:
      cmd.flags['custom-cpu'] = self.cpus
      cmd.flags['custom-memory'] = '{}MiB'.format(self.memory_mib)
    else:
      cmd.flags['machine-type'] = self.machine_type

    if self.min_cpu_platform:
      cmd.flags['min-cpu-platform'] = self.min_cpu_platform

    # metal instances do not support disable SMT.
    if self.threads_per_core and 'metal' not in self.machine_type:
      cmd.flags['threads-per-core'] = self.threads_per_core
      self.metadata['threads_per_core'] = self.threads_per_core

    if self.visible_core_count:
      cmd.flags['visible-core-count'] = self.visible_core_count
      self.metadata['visible_core_count'] = self.visible_core_count

    if self.gpu_count and (
        self.cpus
        or (
            self.machine_type
            and self.machine_type not in _FIXED_GPU_MACHINE_TYPES
        )
    ):
      cmd.flags['accelerator'] = GenerateAcceleratorSpecString(
          self.gpu_type, self.gpu_count
      )

    cmd.flags['tags'] = ','.join(['perfkitbenchmarker'] + (self.gce_tags or []))
    if not self.automatic_restart:
      cmd.flags['no-restart-on-failure'] = True
    self.metadata['automatic_restart'] = self.automatic_restart
    if self.host:
      cmd.flags['node-group'] = self.host.name
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
    for key, value in parsed_metadata_from_file.items():
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
        ['%s=%s' % (k, v) for k, v in metadata_from_file.items()]
    )

    # passing sshKeys does not work with OS Login
    metadata = {'enable-oslogin': 'FALSE'}
    metadata.update(self.boot_metadata)
    metadata.update(util.GetDefaultTags())
    # Signal (along with timeout_utc) that VM is short lived.
    metadata['vm_nature'] = 'ephemeral'

    additional_metadata = {}
    additional_metadata.update(self.vm_metadata)
    additional_metadata.update(
        flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata)
    )

    for key, value in additional_metadata.items():
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

    if (
        self.machine_type is None
        or self.machine_type not in gce_disk.FIXED_SSD_MACHINE_TYPES
    ):
      # Append the `--local-ssd` args only when it's a customized or old-gen VM.
      cmd.flags['local-ssd'] = [
          'interface={}'.format(self.ssd_interface)
      ] * self.max_local_disks

    cmd.flags.update(self.create_disk_strategy.GetCreationCommand())

    if gcp_flags.GCE_VM_SERVICE_ACCOUNT.value:
      cmd.flags['service-account'] = gcp_flags.GCE_VM_SERVICE_ACCOUNT.value
    if gcp_flags.GCLOUD_SCOPES.value:
      cmd.flags['scopes'] = ','.join(
          re.split(r'[,; ]', gcp_flags.GCLOUD_SCOPES.value)
      )
    if gcp_flags.GCE_PERFORMANCE_MONITORING_UNIT.value:
      cmd.flags['performance-monitoring-unit'] = (
          gcp_flags.GCE_PERFORMANCE_MONITORING_UNIT.value
      )

    if gcp_flags.GCE_RESERVATION_ID.value:
      cmd.flags['reservation'] = gcp_flags.GCE_RESERVATION_ID.value
      cmd.flags['reservation-affinity'] = 'specific'

    if gcp_flags.GCE_PROVISIONING_MODEL.value:
      cmd.flags['provisioning-model'] = gcp_flags.GCE_PROVISIONING_MODEL.value

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
    stdout, stderr, retcode = self.create_cmd.Issue(
        timeout=_GCE_VM_CREATE_TIMEOUT, raise_on_failure=False
    )
    # Save the create operation name for use in _WaitUntilRunning
    if 'name' in stdout:
      response = json.loads(stdout)
      self.create_operation_name = response[0]['name']

    self._ParseCreateErrors(self.create_cmd.rate_limited, stderr, retcode)
    if not self.create_return_time:
      self.create_return_time = time.time()
    if pkb_flags.GENERATE_VM_CREATE_CMD_INFO_LOG.value:
      self.create_cmd_info_log, _, _ = vm_util.IssueCommand(
          [FLAGS.gcloud_path, 'info', '--show-log'], raise_on_failure=False
      )

  def _ParseCreateErrors(
      self, cmd_rate_limited: bool, stderr: str, retcode: int
  ):
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
          self.host = self.host_list[-1]
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
      if re.search(
          r'HTTPError 400: .* can not be used without accelerator\(s\) in zone',
          stderr,
      ):
        raise errors.Benchmarks.UnsupportedConfigError(stderr)
      if 'The service is currently unavailable' in stderr:
        raise errors.Benchmarks.KnownIntermittentError(stderr)
      # The initial request failed, prompting a retry, but the instance was
      # still successfully created, which results in
      # '409: The resource X already exists'
      if '(gcloud.compute.instances.create) HTTPError 409' in stderr:
        raise errors.Benchmarks.KnownIntermittentError(stderr)
      # Occurs when creating a large number >30 of VMs in parallel.
      if 'gcloud crashed (SSLError)' in stderr:
        raise errors.Resource.RetryableCreationError(stderr)
      raise errors.Resource.CreationError(
          f'Failed to create VM {self.name}:\n{stderr}\nreturn code: {retcode}'
      )

  def _CreateDependencies(self):
    super()._CreateDependencies()
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
          self.host = GceSoleTenantNodeGroup(
              self.node_type, self.zone, self.project
          )
          self.host_list.append(self.host)
          if gcp_flags.GCE_NODE_GROUP.value is None:
            # GCE_NODE_GROUP is used to identify an existing node group.
            self.host.Create()
        else:
          self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

    # Capture the public key, write it to a temp file, and save the filename.
    with open(self.ssh_public_key) as f:
      ssh_public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(
        mode='w', dir=vm_util.GetTempDir(), prefix='key-metadata', delete=False
    ) as tf:
      tf.write('%s:%s\n' % (self.user_name, ssh_public_key))
      tf.close()
      self.create_cmd = self._GenerateCreateCommand(tf.name)

  def _DeleteDependencies(self):
    if self.host:
      with self._host_lock:
        if self.host in self.host_list:
          self.host_list.remove(self.host)
        if self.host not in self.deleted_hosts:
          self.host.Delete()
          self.deleted_hosts.add(self.host)

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
      if 'accessConfigs' in network_interface:
        self.ip_addresses.append(network_interface['accessConfigs'][0]['natIP'])

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

    for disk_ in self.disks:
      disk_.PostCreate()

    self.image = self.boot_disk.image

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
      retryable_exceptions=(GceServiceUnavailableError,),
  )
  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'describe', self.name
    )
    stdout, stderr, retcode = getinstance_cmd.Issue(raise_on_failure=False)
    transient_error_msgs = (
        'The service is currently unavailable',
        'Max retries exceeded with url',
    )
    if any(msg in stderr for msg in transient_error_msgs):
      logging.info('instances describe command failed, retrying.')
      raise GceServiceUnavailableError(stderr)
    elif retcode and re.search(r"The resource \'.*'\ was not found", stderr):
      return False
    elif retcode:
      raise errors.VmUtil.IssueCommandError(
          f'Failed to check existence of VM {self.name}:\n{stderr}\n'
          f'return code: {retcode}'
      )
    response = json.loads(stdout)
    status = response['status']
    return status in INSTANCE_EXISTS_STATUSES

  @vm_util.Retry(
      poll_interval=1,
      log_errors=False,
      retryable_exceptions=(GceRetryDescribeOperationsError,),
  )
  def _WaitUntilRunning(self):
    """Waits until the VM instances create command completes."""
    getoperation_cmd = util.GcloudCommand(
        self, 'compute', 'operations', 'describe', self.create_operation_name
    )
    if gcp_flags.GCE_CREATE_LOG_HTTP.value:
      getoperation_cmd.flags['log-http'] = True

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
      self._ParseCreateErrors(
          getoperation_cmd.rate_limited, create_stderr, create_retcode
      )
    # Retry if the operation is not yet DONE.
    elif status != OPERATION_DONE:
      logging.info(
          'VM create operation has status %s; retrying operations '
          'describe command.',
          status,
      )
      raise GceRetryDescribeOperationsError()
    # Collect the time-to-running timestamp once the operation completes.
    elif not self.is_running_time:
      self.is_running_time = time.time()

  def SetDiskSpec(self, disk_spec, disk_count):
    """Sets Disk Specs of the current VM. Calls before the VM is created."""
    self.create_disk_strategy = gce_disk_strategies.GetCreateDiskStrategy(
        self, disk_spec, disk_count
    )

  def SetupAllScratchDisks(self):
    """Set up all scratch disks of the current VM."""
    # Prepare vm scratch disks:
    self.create_disk_strategy.GetSetupDiskStrategy().SetUpDisk()

  def ReleaseIpReservation(self, ip_address_name: str) -> None:
    """Releases existing IP reservation.

    Args:
      ip_address_name: name of the IP reservation to be released.
    """
    reserv_ip_address = gce_network.GceIPAddress(
        self.project,
        util.GetRegionFromZone(self.zone),
        ip_address_name,
        self.network.primary_subnet_name,
    )
    reserv_ip_address.Delete()

  def AddMetadata(self, **kwargs):
    """Adds metadata to disk."""
    # vm metadata added to vm on creation.
    # Add metadata to boot disk
    gce_disk.AddLabels(self, self.name)
    self.create_disk_strategy.AddMetadataToDiskResource()

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping string property key to value.
    """
    result = super().GetResourceMetadata()
    for attr_name in 'cpus', 'memory_mib', 'preemptible', 'project':
      attr_value = getattr(self, attr_name)
      if attr_value:
        result[attr_name] = attr_value
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
    result['gce_nic_type'] = self.gce_nic_types
    result['gce_shielded_secure_boot'] = self.gce_shielded_secure_boot
    if self.visible_core_count:
      result['visible_core_count'] = self.visible_core_count
    if self.network.mtu:
      result['mtu'] = self.network.mtu
    if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
      result['confidential_compute'] = True
      result['confidential_compute_type'] = (
          gcp_flags.GCE_CONFIDENTIAL_COMPUTE_TYPE.value
      )

    for disk_ in self.disks:
      result.update(disk_.GetResourceMetadata())

    if gcp_flags.GCE_RESERVATION_ID.value:
      result['reservation_id'] = gcp_flags.GCE_RESERVATION_ID.value

    if gcp_flags.GCE_PROVISIONING_MODEL.value:
      result['provisioning_model'] = gcp_flags.GCE_PROVISIONING_MODEL.value

    return result

  def DownloadPreprovisionedData(
      self,
      install_path,
      module_name,
      filename,
      timeout=virtual_machine.PREPROVISIONED_DATA_TIMEOUT,
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
    logging.info('Testing if preprovisioned data is available %s', filename)
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

  def GetDefaultImageFamily(self, is_arm: bool) -> str | None:
    return None

  def GetDefaultImageProject(self) -> str | None:
    return None

  def GetNumTeardownSkippedVms(self) -> int:
    """Returns the number of lingering VMs in this VM's project and zone."""
    # compute instances list doesn't accept a --zone flag, so we need to drop
    # the zone from the VM object and pass in --zones instead.
    vm_without_zone = copy.copy(self)
    vm_without_zone.zone = None
    args = ['compute', 'instances', 'list']
    cmd = util.GcloudCommand(vm_without_zone, *args)
    cmd.flags['format'] = 'json'
    cmd.flags['zones'] = self.zone
    stdout, _, _ = cmd.Issue()
    all_vms = json.loads(stdout)
    num_teardown_skipped_vms = 0
    for vm_json in all_vms:
      for item in vm_json['metadata']['items']:
        if (
            item['key'] == PKB_SKIPPED_TEARDOWN_METADATA_KEY
            and item['value'] == 'true'
        ):
          num_teardown_skipped_vms += 1
          continue
    return num_teardown_skipped_vms

  def UpdateTimeoutMetadata(self):
    """Updates the timeout metadata for the VM."""
    new_timeout = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=pkb_flags.SKIP_TEARDOWN_KEEP_UP_MINUTES.value
    )
    new_timeout = new_timeout.strftime(resource.METADATA_TIME_FORMAT)
    args = ['compute', 'instances', 'add-metadata', self.name]
    cmd = util.GcloudCommand(self, *args)
    cmd.flags['metadata'] = (
        f'{resource.TIMEOUT_METADATA_KEY}={new_timeout},'
        f'{PKB_SKIPPED_TEARDOWN_METADATA_KEY}=true'
    )
    cmd.Issue()


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
  DEFAULT_X86_IMAGE_FAMILY = None
  DEFAULT_ARM_IMAGE_FAMILY = None
  DEFAULT_IMAGE_PROJECT = None
  SUPPORTS_GVNIC = True

  def __init__(self, vm_spec):
    super().__init__(vm_spec)
    self._gvnic_version = None

  def GetResourceMetadata(self):
    """See base class."""
    metadata = super().GetResourceMetadata().copy()
    if self._gvnic_version:
      metadata['gvnic_version'] = self._gvnic_version

    return metadata

  def OnStartup(self):
    """See base class.  Sets the _gvnic_version."""
    super().OnStartup()
    self._gvnic_version = self.GetGvnicVersion()

  def GetGvnicVersion(self) -> str | None:
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
    if is_arm:
      if self.DEFAULT_ARM_IMAGE_FAMILY:
        return self.DEFAULT_ARM_IMAGE_FAMILY

      assert 'arm64' not in self.DEFAULT_X86_IMAGE_FAMILY
      if 'amd64' in self.DEFAULT_X86_IMAGE_FAMILY:
        # New convention as of Ubuntu 23
        arm_image_family = self.DEFAULT_X86_IMAGE_FAMILY.replace(
            'amd64', 'arm64'
        )
      else:
        # Older convention
        arm_image_family = self.DEFAULT_X86_IMAGE_FAMILY + '-arm64'
      logging.info(
          'ARM image must be used; changing image to %s',
          arm_image_family,
      )
      return arm_image_family
    if not self.DEFAULT_X86_IMAGE_FAMILY:
      raise ValueError(
          'DEFAULT_X86_IMAGE_FAMILY can not be None for non-ARM vms.'
      )
    return self.DEFAULT_X86_IMAGE_FAMILY

  def GetDefaultImageProject(self) -> str:
    if not self.DEFAULT_IMAGE_PROJECT:
      raise ValueError('DEFAULT_IMAGE_PROJECT can not be None')
    return self.DEFAULT_IMAGE_PROJECT

  def GenerateAndCaptureSerialPortOutput(self, local_path: str) -> bool:
    """Generates and captures the serial port output for the remote VM.

    Args:
      local_path: The path to store the serial port output on the caller's
        machine.

    Returns:
      True if the serial port output was successfully generated and captured;
      False otherwise.
    """
    cmd = util.GcloudCommand(
        self,
        'compute',
        'instances',
        'get-serial-port-output',
        self.name,
    )
    cmd.flags['zone'] = self.zone
    cmd.flags['port'] = 1
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Failed to get serial port 1 output')
      return False
    with open(local_path, 'w') as f:
      f.write(stdout)
    return True

  def GetCreateCmdInfoLogPath(self) -> str:
    """Writes the create cmd info log to a temp file and returns the path."""
    if not self.create_cmd_info_log:
      return ''
    path = vm_util.PrependTempDir('create_cmd_info_log')
    with open(path, 'w') as f:
      f.write(self.create_cmd_info_log)
    return path


class Debian11BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine,
    linux_vm.Debian11Mixin,
):
  DEFAULT_X86_IMAGE_FAMILY = 'debian-11'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'

  @property
  def DEFAULT_ARM_IMAGE_FAMILY(self):
    raise errors.Config.InvalidValue(
        'GCE does not support Debian 11 on ARM during LTS.'
    )


class Debian12BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian12Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'debian-12'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class Debian13BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian13Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'debian-13'
  DEFAULT_IMAGE_PROJECT = 'debian-cloud'


class Rhel8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel8Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rhel-8'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Rhel9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel9Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rhel-9'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Rhel10BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Rhel10Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rhel-10'
  DEFAULT_IMAGE_PROJECT = 'rhel-cloud'


class Ol8BasedGceVirtualMachine(BaseLinuxGceVirtualMachine, linux_vm.Ol8Mixin):
  DEFAULT_X86_IMAGE_FAMILY = 'oracle-linux-8'
  DEFAULT_IMAGE_PROJECT = 'oracle-linux-cloud'

  def PrepareVMEnvironment(self):
    super().PrepareVMEnvironment()
    # https://cloud.google.com/compute/docs/images/os-details#oracle_linux
    self.RemoteCommand(
        'sudo dnf install -y google-cloud-cli --enablerepo google-cloud-sdk'
    )


class Ol9BasedGceVirtualMachine(BaseLinuxGceVirtualMachine, linux_vm.Ol9Mixin):
  DEFAULT_X86_IMAGE_FAMILY = 'oracle-linux-9'
  DEFAULT_IMAGE_PROJECT = 'oracle-linux-cloud'

  def PrepareVMEnvironment(self):
    super().PrepareVMEnvironment()
    # https://cloud.google.com/compute/docs/images/os-details#oracle_linux
    self.RemoteCommand(
        'sudo dnf install -y google-cloud-cli --enablerepo google-cloud-sdk'
    )


class RockyLinux8BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.RockyLinux8Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-8'
  DEFAULT_IMAGE_PROJECT = 'rocky-linux-cloud'


# https://cloud.google.com/blog/products/application-modernization/introducing-rocky-linux-optimized-for-google-cloud
class RockyLinux8OptimizedBasedGceVirtualMachine(
    RockyLinux8BasedGceVirtualMachine
):
  OS_TYPE = os_types.ROCKY_LINUX8_OPTIMIZED
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-8-optimized-gcp'


class RockyLinux9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.RockyLinux9Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-9'
  DEFAULT_IMAGE_PROJECT = 'rocky-linux-cloud'


class RockyLinux9OptimizedBasedGceVirtualMachine(
    RockyLinux9BasedGceVirtualMachine
):
  OS_TYPE = os_types.ROCKY_LINUX9_OPTIMIZED
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-9-optimized-gcp'


class RockyLinux10BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.RockyLinux10Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-10'
  DEFAULT_IMAGE_PROJECT = 'rocky-linux-cloud'


class RockyLinux10OptimizedBasedGceVirtualMachine(
    RockyLinux10BasedGceVirtualMachine
):
  OS_TYPE = os_types.ROCKY_LINUX10_OPTIMIZED
  DEFAULT_X86_IMAGE_FAMILY = 'rocky-linux-10-optimized-gcp'


class CentOsStream9BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CentOsStream9Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'centos-stream-9'
  DEFAULT_IMAGE_PROJECT = 'centos-cloud'


class BaseCosBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.BaseContainerLinuxMixin
):
  """Base class for COS-based GCE virtual machines."""

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
  DEFAULT_X86_IMAGE_FAMILY = 'cos-stable'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-stable'


class CosDevBasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS_DEV
  DEFAULT_X86_IMAGE_FAMILY = 'cos-dev'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-dev'


class Cos125BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS125
  DEFAULT_X86_IMAGE_FAMILY = 'cos-125-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-125-lts'


class Cos121BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS121
  DEFAULT_X86_IMAGE_FAMILY = 'cos-121-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-121-lts'


class Cos117BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS117
  DEFAULT_X86_IMAGE_FAMILY = 'cos-117-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-117-lts'


class Cos113BasedGceVirtualMachine(BaseCosBasedGceVirtualMachine):
  OS_TYPE = os_types.COS113
  DEFAULT_X86_IMAGE_FAMILY = 'cos-113-lts'
  DEFAULT_ARM_IMAGE_FAMILY = 'cos-arm64-113-lts'


class CoreOsBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.CoreOsMixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'fedora-coreos-stable'
  DEFAULT_IMAGE_PROJECT = 'fedora-coreos-cloud'
  SUPPORTS_GVNIC = False

  def __init__(self, vm_spec):
    super().__init__(vm_spec)
    # Fedora CoreOS only creates the core user
    self.user_name = 'core'


class Ubuntu2004BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu2004Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'ubuntu-2004-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu2204BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu2204Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'ubuntu-2204-lts'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Ubuntu2404BasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Ubuntu2404Mixin
):
  DEFAULT_X86_IMAGE_FAMILY = 'ubuntu-2404-lts-amd64'
  DEFAULT_IMAGE_PROJECT = 'ubuntu-os-cloud'


class Debian12DeepLearningBasedGceVirtualMachine(
    BaseLinuxGceVirtualMachine, linux_vm.Debian12DLMixin
):
  """Debian12 based Deeplearning image."""

  # This is an self-made image that follow instructions from
  # https://github.com/GoogleCloudPlatform/cluster-toolkit/blob/main/examples/machine-learning/a3-megagpu-8g/slurm-a3mega-image.yaml
  DEFAULT_IMAGE_PROJECT = ''
  DEFAULT_X86_IMAGE_FAMILY = 'slurm-a3mega'

  def __init__(self, vm_spec):
    """Initialize a Debian12 Base DLVM virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.

    Raises:
      ValueError: If an incompatible vm_spec is passed.
    """
    super().__init__(vm_spec)
    self._installed_packages.add('slurm')
    self._installed_packages.add('cuda_toolkit')

  def PrepareVMEnvironment(self):
    super().PrepareVMEnvironment()
    self.Install('tcpxo')
    self.RemoteCommand('sudo mkdir -p /etc/slurm/', ignore_failure=True)
    self.RemoteCommand(
        'echo "cgroupPlugin=cgroup/v2" >> cgroup.conf; '
        'echo "ConstrainCores=no" >> cgroup.conf; '
        'echo "ConstrainRAMSpace=no" >> cgroup.conf; '
    )
    self.RemoteCommand('sudo mv cgroup.conf /etc/slurm/')


def GenerateDownloadPreprovisionedDataCommand(
    install_path: str, module_name: str, filename: str
) -> str:
  """Returns a string used to download preprovisioned data.

  Args:
    install_path: Path to install the module.
    module_name: Name of the module to download.
    filename: Filename of the module. Usually a zip file.

  Returns:
    The gcloud command to run.
  """
  return 'gcloud storage -q cp gs://%s/%s/%s %s' % (
      FLAGS.gcp_preprovisioned_data_bucket,
      module_name,
      filename,
      posixpath.join(install_path, filename),
  )


def GenerateStatPreprovisionedDataCommand(
    module_name: str, filename: str
) -> str:
  """Returns a string used to download preprovisioned data.

  Args:
    module_name: Name of the module to download.
    filename: Filename of the module. Usually a zip file.

  Returns:
    The gcloud command to run.
  """
  return 'gcloud storage ls gs://%s/%s/%s' % (
      FLAGS.gcp_preprovisioned_data_bucket,
      module_name,
      filename,
  )
