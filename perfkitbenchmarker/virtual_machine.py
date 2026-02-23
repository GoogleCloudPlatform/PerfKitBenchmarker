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

"""Class to represent a Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import abc
import copy
import enum
import logging
import os.path
import threading
import typing
from typing import Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_lookup
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags as pkb_flags
from perfkitbenchmarker import os_mixin
from perfkitbenchmarker import os_types
from perfkitbenchmarker import package_lookup
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
DEFAULT_USERNAME = 'perfkit'
QUOTA_EXCEEDED_MESSAGE = 'Creation failed due to quota exceeded: '
PREPROVISIONED_DATA_TIMEOUT = 600


class CpuVersionMismatch(errors.Resource.RetryableCreationError):
  """When a VM does not match an expected CPU version."""

  pass


def ValidateVmMetadataFlag(options_list):
  """Verifies correct usage of the vm metadata flag.

  All provided options must be in the form of key:value.

  Args:
    options_list: A list of strings parsed from the provided value for the flag.

  Returns:
    True if the list of options provided as the value for the flag meets
    requirements.

  Raises:
    flags.ValidationError: If the list of options provided as the value for
      the flag does not meet requirements.
  """
  for option in options_list:
    if ':' not in option[1:-1]:
      raise flags.ValidationError(
          '"%s" not in expected key:value format' % option
      )
  return True


# vm_metadata flag name
VM_METADATA = 'vm_metadata'

BOOT_DISK_SIZE = flags.DEFINE_integer(
    'boot_disk_size', None, 'The boot disk size in GiB for VMs.'
)
flags.DEFINE_boolean(
    'dedicated_hosts',
    False,
    'If True, use hosts that only have VMs from the same '
    'benchmark running on them.',
)
flags.DEFINE_integer(
    'num_vms_per_host',
    None,
    'The number of VMs per dedicated host. If None, VMs will be packed on a '
    'single host until no more can be packed at which point a new host will '
    'be created.',
)
flags.DEFINE_integer(
    'num_cpus_override',
    None,
    'Rather than detecting the number of CPUs present on the machine, use this '
    'value if set. Some benchmarks will use this number to automatically '
    'scale their configurations; this can be used as a method to control '
    'benchmark scaling. It will also change the num_cpus metadata '
    'published along with the benchmark data.',
)
flags.DEFINE_list(
    VM_METADATA, [], 'Metadata to add to the vm. It expectskey:value pairs.'
)
flags.register_validator(VM_METADATA, ValidateVmMetadataFlag)
flags.DEFINE_bool(
    'skip_firewall_rules',
    False,
    'If set, this run will not create firewall rules. This is useful if the '
    'user project already has all of the firewall rules in place and/or '
    'creating new ones is expensive',
)
flags.DEFINE_bool(
    'preprovision_ignore_checksum',
    False,
    'Ignore checksum verification for preprovisioned data. '
    'Not recommended, please use with caution',
)
flags.DEFINE_boolean(
    'connect_via_internal_ip',
    False,
    'Whether to use internal IP addresses for running commands on and pushing '
    'data to VMs. By default, PKB interacts with VMs using external IP '
    'addresses.',
)
_ASSIGN_EXTERNAL_IP = flags.DEFINE_boolean(
    'assign_external_ip',
    True,
    'If True, an external (public) IP will be created for primary NIC of VMs. '
    'If False, --connect_via_internal_ip may also be needed.',
)
flags.DEFINE_boolean(
    'assign_external_ip_all_nics',
    False,
    'If True, an external (public) IP will be created for each network '
    'interface. If False, an external IP is created for the first '
    'interface only if --assign_external_ip is True.',
)
flags.DEFINE_string(
    'boot_startup_script',
    None,
    (
        'Startup script to run during boot. '
        'Requires provider support, only implemented for Linux VMs '
        'on GCP, AWS, Azure for now.'
    ),
)
_VM_INSTANCE_NAME_SUFFIX = flags.DEFINE_string(
    'vm_instance_name_suffix',
    None,
    (
        'Optional, a suffix to add after the VM instance name. Without this,'
        ' instance is named as pkb-{run_uri}-{instance_number}. With this'
        ' option, the instance name will be'
        ' pkb-{run_uri}-{instance_number}-{vm_instance_name_suffix}'
    ),
)
_VM_INSTANCE_NAME_SUFFIXES = flags.DEFINE_list(
    'vm_instance_name_suffixes',
    None,
    (
        'Optional, the list of suffixes to add after the VM instance name.'
        ' Without this, instance is named as pkb-{run_uri}-{instance_number}.'
        ' With this option, the instance name will be'
        ' pkb-{run_uri}-{vm_instance_name_suffixes[instance_number]}.'
        ' The number of suffixes must be equal to the total number of VMs'
        ' (servers and clients).'
    ),
)
_VM_NAME_PREFIX = flags.DEFINE_string(
    'vm_name_prefix',
    None,
    (
        'Optional, a prefix to add after the VM instance name. Without this,'
        ' instance is named as pkb-{run_uri}-{instance_number}. With this'
        ' option, the instance name will be'
        ' {prefix}-{instance_number}-{vm_instance_name_suffix}'
    ),
)
_REQUIRED_CPU_VERSION = flags.DEFINE_string(
    'required_cpu_version',
    None,
    'The required CPU version. Benchmark will fail if CPU does not match.'
    ' The version is defined as the fields in /proc/cpuinfo that define the cpu'
    ' version joined by underscores. On x86, the fields are (Vendor ID,'
    ' cpu family, Model, Stepping) (e.g. GenuineIntel_6_143_8). On ARM, the'
    ' fields are (CPU implementer, CPU architecture, CPU variant, CPU part).'
    ' This is only useful when a machine type comprises of multiple CPU'
    ' versions.',
)
_REQUIRED_CPU_VERSION_RETRIES = flags.DEFINE_integer(
    'required_cpu_version_retries',
    5,
    'The number of times to retry if the required CPU version does not match.',
)
_VM_USER_NAME = flags.DEFINE_string(
    'vm_user_name',
    DEFAULT_USERNAME,
    'The user name to use for the VM. If not specified, the default user name'
    ' "perfkit" is used.',
)
SKIP_VM_PREPARATION = flags.DEFINE_bool(
    'skip_vm_preparation',
    False,
    'Skips the PrepareAfterBoot step for VMs.',
)


@enum.unique
class BootCompletionIpSubset(enum.Enum):
  DEFAULT = enum.auto()
  EXTERNAL = enum.auto()
  INTERNAL = enum.auto()
  BOTH = enum.auto()


_BOOT_COMPLETION_IP_SUBSET = flags.DEFINE_enum_class(
    'boot_completion_ip_subset',
    BootCompletionIpSubset.DEFAULT,
    BootCompletionIpSubset,
    (
        'The ip(s) to use to measure BootCompletion.  If DEFAULT, determined'
        ' based on --connect_via_internal_ip.'
    ),
)
# Deprecated. Use connect_via_internal_ip.
flags.DEFINE_boolean(
    'ssh_via_internal_ip',
    False,
    'Whether to use internal IP addresses for running commands on and pushing '
    'data to VMs. By default, PKB interacts with VMs using external IP '
    'addresses.',
)
flags.DEFINE_boolean(
    'retry_on_rate_limited',
    True,
    'Whether to retry commands when rate limited.',
)

CPUARCH_X86_64 = 'x86_64'
CPUARCH_AARCH64 = 'aarch64'

GPU_COUNT = flags.DEFINE_integer(
    'gpu_count',
    None,
    'Number of gpus to attach to the VM. Requires gpu_type to be specified.',
)
GPU_TYPE = flags.DEFINE_enum(
    'gpu_type',
    None,
    virtual_machine_spec.VALID_GPU_TYPES,
    'Type of gpus to attach to the VM. Requires gpu_count to be specified.',
)


def GetVmClass(
    cloud: str, os_type: str, platform: str | None = None
) -> type['BaseVirtualMachine']:
  """Returns the VM class with corresponding attributes.

  Args:
    cloud: The cloud being used.
    os_type: The os type of the VM.
    platform: The vm platform being used (see enum above). If not provided,
      defaults to flag value.

  Returns:
    A child of BaseVirtualMachine with the corresponding attributes.
  """
  if platform is None:
    platform = provider_info.VM_PLATFORM.value
  return resource.GetResourceClass(
      BaseVirtualMachine,
      CLOUD=cloud,
      OS_TYPE=os_type,
      PLATFORM=platform,
  )


class BaseVirtualMachine(os_mixin.BaseOsMixin, resource.BaseResource):
  """Base class for Virtual Machines.

  This class holds VM methods and attributes relating to the VM as a cloud
  resource. For methods and attributes that interact with the VM's guest
  OS, see BaseOsMixin and its subclasses.

  Attributes:
    image: The disk image used to boot.
    assign_external_ip: If True, create an external (public) IP for primary NIC.
    assign_external_ip_all_nics: If True, create an external (public) IP for
      each network interface.
    ip_address: Public (external) IP address.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    project: The provider-specific project associated with the VM (e.g.
      artisanal-lightbulb-883).
    ssh_public_key: Path to SSH public key file.
    ssh_private_key: Path to SSH private key file.
    user_name: Account name for login. the contents of 'ssh_public_key' should
      be in .ssh/authorized_keys for this user.
    zone: The region / zone the VM was launched in.
    cidr: The CIDR range the VM was launched in.
    disk_specs: list of BaseDiskSpec objects. Specifications for disks attached
      to the VM.
    scratch_disks: list of BaseDisk objects. Scratch disks attached to the VM.
    max_local_disks: The number of local disks on the VM that can be used as
      scratch disks or that can be striped together.
    background_cpu_threads: The number of threads of background CPU usage while
      running the benchmark.
    background_network_mbits_per_sec: Number of mbits/sec of background network
      usage while running the benchmark.
    background_network_ip_type: Type of IP address to use for generating
      background network workload
    vm_group: The VM group this VM is associated with, if applicable.
    create_operation_name: The name of a VM's create command operation, used to
      poll the operation in WaitUntilRunning.
    create_return_time: The time at which a VM's create command returns.
    is_running_time: The time at which the VM entered the running state.
  """

  is_static = False

  RESOURCE_TYPE = 'BaseVirtualMachine'
  REQUIRED_ATTRS = ['CLOUD', 'OS_TYPE', 'PLATFORM']
  # TODO(user): Define this value in all individual children.
  PLATFORM = provider_info.DEFAULT_VM_PLATFORM

  _instance_counter_lock = threading.Lock()
  _instance_counter = 0

  # This is implemented in os_mixin.BaseOsMixin, but due to insane diamond
  # inheritence, we need it here.
  cpu_arch: str

  def __init__(self, vm_spec: virtual_machine_spec.BaseVmSpec):
    """Initialize BaseVirtualMachine class.

    Args:
      vm_spec: virtual_machine_spec.BaseVmSpec object of the vm.
    """
    super().__init__()
    with self._instance_counter_lock:
      self.instance_number = self._instance_counter
      name_prefix = (
          _VM_NAME_PREFIX.value
          if _VM_NAME_PREFIX.value
          else f'pkb-{FLAGS.run_uri}'
      )
      if _VM_INSTANCE_NAME_SUFFIX.value:
        self.name = '%s-%d-%s' % (
            name_prefix,
            self.instance_number,
            _VM_INSTANCE_NAME_SUFFIX.value,
        )
      elif _VM_INSTANCE_NAME_SUFFIXES.value:
        self.name = '%s-%s' % (
            name_prefix,
            _VM_INSTANCE_NAME_SUFFIXES.value[self.instance_number],
        )
      else:
        self.name = '%s-%d' % (name_prefix, self.instance_number)
      BaseVirtualMachine._instance_counter += 1
    self.vm_spec = vm_spec
    self.machine_type = vm_spec.machine_type
    self.zone = vm_spec.zone
    self.disable_interrupt_moderation = vm_spec.disable_interrupt_moderation
    self.disable_rss = vm_spec.disable_rss
    self.boot_disk_size = vm_spec.boot_disk_size
    self.cidr = vm_spec.cidr
    self.gpu_count = vm_spec.gpu_count
    self.gpu_type = vm_spec.gpu_type
    self.image = vm_spec.image
    self.install_packages = vm_spec.install_packages
    self.can_connect_via_internal_ip = (
        FLAGS.ssh_via_internal_ip or FLAGS.connect_via_internal_ip
    )
    self.boot_completion_ip_subset = _BOOT_COMPLETION_IP_SUBSET.value
    self.assign_external_ip = vm_spec.assign_external_ip
    self.assign_external_ip_all_nics = vm_spec.assign_external_ip_all_nics
    self.ip_address = None
    self.ip_addresses = []
    self.internal_ip = None
    self.internal_ips = []
    self.user_name = _VM_USER_NAME.value
    self.ssh_public_key = vm_util.GetPublicKeyPath()
    self.ssh_private_key = vm_util.GetPrivateKeyPath()
    self.disk_specs = []
    self.scratch_disks = []
    self.max_local_disks = 0
    self.local_disk_counter = 0
    self.remote_disk_counter = 0
    self.host = None
    self.background_cpu_threads = vm_spec.background_cpu_threads
    self.background_network_mbits_per_sec = (
        vm_spec.background_network_mbits_per_sec
    )
    self.background_network_ip_type = vm_spec.background_network_ip_type
    self.use_dedicated_host = None
    self.num_vms_per_host = None

    self.network = None
    self.firewall = None
    self.tcp_congestion_control = None
    self.numa_node_count = None
    self.num_disable_cpus = None
    self.capacity_reservation_id = None
    self.vm_metadata = dict(item.split(':', 1) for item in vm_spec.vm_metadata)
    self.vm_group = None
    self.id = None
    self._is_aarch64 = None
    self.cpu_version = None
    self.create_operation_name = None
    self.create_return_time = None
    self.is_running_time = None
    self.boot_startup_script = vm_spec.boot_startup_script
    self.create_cmd_info_log = None
    if self.OS_TYPE == os_types.CORE_OS and self.boot_startup_script:
      raise errors.Setup.InvalidConfigurationError(
          'Startup script are not supported on CoreOS.'
      )

  @property
  def is_aarch64(self) -> bool:
    """Whether the VM is known to be an ARM VM.

    Historically each provider sets this explicitly, but for some providers like
    KuberentesVirtualMachine, it's conveniet to fall back to what is reported by
    Linux.
    """
    if self._is_aarch64 is not None:
      return self._is_aarch64
    # Only detect arch if it's not explicitly set.
    return self.cpu_arch == CPUARCH_AARCH64

  @is_aarch64.setter
  def is_aarch64(self, value: bool):
    self._is_aarch64 = value

  @property
  @classmethod
  @abc.abstractmethod
  def CLOUD(cls):
    raise NotImplementedError()

  def __repr__(self):
    return '<BaseVirtualMachine [ip={}, internal_ip={}]>'.format(
        self.ip_address, self.internal_ip
    )

  def __str__(self):
    if self.ip_address:
      return self.ip_address
    return super().__str__()

  def GetConnectionIp(self):
    """Gets the IP to use for connecting to the VM."""
    if not self.created:
      raise errors.VirtualMachine.VirtualMachineError(
          'VM was not properly created, but PKB is attempting to connect to '
          'it anyways. Caller should guard against VM not being created.'
      )
    if self.can_connect_via_internal_ip:
      return self.internal_ip
    if not self.ip_address:
      raise errors.VirtualMachine.VirtualMachineError(
          f'{self.name} does not have a (public) ip_address.  Do you need to'
          ' specify --connect_via_internal_ip?'
      )
    return self.ip_address

  def GetInternalIPs(self):
    """Gets the Internal IP's for the VM."""
    if self.internal_ips:
      return self.internal_ips
    elif self.internal_ip:
      return [self.internal_ip]
    return []

  def GetExternalIPs(self):
    """Gets the External IP's for the VM."""
    if self.ip_addresses:
      return self.ip_addresses
    elif self.ip_address:
      return [self.ip_address]
    return []

  def GetLocalhostAddr(self):
    """Returns ::1 if use_ipv6 is set, and localhost otherwise."""
    return '::1' if pkb_flags.FLAGS.use_ipv6 else 'localhost'

  def SetDiskSpec(self, disk_spec, disk_count):
    """Set Disk Specs of the current VM."""
    # This method will be depreciate soon.
    if disk_spec.disk_type == disk.LOCAL and disk_count is None:
      disk_count = self.max_local_disks
    self.disk_specs = [copy.copy(disk_spec) for _ in range(disk_count)]
    # In the event that we need to create multiple disks from the same
    # DiskSpec, we need to ensure that they have different mount points.
    if disk_count > 1 and disk_spec.mount_point:
      for i, vm_disk_spec in enumerate(self.disk_specs):
        vm_disk_spec.mount_point += str(i)

  def SetupAllScratchDisks(self):
    """Set up all scratch disks of the current VM."""
    # This method will be depreciate soon.
    # Prepare vm scratch disks:
    if any(spec.disk_type == disk.RAM for spec in self.disk_specs):
      disk_strategies.SetUpRamDiskStrategy(self, self.disk_specs[0]).SetUpDisk()
      return
    if any(spec.disk_type == disk.NFS for spec in self.disk_specs):
      disk_strategies.SetUpNFSDiskStrategy(self, self.disk_specs[0]).SetUpDisk()
      return

    if any(spec.disk_type == disk.SMB for spec in self.disk_specs):
      disk_strategies.SetUpSMBDiskStrategy(self, self.disk_specs[0]).SetUpDisk()
      return

    if any(spec.disk_type == disk.LOCAL for spec in self.disk_specs):
      self.SetupLocalDisks()

    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      self.CreateScratchDisk(disk_spec_id, disk_spec)
      # TODO(user): Simplify disk logic.
      if disk_spec.num_striped_disks > 1:
        # scratch disks has already been created and striped together.
        break
    # This must come after Scratch Disk creation to support the
    # Containerized VM case

  def CreateScratchDisk(self, disk_spec_id, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec_id: Deterministic order of this disk_spec in the VM's list of
        disk_specs.
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    pass

  def DeleteScratchDisks(self):
    """Delete a VM's scratch disks."""
    for scratch_disk in self.scratch_disks:
      if scratch_disk.disk_type != disk.LOCAL:
        scratch_disk.Delete()

  def GetScratchDir(self, disk_num=0):
    """Gets the path to the scratch directory.

    Args:
      disk_num: The number of the disk to mount.

    Returns:
      The mounted disk directory.
    """
    if disk_num >= len(self.scratch_disks):
      raise errors.Error(
          'GetScratchDir(disk_num=%s) is invalid, max disk_num is %s'
          % (disk_num, len(self.scratch_disks))
      )
    return self.scratch_disks[disk_num].mount_point

  def CreateAndBoot(self):
    """Creates a single VM and waits for boot to complete.

    This is done repeatedly to get --required_cpu_version if it is set.
    """

    def CreateAndBootOnce():
      self.Create()
      logging.info('VM: %s (%s)', self.ip_address, self.internal_ip)
      logging.info('Waiting for boot completion.')
      self.AllowRemoteAccessPorts()
      self.WaitForBootCompletion()
      self.cpu_version = self.GetCPUVersion()
      if (
          _REQUIRED_CPU_VERSION.value
          and _REQUIRED_CPU_VERSION.value != self.cpu_version
      ):
        error_msg = (
            f'Guest arch {self.cpu_version} is not enforced guest arch'
            f' {_REQUIRED_CPU_VERSION.value}. Deleting VM and scratch disk and'
            ' recreating.'
        )
        logging.error(error_msg)
        self.Delete()
        raise CpuVersionMismatch(error_msg)

    try:
      vm_util.Retry(
          max_retries=_REQUIRED_CPU_VERSION_RETRIES.value,
          retryable_exceptions=CpuVersionMismatch,
      )(CreateAndBootOnce)()
    except vm_util.RetriesExceededRetryError as exc:
      if isinstance(exc.__cause__, CpuVersionMismatch):
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            f'{_REQUIRED_CPU_VERSION.value} was not obtained after'
            f' {_REQUIRED_CPU_VERSION_RETRIES.value} retries.'
        ) from exc
      raise exc

  def PrepareAfterBoot(self):
    """Prepares a VM after it has booted.

    This function will prepare a scratch disk if required.

    Raises:
        Exception: If --vm_metadata is malformed.
    """
    self.AddMetadata()
    self.OnStartup()
    self.SetupAllScratchDisks()
    self.PrepareVMEnvironment()
    self.RecordAdditionalMetadata()

  def AllowIcmp(self):
    """Opens ICMP protocol on the firewall corresponding to the VM if exists."""
    if self.firewall and not FLAGS.skip_firewall_rules:
      self.firewall.AllowIcmp(self)

  def AllowPort(self, start_port, end_port=None, source_range=None):
    """Opens the port on the firewall corresponding to the VM if one exists.

    Args:
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: list of CIDRs. If none, all sources are allowed.
    """
    if self.firewall and not FLAGS.skip_firewall_rules:
      self.firewall.AllowPort(self, start_port, end_port, source_range)

  def AllowRemoteAccessPorts(self):
    """Allow all ports in self.remote_access_ports."""
    for port in self.remote_access_ports:
      self.AllowPort(port)

  def AddMetadata(self, **kwargs):
    """Add key/value metadata to the instance.

    Setting the metadata on create is preferred. If that is not possible, this
    method adds metadata in the form of key value pairs to the instance. Useful
    for debugging / introspection.

    The default implementation is a noop. Cloud providers supporting instance
    metadata should override.

    Args:
      **kwargs: dict. (tag name, tag value) pairs to set as metadata on the
        instance.
    """
    pass

  def GetResourceMetadata(self):
    """Returns a dict containing VM metadata.

    Returns:
      dict mapping string property key to value.
    """
    if not self.created:
      return {}
    result = self.metadata.copy()
    result.update({
        'image': self.image,
        'cloud': self.CLOUD,
        'os_type': type(self).OS_TYPE,
        'vm_platform': self.PLATFORM,
    })
    if self.zone is not None:
      result['zone'] = self.zone
    if self.cidr is not None:
      result['cidr'] = self.cidr
    if self.machine_type is not None:
      result['machine_type'] = self.machine_type
    if self.use_dedicated_host is not None:
      result['dedicated_host'] = self.use_dedicated_host
    if self.num_vms_per_host is not None:
      result['num_vms_per_host'] = self.num_vms_per_host
    if self.tcp_congestion_control is not None:
      result['tcp_congestion_control'] = self.tcp_congestion_control
    if self.numa_node_count is not None:
      result['numa_node_count'] = self.numa_node_count
    if self.num_disable_cpus is not None:
      result['num_disable_cpus'] = self.num_disable_cpus
    if self.num_cpus is not None:
      result['num_cpus'] = self.num_cpus
    if (
        self.num_cpus is not None
        and self.NumCpusForBenchmark() != self.num_cpus
    ):
      result['num_benchmark_cpus'] = self.NumCpusForBenchmark()
    # Some metadata is unique per VM.
    # Update publisher._VM_METADATA_TO_LIST to add more
    if self.id is not None:
      result['id'] = self.id
    if self.name is not None:
      result['name'] = self.name
    if self.GetInternalIPs():
      result['internal_ips'] = self.GetInternalIPs()
    if self.GetExternalIPs():
      result['external_ips'] = self.GetExternalIPs()
    if pkb_flags.RECORD_PROCCPU.value and self.cpu_version:
      result['cpu_version'] = self.cpu_version
    if self.create_operation_name is not None:
      result['create_operation_name'] = self.create_operation_name
    if self.create_start_time:
      result['create_start_time'] = self.create_start_time
    return result

  def _InstallData(
      self,
      preprovisioned_data: Dict[str, str],
      module_name: str,
      filenames: List[str],
      install_path: str,
      fallback_url: Dict[str, str],
      timeout=PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Installs preprovisioned_data on this VM.

    Args:
      preprovisioned_data: The dict mapping filenames to sha256sum hashes.
      module_name: The name of the module defining the preprovisioned data.
      filenames: An iterable of preprovisioned data filenames for a particular
        module.
      install_path: The path to download the data file.
      fallback_url: The dict mapping filenames to fallback url for downloading.
      timeout: The timeout for downloading the data file.

    Raises:
      errors.Setup.BadPreprovisionedDataError: If the module or filename are
          not defined with preprovisioned data, or if the sha256sum hash in the
          code does not match the sha256 of the file.
    """
    for filename in filenames:
      if data.ResourceExists(filename):
        local_tar_file_path = data.ResourcePath(filename)
        self.PushFile(local_tar_file_path, install_path)
        continue
      url = fallback_url.get(filename)
      sha256sum = preprovisioned_data.get(filename)
      chunked = False
      try:
        # For extremely large files, we chunk into multiple files of 5GB each.
        # Named with [original_filename]_[index]].part, index starting at 000.
        # On Linux, run:
        # split -b 5G [filename] -d -a 3 --additional-suffix=.part filename_
        # Recovery is done during checksum verification.
        preprovisioned = self.ShouldDownloadPreprovisionedData(
            module_name, filename
        )
        if not preprovisioned:
          preprovisioned = self.ShouldDownloadPreprovisionedData(
              module_name, f'{filename}_000.part'
          )
          if preprovisioned:
            chunked = True
      except NotImplementedError:
        logging.info(
            'The provider does not implement '
            'ShouldDownloadPreprovisionedData. Attempting to '
            'download the data via URL'
        )
        preprovisioned = False
      if not FLAGS.preprovision_ignore_checksum and not sha256sum:
        raise errors.Setup.BadPreprovisionedDataError(
            'Cannot find sha256sum hash for file %s in module %s. Might want '
            'to run using --preprovision_ignore_checksum (not recommended). '
            'See README.md for information about preprovisioned data. '
            'Cannot find file in /data directory either, fail to upload from '
            'local directory.' % (filename, module_name)
        )
      # TODO(yuyanting): Cleanup the logic, extract chunked download to a
      # separate function.
      if preprovisioned:
        if not chunked:
          self.DownloadPreprovisionedData(
              install_path, module_name, filename, timeout
          )
        else:
          file_index = 0
          while True:
            chunked_filename = f'{filename}_{file_index:03d}.part'
            # TODO(yuyanting): Use list + parallel download is probably faser.
            # But in case we do not have list permission.
            if not self.ShouldDownloadPreprovisionedData(
                module_name, chunked_filename
            ):
              break
            self.DownloadPreprovisionedData(
                install_path, module_name, chunked_filename, timeout
            )
            file_index += 1
          self.RecoverChunkedPreprovisionedData(install_path, filename)
      elif url:
        self.Install('wget')
        file_name = os.path.basename(url)
        self.RemoteCommand(
            'wget -O {} {}'.format(os.path.join(install_path, file_name), url)
        )
      else:
        raise errors.Setup.BadPreprovisionedDataError(
            'Cannot find preprovisioned file %s inside preprovisioned bucket '
            'in module %s. See README.md for information about '
            'preprovisioned data. '
            'Cannot find fallback url of the file to download from web. '
            'Cannot find file in /data directory either, fail to upload from '
            'local directory.' % (filename, module_name)
        )
      if not FLAGS.preprovision_ignore_checksum:
        self.CheckPreprovisionedData(
            install_path, module_name, filename, sha256sum
        )

  def InstallPreprovisionedBenchmarkData(
      self,
      benchmark_name,
      filenames,
      install_path,
      timeout=PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Installs preprovisioned benchmark data on this VM.

    Some benchmarks require importing many bytes of data into the virtual
    machine. This data can be staged in a particular cloud and the virtual
    machine implementation can override how the preprovisioned data is
    installed in the VM by overriding DownloadPreprovisionedData.

    For example, for GCP VMs, benchmark data can be preprovisioned in a GCS
    bucket that the VMs may access. For a benchmark that requires
    preprovisioned data, follow the instructions for that benchmark to download
    and store the data so that it may be accessed by a VM via this method.

    Before installing from preprovisioned data in the cloud, this function looks
    for files in the local data directory. If found, they are pushed to the VM.
    Otherwise, this function attempts to download them from their preprovisioned
    location onto the VM.

    Args:
      benchmark_name: The name of the benchmark defining the preprovisioned
        data. The benchmark's module must define the dict BENCHMARK_DATA mapping
        filenames to sha256sum hashes.
      filenames: An iterable of preprovisioned data filenames for a particular
        benchmark.
      install_path: The path to download the data file.
      timeout: The timeout for downloading the data file.

    Raises:
      errors.Setup.BadPreprovisionedDataError: If the benchmark or filename are
          not defined with preprovisioned data, or if the sha256sum hash in the
          code does not match the sha256sum of the file.
    """
    benchmark_module = benchmark_lookup.BenchmarkModule(benchmark_name)
    if not benchmark_module:
      raise errors.Setup.BadPreprovisionedDataError(
          'Cannot install preprovisioned data for undefined benchmark %s.'
          % benchmark_name
      )
    try:
      # TODO(user): Change BENCHMARK_DATA to PREPROVISIONED_DATA.
      preprovisioned_data = benchmark_module.BENCHMARK_DATA
    except AttributeError:
      raise errors.Setup.BadPreprovisionedDataError(
          'Benchmark %s does not define a BENCHMARK_DATA dict with '
          'preprovisioned data.' % benchmark_name
      )
    fallback_url = getattr(benchmark_module, 'BENCHMARK_DATA_URL', {})
    self._InstallData(
        preprovisioned_data,
        benchmark_name,
        filenames,
        install_path,
        fallback_url,
        timeout,
    )

  def InstallPreprovisionedPackageData(
      self,
      package_name,
      filenames,
      install_path,
      timeout=PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Installs preprovisioned Package data on this VM.

    Some benchmarks require importing many bytes of data into the virtual
    machine. This data can be staged in a particular cloud and the virtual
    machine implementation can override how the preprovisioned data is
    installed in the VM by overriding DownloadPreprovisionedData.

    For example, for GCP VMs, benchmark data can be preprovisioned in a GCS
    bucket that the VMs may access. For a benchmark that requires
    preprovisioned data, follow the instructions for that benchmark to download
    and store the data so that it may be accessed by a VM via this method.

    Before installing from preprovisioned data in the cloud, this function looks
    for files in the local data directory. If found, they are pushed to the VM.
    Otherwise, this function attempts to download them from their preprovisioned
    location onto the VM.

    Args:
      package_name: The name of the package file defining the preprovisoned
        data. The default value is None. If the package_name is provided, the
        package file must define the dict PREPROVISIONED_DATA mapping filenames
        to sha256sum hashes.
      filenames: An iterable of preprovisioned data filenames for a particular
        package.
      install_path: The path to download the data file.
      timeout: The timeout for downloading the data file.

    Raises:
      errors.Setup.BadPreprovisionedDataError: If the package or filename are
          not defined with preprovisioned data, or if the sha256sum hash in the
          code does not match the sha256sum of the file.
    """
    package_module = package_lookup.PackageModule(package_name)
    if not package_module:
      raise errors.Setup.BadPreprovisionedDataError(
          'Cannot install preprovisioned data for undefined package %s.'
          % package_name
      )
    try:
      preprovisioned_data = package_module.PREPROVISIONED_DATA
    except AttributeError:
      raise errors.Setup.BadPreprovisionedDataError(
          'Package %s does not define a PREPROVISIONED_DATA dict with '
          'preprovisioned data.' % package_name
      )
    fallback_url = getattr(package_module, 'PACKAGE_DATA_URL', {})
    self._InstallData(
        preprovisioned_data,
        package_name,
        filenames,
        install_path,
        fallback_url,
        timeout,
    )

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available.

    This function should be overridden by each cloud provider VM.

    Args:
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.

    Returns:
      A boolean indicates if preprovisioned data is available.
    """
    raise NotImplementedError()

  def InstallCli(self):
    """Installs the cloud specific cli along with credentials on this vm."""
    raise NotImplementedError()

  def DownloadPreprovisionedData(
      self,
      install_path: str,
      module_name: str,
      filename: str,
      timeout: int = PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Downloads preprovisioned benchmark data.

    This function should be overridden by each cloud provider VM. The file
    should be downloaded into the install path within a subdirectory with the
    benchmark name.

    The downloaded file's parent directory will be created if it does not
    exist.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisioned data, default 5 min.
    """
    raise NotImplementedError()

  def IsInterruptible(self):
    """Returns whether this vm is a interruptible vm (e.g. spot, preemptible).

    Caller must call UpdateInterruptibleVmStatus before calling this function
    to make sure return value is up to date.

    Returns:
      True if this vm is a interruptible vm.
    """
    return False

  def _UpdateInterruptibleVmStatusThroughMetadataService(self):
    raise NotImplementedError()

  def _UpdateInterruptibleVmStatusThroughApi(self):
    # Azure do not support detecting through api
    pass

  def UpdateInterruptibleVmStatus(self, use_api=False):
    """Updates the status of the discounted vm.

    Args:
      use_api: boolean, If use_api is false, method will attempt to query
        metadata service to check vm preemption. If use_api is true, method will
        attempt to use API to detect vm preemption query if metadata service
        detecting fails.
    """
    if not self.IsInterruptible():
      return
    if self.WasInterrupted():
      return
    try:
      self._UpdateInterruptibleVmStatusThroughMetadataService()
    except (NotImplementedError, errors.VirtualMachine.RemoteCommandError):
      self._UpdateInterruptibleVmStatusThroughApi()

  def WasInterrupted(self):
    """Returns whether this interruptible vm was terminated early.

    Caller must call UpdateInterruptibleVmStatus before calling this function
    to make sure return value is up to date.

    Returns:
      True if this vm is a interruptible vm was terminated early.
    """
    return False

  def GetVmStatusCode(self):
    """Returns the vm status code if any.

    Caller must call UpdateInterruptibleVmStatus before calling this function
    to make sure return value is up to date.

    Returns:
      Vm status code.
    """
    return None

  def GetInterruptableStatusPollSeconds(self):
    """Get seconds between interruptable status polls.

    Returns:
      Seconds between polls
    """
    return 5

  def _PreDelete(self):
    """See base class."""
    self.LogVmDebugInfo()

  def UpdateTimeoutMetadata(self):
    """Updates the timeout tag for the VM."""

  def GetNumTeardownSkippedVms(self) -> int | None:
    """Returns the number of lingering VMs in this VM's zone."""


VirtualMachine = typing.TypeVar('VirtualMachine', bound=BaseVirtualMachine)
