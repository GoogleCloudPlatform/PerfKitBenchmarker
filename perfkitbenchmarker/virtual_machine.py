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
import contextlib
import enum
import logging
import os.path
import socket
import threading
import time
import typing
from typing import Any, Dict, List, Optional, Tuple, Union

from absl import flags
import jinja2
from perfkitbenchmarker import background_workload
from perfkitbenchmarker import benchmark_lookup
from perfkitbenchmarker import context as pkb_context
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import os_types
from perfkitbenchmarker import package_lookup
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
import six

FLAGS = flags.FLAGS
DEFAULT_USERNAME = 'perfkit'
QUOTA_EXCEEDED_MESSAGE = 'Creation failed due to quota exceeded: '


def ValidateVmMetadataFlag(options_list):
  """Verifies correct usage of the vm metadata flag.

  All provided options must be in the form of key:value.

  Args:
    options_list: A list of strings parsed from the provided value for the
      flag.

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
          '"%s" not in expected key:value format' % option)
  return True

# vm_metadata flag name
VM_METADATA = 'vm_metadata'

flags.DEFINE_boolean(
    'dedicated_hosts', False,
    'If True, use hosts that only have VMs from the same '
    'benchmark running on them.')
flags.DEFINE_integer(
    'num_vms_per_host', None,
    'The number of VMs per dedicated host. If None, VMs will be packed on a '
    'single host until no more can be packed at which point a new host will '
    'be created.')
flags.DEFINE_integer(
    'num_cpus_override', None,
    'Rather than detecting the number of CPUs present on the machine, use this '
    'value if set. Some benchmarks will use this number to automatically '
    'scale their configurations; this can be used as a method to control '
    'benchmark scaling. It will also change the num_cpus metadata '
    'published along with the benchmark data.')
flags.DEFINE_list(VM_METADATA, [], 'Metadata to add to the vm. It expects'
                  'key:value pairs.')
flags.register_validator(VM_METADATA, ValidateVmMetadataFlag)
flags.DEFINE_bool(
    'skip_firewall_rules', False,
    'If set, this run will not create firewall rules. This is useful if the '
    'user project already has all of the firewall rules in place and/or '
    'creating new ones is expensive')
flags.DEFINE_bool(
    'preprovision_ignore_checksum', False,
    'Ignore checksum verification for preprovisioned data. '
    'Not recommended, please use with caution')
flags.DEFINE_boolean(
    'connect_via_internal_ip', False,
    'Whether to use internal IP addresses for running commands on and pushing '
    'data to VMs. By default, PKB interacts with VMs using external IP '
    'addresses.')
_ASSIGN_EXTERNAL_IP = flags.DEFINE_boolean(
    'assign_external_ip',
    True,
    'If True, an external (public) IP will be created for VMs. '
    'If False, --connect_via_internal_ip may also be needed.')
flags.DEFINE_string(
    'boot_startup_script',
    None,
    (
        'Startup script to run during boot. '
        'Requires provider support, only implemented for Linux VMs '
        'on GCP, AWS, Azure for now.'
    ),
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
    'ssh_via_internal_ip', False,
    'Whether to use internal IP addresses for running commands on and pushing '
    'data to VMs. By default, PKB interacts with VMs using external IP '
    'addresses.')
flags.DEFINE_boolean('retry_on_rate_limited', True,
                     'Whether to retry commands when rate limited.')

GPU_K80 = 'k80'
GPU_P100 = 'p100'
GPU_V100 = 'v100'
GPU_A100 = 'a100'
GPU_H100 = 'h100'
GPU_P4 = 'p4'
GPU_P4_VWS = 'p4-vws'
GPU_T4 = 't4'
GPU_L4 = 'l4'
GPU_A10 = 'a10'
TESLA_GPU_TYPES = [
    GPU_K80,
    GPU_P100,
    GPU_V100,
    GPU_A100,
    GPU_P4,
    GPU_P4_VWS,
    GPU_T4,
    GPU_A10,
]
VALID_GPU_TYPES = TESLA_GPU_TYPES + [GPU_L4, GPU_H100]
CPUARCH_X86_64 = 'x86_64'
CPUARCH_AARCH64 = 'aarch64'

flags.DEFINE_integer(
    'gpu_count', None,
    'Number of gpus to attach to the VM. Requires gpu_type to be '
    'specified.')
flags.DEFINE_enum(
    'gpu_type', None, VALID_GPU_TYPES,
    'Type of gpus to attach to the VM. Requires gpu_count to be '
    'specified.')


def GetVmSpecClass(cloud):
  """Returns the VmSpec class corresponding to 'cloud'."""
  return spec.GetSpecClass(BaseVmSpec, CLOUD=cloud)


def GetVmClass(cloud, os_type):
  """Returns the VM class corresponding to 'cloud' and 'os_type'."""
  return resource.GetResourceClass(BaseVirtualMachine,
                                   CLOUD=cloud, OS_TYPE=os_type)


class BaseVmSpec(spec.BaseSpec):
  """Storing various data about a single vm.

  Attributes:
    zone: The region / zone the in which to launch the VM.
    cidr: The CIDR subnet range in which to launch the VM.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    gpu_count: None or int. Number of gpus to attach to the VM.
    gpu_type: None or string. Type of gpus to attach to the VM.
    image: The disk image to boot from.
    assign_external_ip: Bool.  If true, create an external (public) IP.
    install_packages: If false, no packages will be installed. This is
        useful if benchmark dependencies have already been installed.
    background_cpu_threads: The number of threads of background CPU usage
        while running the benchmark.
    background_network_mbits_per_sec: The number of megabits per second of
        background network traffic during the benchmark.
    background_network_ip_type: The IP address type (INTERNAL or
        EXTERNAL) to use for generating background network workload.
    disable_interrupt_moderation: If true, disables interrupt moderation.
    disable_rss: = If true, disables rss.
    boot_startup_script: Startup script to run during boot.
    vm_metadata: = Additional metadata for the VM.
  """

  SPEC_TYPE = 'BaseVmSpec'
  CLOUD = None

  def __init__(self, *args, **kwargs):
    self.zone = None
    self.cidr = None
    self.machine_type = None
    self.gpu_count = None
    self.gpu_type = None
    self.image = None
    self.assign_external_ip = None
    self.install_packages = None
    self.background_cpu_threads = None
    self.background_network_mbits_per_sec = None
    self.background_network_ip_type = None
    self.disable_interrupt_moderation = None
    self.disable_rss = None
    self.vm_metadata: Dict[str, Any] = None
    self.boot_startup_script: str = None
    super(BaseVmSpec, self).__init__(*args, **kwargs)

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
    super(BaseVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['image'].present:
      config_values['image'] = flag_values.image
    if flag_values['install_packages'].present:
      config_values['install_packages'] = flag_values.install_packages
    if flag_values['machine_type'].present:
      config_values['machine_type'] = flag_values.machine_type
    if flag_values['background_cpu_threads'].present:
      config_values['background_cpu_threads'] = (
          flag_values.background_cpu_threads)
    if flag_values['background_network_mbits_per_sec'].present:
      config_values['background_network_mbits_per_sec'] = (
          flag_values.background_network_mbits_per_sec)
    if flag_values['background_network_ip_type'].present:
      config_values['background_network_ip_type'] = (
          flag_values.background_network_ip_type)
    if flag_values['dedicated_hosts'].present:
      config_values['use_dedicated_host'] = flag_values.dedicated_hosts
    if flag_values['num_vms_per_host'].present:
      config_values['num_vms_per_host'] = flag_values.num_vms_per_host
    if flag_values['gpu_type'].present:
      config_values['gpu_type'] = flag_values.gpu_type
    if flag_values['gpu_count'].present:
      config_values['gpu_count'] = flag_values.gpu_count
    if flag_values['assign_external_ip'].present:
      config_values['assign_external_ip'] = flag_values.assign_external_ip
    if flag_values['disable_interrupt_moderation'].present:
      config_values['disable_interrupt_moderation'] = (
          flag_values.disable_interrupt_moderation)
    if flag_values['disable_rss'].present:
      config_values['disable_rss'] = flag_values.disable_rss
    if flag_values['vm_metadata'].present:
      config_values['vm_metadata'] = flag_values.vm_metadata
    if flag_values['boot_startup_script'].present:
      config_values['boot_startup_script'] = flag_values.boot_startup_script

    if 'gpu_count' in config_values and 'gpu_type' not in config_values:
      raise errors.Config.MissingOption(
          'gpu_type must be specified if gpu_count is set')
    if 'gpu_type' in config_values and 'gpu_count' not in config_values:
      raise errors.Config.MissingOption(
          'gpu_count must be specified if gpu_type is set')

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
    result = super(BaseVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'disable_interrupt_moderation': (option_decoders.BooleanDecoder, {
            'default': False}),
        'disable_rss': (option_decoders.BooleanDecoder, {'default': False}),
        'image': (option_decoders.StringDecoder, {'none_ok': True,
                                                  'default': None}),
        'install_packages': (option_decoders.BooleanDecoder, {'default': True}),
        'machine_type': (option_decoders.StringDecoder, {'none_ok': True,
                                                         'default': None}),
        'assign_external_ip': (
            option_decoders.BooleanDecoder,
            {'default': True},
        ),
        'gpu_type': (option_decoders.EnumDecoder, {
            'valid_values': VALID_GPU_TYPES,
            'default': None}),
        'gpu_count': (option_decoders.IntDecoder, {'min': 1, 'default': None}),
        'zone': (option_decoders.StringDecoder, {'none_ok': True,
                                                 'default': None}),
        'cidr': (option_decoders.StringDecoder, {'none_ok': True,
                                                 'default': None}),
        'use_dedicated_host': (option_decoders.BooleanDecoder,
                               {'default': False}),
        'num_vms_per_host': (option_decoders.IntDecoder,
                             {'default': None}),
        'background_network_mbits_per_sec': (option_decoders.IntDecoder, {
            'none_ok': True, 'default': None}),
        'background_network_ip_type': (option_decoders.EnumDecoder, {
            'default': vm_util.IpAddressSubset.EXTERNAL,
            'valid_values': [vm_util.IpAddressSubset.EXTERNAL,
                             vm_util.IpAddressSubset.INTERNAL]}),
        'background_cpu_threads': (option_decoders.IntDecoder, {
            'none_ok': True, 'default': None}),
        'boot_startup_script': (option_decoders.StringDecoder, {
            'none_ok': True, 'default': None}),
        'vm_metadata': (option_decoders.ListDecoder, {
            'item_decoder': option_decoders.StringDecoder(),
            'default': []})})
    return result


class BaseOsMixin(six.with_metaclass(abc.ABCMeta, object)):
  """The base class for OS Mixin classes.

  This class holds VM methods and attributes relating to the VM's guest OS.
  For methods and attributes that relate to the VM as a cloud resource,
  see BaseVirtualMachine and its subclasses.

  Attributes:
    bootable_time: The time when the VM finished booting.
    hostname: The VM's hostname.
    remote_access_ports: A list of ports which must be opened on the firewall
        in order to access the VM.
  """
  # Represents whether the VM type can be (cleanly) rebooted. Should be false
  # for a class if rebooting causes issues, e.g. for KubernetesVirtualMachine
  # needing to reboot often indicates a design problem since restarting a
  # container can have side effects in certain situations.
  IS_REBOOTABLE = True

  password: str = None
  install_packages: bool  # mixed from BaseVirtualMachine
  is_static: bool  # mixed from BaseVirtualMachine
  scratch_disks: List[disk.BaseDisk]  # mixed from BaseVirtualMachine
  name: str  # mixed from BaseVirtualMachine
  ssh_private_key: str  # mixed from BaseVirtualMachine
  user_name: str  # mixed from BaseVirtualMachine
  disable_interrupt_moderation: str  # mixed from BaseVirtualMachine
  disable_rss: str  # mixed from BaseVirtualMachine
  num_disable_cpus: str  # mixed from BaseVirtualMachine
  ip_address: str  # mixed from BaseVirtualMachine
  internal_ip: str  # mixed from BaseVirtualMachine
  can_connect_via_internal_ip: bool  # mixed from BaseVirtualMachine
  boot_completion_ip_subset: bool  # mixed from BaseVirtualMachine

  @abc.abstractmethod
  def GetConnectionIp(self):
    """See BaseVirtualMachine."""

  def __init__(self):
    super(BaseOsMixin, self).__init__()
    self._installed_packages = set()
    self.startup_script_output = None
    self.postrun_script_output = None
    self.bootable_time = None
    self.port_listening_time = None
    self.hostname = None

    # Ports that will be opened by benchmark_spec to permit access to the VM.
    self.remote_access_ports = []
    # Port to be used to see if VM is ready to receive a remote command.
    self.primary_remote_access_port = None

    # Cached values
    self._reachable = {}
    self._total_memory_kb = None
    self._num_cpus = None
    self._is_smt_enabled = None
    # Update to Json type if ever available:
    # https://github.com/python/typing/issues/182
    self.os_metadata: Dict[str, Union[str, int, list[str]]] = {}
    assert type(
        self).BASE_OS_TYPE in os_types.BASE_OS_TYPES, '%s is not in %s' % (
            type(self).BASE_OS_TYPE, os_types.BASE_OS_TYPES)

  @property
  @classmethod
  @abc.abstractmethod
  def OS_TYPE(cls):
    raise NotImplementedError()

  @property
  @classmethod
  @abc.abstractmethod
  def BASE_OS_TYPE(cls):
    raise NotImplementedError()

  def GetOSResourceMetadata(self) -> Dict[str, Union[str, int, list[str]]]:
    """Returns a dict containing VM OS metadata.

    Returns:
      dict mapping string property key to value.
    """
    return self.os_metadata

  def CreateRamDisk(self, disk_spec):
    """Create and mount Ram disk."""
    raise NotImplementedError()

  def RemoteCommand(
      self,
      command: str,
      ignore_failure: bool = False,
      timeout: Optional[float] = None,
      **kwargs
  ) -> Tuple[str, str]:
    """Runs a command on the VM.

    Derived classes may add additional kwargs if necessary, but they should not
    be used outside of the class itself since they are non standard.

    Args:
      command: A valid bash command.
      ignore_failure: Ignore any failure if set to true.
      timeout: The time to wait in seconds for the command before exiting.
          None means no timeout.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    raise NotImplementedError()

  def RobustRemoteCommand(
      self,
      command: str,
      timeout: Optional[float] = None,
      ignore_failure: bool = False,
  ) -> Tuple[str, str]:
    """Runs a command on the VM in a more robust way than RemoteCommand.

    The default should be to call RemoteCommand and log that it is not yet
    implemented. This function should be overwritten it is decendents.

    Args:
      command: The command to run.
      timeout: The timeout for the command in seconds.
      ignore_failure: Ignore any failure if set to true.

    Returns:
      A tuple of stdout, stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem establishing the connection, or
          the command fails.
    """
    logging.info('RobustRemoteCommand not implemented, using RemoteCommand.')
    return self.RemoteCommand(command, ignore_failure, timeout)

  def TryRemoteCommand(self, command: str, **kwargs):
    """Runs a remote command and returns True iff it succeeded."""
    try:
      self.RemoteCommand(command, **kwargs)
      return True
    except errors.VirtualMachine.RemoteCommandError:
      return False
    except:
      raise

  def Reboot(self):
    """Reboots the VM.

    Returns:
      The duration in seconds from the time the reboot command was issued to
      the time we could SSH into the VM and verify that the timestamp changed.

    Raises:
      Exception: If trying to reboot a VM that isn't rebootable
        (e.g. Kubernetes).
    """
    if not self.IS_REBOOTABLE:
      raise errors.VirtualMachine.VirtualMachineError(
          "Trying to reboot a VM that isn't rebootable.")

    vm_bootable_time = None

    # Use self.bootable_time to determine if this is the first boot.
    # On the first boot, WaitForBootCompletion will only run once.
    # On subsequent boots, need to WaitForBootCompletion and ensure
    # the last boot time changed.
    if self.bootable_time is not None:
      vm_bootable_time = self.VMLastBootTime()

    before_reboot_timestamp = time.time()
    self._Reboot()

    while True:
      self.WaitForBootCompletion()
      # WaitForBootCompletion ensures that the machine is up
      # this is sufficient check for the first boot - but not for a reboot
      if vm_bootable_time != self.VMLastBootTime():
        break
    reboot_duration_sec = time.time() - before_reboot_timestamp
    self._AfterReboot()
    return reboot_duration_sec

  def _BeforeSuspend(self):
    pass

  def _PostSuspend(self):
    pass

  def Suspend(self) -> float:
    """Suspends the vm.

      Future plans and edge cases: checking if a vm is suspendable.
      Accidentally suspending a VM that is already suspending.
      Trying to resume a VM that is not already suspended.

    Returns:
      The amount of time it takes to Suspend a VM that is suspendable.
    """
    self._BeforeSuspend()

    before_suspend_timestamp = time.time()

    self._Suspend()
    self._PostSuspend()

    return time.time() - before_suspend_timestamp

  def Resume(self) -> float:
    """Resumes the vm.

    Returns:
      The amount of time it takes to resume a VM that is suspendable.
    """
    before_resume_timestamp = time.time()
    self._Resume()

    # WaitForSSH tries to ssh into the VM,ensuring resume was successful
    self._WaitForSSH()

    return time.time() - before_resume_timestamp

  def _Reboot(self):
    """OS-specific implementation of reboot command."""
    raise NotImplementedError()

  def _Suspend(self):
    """Provider specific implementation of a VM suspend command."""
    raise NotImplementedError()

  def _Resume(self):
    """Provider specific implementation of a VM resume command."""
    raise NotImplementedError()

  def _AfterReboot(self):
    """Performs any OS-specific setup on the VM following reboot.

    This will be called after every call to Reboot().
    """
    pass

  def Start(self) -> float:
    """Starts the VM.

    Returns:
      The duration in seconds from the time the start command was issued to
      the time we could SSH into the VM and verify that the timestamp changed.
    """

    before_start_timestamp = time.time()
    self._Start()
    self._PostStart()
    self._WaitForSSH()
    start_duration_sec = time.time() - before_start_timestamp
    return start_duration_sec

  @abc.abstractmethod
  def _Start(self):
    """Provider-specific implementation of start command."""
    raise NotImplementedError()

  def _PostStart(self):
    """Provider-specific checks after start command."""
    pass

  def Stop(self) -> float:
    """Stop the VM.

    Returns:
      The duration in seconds from the time the start command was issued to
      after the API call
    """

    before_stop_timestamp = time.time()
    self._Stop()
    self._PostStop()
    stop_duration_sec = time.time() - before_stop_timestamp
    return stop_duration_sec

  def _Stop(self):
    """Provider-specific implementation of stop command."""
    raise NotImplementedError()

  def _PostStop(self):
    """Provider-specific checks after stop command."""
    pass

  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    raise NotImplementedError()

  def WaitForBootCompletion(self):
    """Waits until VM is has booted.

    Implementations of this method should set the 'bootable_time' attribute
    and the 'hostname' attribute.

    If bootable_time is not set, this indicates a failure due to an
    unreachable VM, and is reported as a BootCompletionError.
    """
    raise NotImplementedError()

  def _WaitForSSH(self, ip_address: Union[str, None] = None):
    """Waits until VM is ready.

    Implementations of this method should set the 'hostname' attribute.

    Args:
      ip_address: The IP address to use for SSH, if None an implementation
        default is used.
    """
    raise NotImplementedError()

  def VMLastBootTime(self):
    """Returns the time the VM was last rebooted as reported by the VM.
    """
    raise NotImplementedError()

  def OnStartup(self):
    """Performs any necessary setup on the VM specific to the OS.

    This will be called once immediately after the VM has booted.
    """
    events.on_vm_startup.send(vm=self)
    # Resets the cached SMT enabled status and number cpus value.
    self._is_smt_enabled = None
    self._num_cpus = None

  def PrepareVMEnvironment(self):
    """Performs any necessary setup on the VM specific to the OS.

    This will be called once after setting up scratch disks.
    """
    if self.disable_interrupt_moderation:
      self.DisableInterruptModeration()

    if self.disable_rss:
      self.DisableRSS()

  def DisableInterruptModeration(self):
    """Disables interrupt moderation on the VM."""
    raise NotImplementedError()

  def DisableRSS(self):
    """Disables RSS on the VM."""
    raise NotImplementedError()

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    raise NotImplementedError()

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    raise NotImplementedError()

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the temp directory, restores packages, and uninstalls all
    PerfKit packages.
    """
    raise NotImplementedError()

  def SetupLocalDisks(self):
    """Perform OS specific setup on any local disks that exist."""
    pass

  def LogVmDebugInfo(self):
    """Logs OS-specific debug info. Must be overridden on an OS mixin."""
    pass

  def PushFile(self, source_path, remote_path=''):
    """Copies a file or a directory to the VM.

    Args:
      source_path: The location of the file or directory on the LOCAL machine.
      remote_path: The destination of the file on the REMOTE machine, default
          is the home directory.
    """
    self.RemoteCopy(source_path, remote_path)

  def PullFile(self, local_path, remote_path):
    """Copies a file or a directory from the VM to the local machine.

    Args:
      local_path: string. The destination path of the file or directory on the
          local machine.
      remote_path: string. The source path of the file or directory on the
          remote machine.
    """
    self.RemoteCopy(local_path, remote_path, copy_to=False)

  def PushDataFile(self, data_file, remote_path='', should_double_copy=None):
    """Upload a file in perfkitbenchmarker.data directory to the VM.

    Args:
      data_file: The filename of the file to upload.
      remote_path: The destination for 'data_file' on the VM. If not specified,
        the file will be placed in the user's home directory.
      should_double_copy: Indicates whether to first copy to the home directory
    Raises:
      perfkitbenchmarker.data.ResourceNotFound: if 'data_file' does not exist.
    """
    file_path = data.ResourcePath(data_file)
    if should_double_copy:
      home_file_path = '~/' + data_file
      self.PushFile(file_path, home_file_path)
      copy_cmd = (' '.join(['cp', home_file_path, remote_path]))
      self.RemoteCommand(copy_cmd)
    else:
      self.PushFile(file_path, remote_path)

  def RenderTemplate(self, template_path, remote_path, context):
    """Renders a local Jinja2 template and copies it to the remote host.

    The template will be provided variables defined in 'context', as well as a
    variable named 'vm' referencing this object.

    Args:
      template_path: string. Local path to jinja2 template.
      remote_path: string. Remote path for rendered file on the remote vm.
      context: dict. Variables to pass to the Jinja2 template during rendering.

    Raises:
      jinja2.UndefinedError: if template contains variables not present in
        'context'.
      RemoteCommandError: If there was a problem copying the file.
    """
    with open(template_path) as fp:
      template_contents = fp.read()

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = environment.from_string(template_contents)
    prefix = 'pkb-' + os.path.basename(template_path)

    with vm_util.NamedTemporaryFile(prefix=prefix, dir=vm_util.GetTempDir(),
                                    delete=False, mode='w') as tf:
      tf.write(template.render(vm=self, **context))
      tf.close()
      self.RemoteCopy(tf.name, remote_path)

  def DiskCreatedOnVMCreation(self, data_disk):
    """Returns whether the disk has been created during VM creation."""
    return data_disk.disk_type == disk.LOCAL

  def _CreateScratchDiskFromDisks(self, disk_spec, disks):
    """Helper method to create scratch data disks.

    Given a list of BaseDisk objects, create and attach scratch disk to VM.
    Multiple BaseDisk objects are striped and combined into one 'logical' data
    disk and treated as a single disk from a benchmarks perspective.

    Args:
      disk_spec: The BaseDiskSpec object corresponding to the disk.
      disks: A list of the disk(s) to be created, attached, and striped.

    Returns:
      The created scratch disk.
    """
    if len(disks) > 1:
      # If the disk_spec called for a striped disk, create one.
      scratch_disk = disk.StripedDisk(disk_spec, disks)
    else:
      scratch_disk = disks[0]

    if not self.DiskCreatedOnVMCreation(scratch_disk):
      scratch_disk.Create()
      scratch_disk.Attach(self)

    return scratch_disk

  def _PrepareScratchDisk(self, scratch_disk, disk_spec):
    """Helper method to format and mount scratch disk.

    Args:
      scratch_disk: Scratch disk to be formatted and mounted.
      disk_spec: The BaseDiskSpec object corresponding to the disk.
    """
    raise NotImplementedError()

  @property
  def num_cpus(self):
    """Gets the number of CPUs on the VM.

    Returns:
      The number of CPUs on the VM.
    """
    if self._num_cpus is None:
      self._num_cpus = self._GetNumCpus()
    # If num_cpus is set to -1, this means the VM was unreachable, preventing
    # any attempt to capture the CPU count.
    if self._num_cpus == -1:
      raise vm_util.UnreachableVmError(
          'VM connection attempts failed.'
          + ' num_cpus: skipping CPU count capture.')
    return self._num_cpus

  def NumCpusForBenchmark(self, report_only_physical_cpus=False):
    """Gets the number of CPUs for benchmark configuration purposes.

    Many benchmarks scale their configurations based off of the number of CPUs
    available on the system (e.g. determine the number of threads). Benchmarks
    should use this property rather than num_cpus so that users can override
    that behavior with the --num_cpus_override flag. Not all benchmarks may
    use this, and they may use more than this number of CPUs. To actually
    ensure that they are not being used during benchmarking, the CPUs should be
    disabled.

    Args:
      report_only_physical_cpus: Whether to report only the physical
      (non-SMT) CPUs.  Default is to report all vCPUs.

    Returns:
      The number of CPUs for benchmark configuration purposes.
    """
    if FLAGS.num_cpus_override:
      return FLAGS.num_cpus_override
    if report_only_physical_cpus and self.IsSmtEnabled():
      # return half the number of CPUs.
      return self.num_cpus // 2
    if self.num_disable_cpus:
      return self.num_cpus - self.num_disable_cpus
    return self.num_cpus

  def _GetNumCpus(self):
    """Returns the number of logical CPUs on the VM.

    This method does not cache results (unlike "num_cpus").
    """
    raise NotImplementedError()

  @property
  def total_free_memory_kb(self):
    """Gets the amount of free memory on the VM.

    Returns:
      The number of kilobytes of memory on the VM.
    """
    return self._GetTotalFreeMemoryKb()

  @property
  def total_memory_kb(self):
    """Gets the amount of memory on the VM.

    Returns:
      The number of kilobytes of memory on the VM.
    """
    if not self._total_memory_kb:
      self._total_memory_kb = self._GetTotalMemoryKb()
    return self._total_memory_kb

  def _GetTotalFreeMemoryKb(self):
    """Returns the amount of free physical memory on the VM in Kilobytes."""
    raise NotImplementedError()

  def _GetTotalMemoryKb(self):
    """Returns the amount of physical memory on the VM in Kilobytes.

    This method does not cache results (unlike "total_memory_kb").
    """
    raise NotImplementedError()

  def IsReachable(self, target_vm):
    """Indicates whether the target VM can be reached from its internal ip.

    Args:
      target_vm: The VM whose reachability is being tested.

    Returns:
      True if the internal ip address of the target VM can be reached, false
      otherwise.
    """
    if target_vm not in self._reachable:
      if target_vm.internal_ip:
        self._reachable[target_vm] = self._TestReachable(target_vm.internal_ip)
      else:
        self._reachable[target_vm] = False
    return self._reachable[target_vm]

  def _TestReachable(self, ip):
    """Returns True if the VM can reach the ip address and False otherwise."""
    raise NotImplementedError()

  def StartBackgroundWorkload(self):
    """Start the background workload."""
    for workload in background_workload.BACKGROUND_WORKLOADS:
      if workload.IsEnabled(self):
        if self.OS_TYPE in workload.EXCLUDED_OS_TYPES:
          raise NotImplementedError()
        workload.Start(self)

  def StopBackgroundWorkload(self):
    """Stop the background workoad."""
    for workload in background_workload.BACKGROUND_WORKLOADS:
      if workload.IsEnabled(self):
        if self.OS_TYPE in workload.EXCLUDED_OS_TYPES:
          raise NotImplementedError()
        workload.Stop(self)

  def PrepareBackgroundWorkload(self):
    """Prepare for the background workload."""
    for workload in background_workload.BACKGROUND_WORKLOADS:
      if workload.IsEnabled(self):
        if self.OS_TYPE in workload.EXCLUDED_OS_TYPES:
          raise NotImplementedError()
        workload.Prepare(self)

  def SetReadAhead(self, num_sectors, devices):
    """Set read-ahead value for block devices.

    Args:
      num_sectors: int. Number of sectors of read ahead.
      devices: list of strings. A list of block devices.
    """
    raise NotImplementedError()

  def GetSha256sum(self, path, filename):
    """Gets the sha256sum hash for a filename in a path on the VM.

    Args:
      path: string; Path on the VM.
      filename: string; Name of the file in the path.

    Returns:
      string; The sha256sum hash.
    """
    raise NotImplementedError()

  def CheckPreprovisionedData(self, install_path, module_name, filename,
                              expected_sha256):
    """Checks preprovisioned data for a checksum.

    Checks the expected 256sum against the actual sha256sum. Called after the
    file is downloaded.

    This function should be overridden by each OS-specific MixIn.

    Args:
      install_path: The install path on this VM. The benchmark is installed at
          this path in a subdirectory of the benchmark name.
      module_name: Name of the benchmark associated with this data file.
      filename: The name of the file that was downloaded.
      expected_sha256: The expected sha256 checksum value.
    """
    actual_sha256 = self.GetSha256sum(install_path, filename)
    if actual_sha256 != expected_sha256:
      raise errors.Setup.BadPreprovisionedDataError(
          'Invalid sha256sum for %s/%s: %s (actual) != %s (expected). Might '
          'want to run using --preprovision_ignore_checksum '
          '(not recommended).' % (
              module_name, filename, actual_sha256, expected_sha256))

  def TestConnectRemoteAccessPort(self, port=None, socket_timeout=0.5):
    """Tries to connect to remote access port and throw if it fails.

    Args:
      port: Integer of the port to connect to. Defaults to
          the default remote connection port of the VM.
      socket_timeout: The number of seconds to wait on the socket before failing
          and retrying. If this is too low, the connection may never succeed. If
          this is too high it will add latency (because the current connection
          may fail after a time that a new connection would succeed).
          Defaults to 500ms.
    """
    if not port:
      port = self.primary_remote_access_port
    # TODO(user): refactor to reuse sockets?
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) \
        as sock:
      # Before the IP is reachable the socket times out (and throws). After that
      # it throws immediately.
      sock.settimeout(socket_timeout)  # seconds
      sock.connect((self.GetConnectionIp(), port))
    logging.info('Connected to port %s on %s', port, self)

  def IsSmtEnabled(self):
    """Whether simultaneous multithreading (SMT) is enabled on the vm."""
    if self._is_smt_enabled is None:
      self._is_smt_enabled = self._IsSmtEnabled()
    return self._is_smt_enabled

  def _IsSmtEnabled(self):
    """Whether SMT is enabled on the vm."""
    raise NotImplementedError()

  def _GetNfsService(self):
    """Returns the NfsService created in the benchmark spec.

    Before calling this method check that the disk.disk_type is equal to
    disk.NFS or else an exception will be raised.

    Returns:
      The nfs_service.BaseNfsService service for this cloud.

    Raises:
      CreationError: If no NFS service was created.
    """
    nfs = getattr(pkb_context.GetThreadBenchmarkSpec(), 'nfs_service')
    if nfs is None:
      raise errors.Resource.CreationError('No NFS Service created')
    return nfs

  # TODO(pclay): Implement on Windows, make abstract and non Optional
  @property
  def cpu_arch(self) -> Optional[str]:
    """The basic CPU architecture of the VM."""
    return None


class DeprecatedOsMixin(BaseOsMixin):
  """Class that adds a deprecation log message to OsBasedVms."""

  # The time or version in which this OS input will be removed
  END_OF_LIFE = None

  # Optional alternative to use instead.
  ALTERNATIVE_OS = None

  def __init__(self):
    super(DeprecatedOsMixin, self).__init__()
    assert self.OS_TYPE
    assert self.END_OF_LIFE
    warning = "os_type '%s' is deprecated and will be removed after %s." % (
        self.OS_TYPE, self.END_OF_LIFE)
    if self.ALTERNATIVE_OS:
      warning += " Use '%s' instead." % self.ALTERNATIVE_OS
    logging.warning(warning)


class BaseVirtualMachine(BaseOsMixin, resource.BaseResource):
  """Base class for Virtual Machines.

  This class holds VM methods and attributes relating to the VM as a cloud
  resource. For methods and attributes that interact with the VM's guest
  OS, see BaseOsMixin and its subclasses.

  Attributes:
    image: The disk image used to boot.
    internal_ip: Internal IP address.
    assign_external_ip: If True, create an external (public) IP.
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
    background_cpu_threads: The number of threads of background CPU usage
      while running the benchmark.
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
  REQUIRED_ATTRS = ['CLOUD', 'OS_TYPE']

  _instance_counter_lock = threading.Lock()
  _instance_counter = 0

  def __init__(self, vm_spec: BaseVmSpec):
    """Initialize BaseVirtualMachine class.

    Args:
      vm_spec: virtual_machine.BaseVmSpec object of the vm.
    """
    super(BaseVirtualMachine, self).__init__()
    with self._instance_counter_lock:
      self.instance_number = self._instance_counter
      self.name = 'pkb-%s-%d' % (FLAGS.run_uri, self.instance_number)
      BaseVirtualMachine._instance_counter += 1
    self.disable_interrupt_moderation = vm_spec.disable_interrupt_moderation
    self.disable_rss = vm_spec.disable_rss
    self.zone = vm_spec.zone
    self.cidr = vm_spec.cidr
    self.machine_type = vm_spec.machine_type
    self.gpu_count = vm_spec.gpu_count
    self.gpu_type = vm_spec.gpu_type
    self.image = vm_spec.image
    self.install_packages = vm_spec.install_packages
    self.can_connect_via_internal_ip = (FLAGS.ssh_via_internal_ip
                                        or FLAGS.connect_via_internal_ip)
    self.boot_completion_ip_subset = _BOOT_COMPLETION_IP_SUBSET.value
    self.assign_external_ip = vm_spec.assign_external_ip
    self.ip_address = None
    self.internal_ip = None
    self.internal_ips = []
    self.user_name = DEFAULT_USERNAME
    self.ssh_public_key = vm_util.GetPublicKeyPath()
    self.ssh_private_key = vm_util.GetPrivateKeyPath()
    self.disk_specs = []
    self.scratch_disks = []
    self.max_local_disks = 0
    self.local_disk_counter = 0
    self.remote_disk_counter = 0
    self.background_cpu_threads = vm_spec.background_cpu_threads
    self.background_network_mbits_per_sec = (
        vm_spec.background_network_mbits_per_sec)
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
    self.is_aarch64 = False
    self.create_operation_name = None
    self.create_return_time = None
    self.is_running_time = None
    self.boot_startup_script = vm_spec.boot_startup_script
    if self.OS_TYPE == os_types.CORE_OS and self.boot_startup_script:
      raise errors.Setup.InvalidConfigurationError(
          'Startup script are not supported on CoreOS.')

  @property
  @classmethod
  @abc.abstractmethod
  def CLOUD(cls):
    raise NotImplementedError()

  def __repr__(self):
    return '<BaseVirtualMachine [ip={0}, internal_ip={1}]>'.format(
        self.ip_address, self.internal_ip)

  def __str__(self):
    if self.ip_address:
      return self.ip_address
    return super(BaseVirtualMachine, self).__str__()

  def GetConnectionIp(self):
    """Gets the IP to use for connecting to the VM."""
    if not self.created:
      raise errors.VirtualMachine.VirtualMachineError(
          'VM was not properly created, but PKB is attempting to connect to '
          'it anyways. Caller should guard against VM not being created.')
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
          'GetScratchDir(disk_num=%s) is invalid, max disk_num is %s' % (
              disk_num, len(self.scratch_disks)))
    return self.scratch_disks[disk_num].mount_point

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
        'zone': self.zone,
        'cloud': self.CLOUD,
        'os_type': type(self).OS_TYPE,
    })
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
    try:
      if self.num_cpus is not None:
        result['num_cpus'] = self.num_cpus
        if self.NumCpusForBenchmark() != self.num_cpus:
          result['num_benchmark_cpus'] = self.NumCpusForBenchmark()
    except vm_util.UnreachableVmError:
      logging.info('GetResourceMetadata: '
                   + 'Unable to determine CPU count due to an unreachable VM.')
    # Some metadata is unique per VM.
    # Update publisher._VM_METADATA_TO_LIST to add more
    if self.id is not None:
      result['id'] = self.id
    if self.name is not None:
      result['name'] = self.name
    if self.ip_address is not None:
      result['ip_address'] = self.ip_address
    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    raise NotImplementedError()

  def SetupLMNotification(self):
    """Prepare environment for /scripts/gce_maintenance_notify.py script."""
    raise NotImplementedError()

  def _GetLMNotifificationCommand(self):
    """Return Remote python execution command for LM notify script."""
    raise NotImplementedError()

  def StartLMNotification(self):
    """Start meta-data server notification subscription."""
    raise NotImplementedError()

  def WaitLMNotificationRelease(self):
    """Block main thread until LM ended."""
    raise NotImplementedError()

  def CollectLMNotificationsTime(self):
    """Extract LM notifications from log file."""
    raise NotImplementedError()

  def _InstallData(self, preprovisioned_data, module_name, filenames,
                   install_path, fallback_url):
    """Installs preprovisioned_data on this VM.

    Args:
      preprovisioned_data: The dict mapping filenames to sha256sum hashes.
      module_name: The name of the module defining the preprovisioned data.
      filenames: An iterable of preprovisioned data filenames for a particular
      module.
      install_path: The path to download the data file.
      fallback_url: The dict mapping filenames to fallback url for downloading.

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
      try:
        preprovisioned = self.ShouldDownloadPreprovisionedData(
            module_name, filename)
      except NotImplementedError:
        logging.info('The provider does not implement '
                     'ShouldDownloadPreprovisionedData. Attempting to '
                     'download the data via URL')
        preprovisioned = False
      if not FLAGS.preprovision_ignore_checksum and not sha256sum:
        raise errors.Setup.BadPreprovisionedDataError(
            'Cannot find sha256sum hash for file %s in module %s. Might want '
            'to run using --preprovision_ignore_checksum (not recommended). '
            'See README.md for information about preprovisioned data. '
            'Cannot find file in /data directory either, fail to upload from '
            'local directory.' % (filename, module_name))

      if preprovisioned:
        self.DownloadPreprovisionedData(install_path, module_name, filename)
      elif url:
        self.Install('wget')
        file_name = os.path.basename(url)
        self.RemoteCommand(
            'wget -O {0} {1}'.format(
                os.path.join(install_path, file_name), url))
      else:
        raise errors.Setup.BadPreprovisionedDataError(
            'Cannot find preprovisioned file %s inside preprovisioned bucket '
            'in module %s. See README.md for information about '
            'preprovisioned data. '
            'Cannot find fallback url of the file to download from web. '
            'Cannot find file in /data directory either, fail to upload from '
            'local directory.' % (filename, module_name))
      if not FLAGS.preprovision_ignore_checksum:
        self.CheckPreprovisionedData(
            install_path, module_name, filename, sha256sum)

  def InstallPreprovisionedBenchmarkData(self, benchmark_name, filenames,
                                         install_path):
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
          data. The benchmark's module must define the dict BENCHMARK_DATA
          mapping filenames to sha256sum hashes.
      filenames: An iterable of preprovisioned data filenames for a particular
          benchmark.
      install_path: The path to download the data file.

    Raises:
      errors.Setup.BadPreprovisionedDataError: If the benchmark or filename are
          not defined with preprovisioned data, or if the sha256sum hash in the
          code does not match the sha256sum of the file.
    """
    benchmark_module = benchmark_lookup.BenchmarkModule(benchmark_name)
    if not benchmark_module:
      raise errors.Setup.BadPreprovisionedDataError(
          'Cannot install preprovisioned data for undefined benchmark %s.' %
          benchmark_name)
    try:
      # TODO(user): Change BENCHMARK_DATA to PREPROVISIONED_DATA.
      preprovisioned_data = benchmark_module.BENCHMARK_DATA
    except AttributeError:
      raise errors.Setup.BadPreprovisionedDataError(
          'Benchmark %s does not define a BENCHMARK_DATA dict with '
          'preprovisioned data.' % benchmark_name)
    fallback_url = getattr(benchmark_module, 'BENCHMARK_DATA_URL', {})
    self._InstallData(preprovisioned_data, benchmark_name, filenames,
                      install_path, fallback_url)

  def InstallPreprovisionedPackageData(self, package_name, filenames,
                                       install_path):
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
      data. The default vaule is None. If the package_name is provided, the
      package file must define the dict PREPROVISIONED_DATA mapping filenames to
      sha256sum hashes.
      filenames: An iterable of preprovisioned data filenames for a particular
      package.
      install_path: The path to download the data file.

    Raises:
      errors.Setup.BadPreprovisionedDataError: If the package or filename are
          not defined with preprovisioned data, or if the sha256sum hash in the
          code does not match the sha256sum of the file.
    """
    package_module = package_lookup.PackageModule(package_name)
    if not package_module:
      raise errors.Setup.BadPreprovisionedDataError(
          'Cannot install preprovisioned data for undefined package %s.' %
          package_name)
    try:
      preprovisioned_data = package_module.PREPROVISIONED_DATA
    except AttributeError:
      raise errors.Setup.BadPreprovisionedDataError(
          'Package %s does not define a PREPROVISIONED_DATA dict with '
          'preprovisioned data.' % package_name)
    fallback_url = getattr(package_module, 'PACKAGE_DATA_URL', {})
    self._InstallData(preprovisioned_data, package_name, filenames,
                      install_path, fallback_url)

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

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
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


VirtualMachine = typing.TypeVar('VirtualMachine', bound=BaseVirtualMachine)
