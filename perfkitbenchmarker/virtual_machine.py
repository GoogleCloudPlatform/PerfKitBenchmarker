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
import os.path
import threading

import jinja2

from perfkitbenchmarker import background_workload
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS
DEFAULT_USERNAME = 'perfkit'

_VM_SPEC_REGISTRY = {}
_VM_REGISTRY = {}


flags.DEFINE_boolean(
    'dedicated_hosts', False,
    'If True, use hosts that only have VMs from the same '
    'benchmark running on them.')
flags.DEFINE_list('vm_metadata', [], 'Metadata to add to the vm '
                  'via the provider\'s AddMetadata function. It expects'
                  'key:value pairs')

VALID_GPU_TYPES = ['k80', 'p100']


def GetVmSpecClass(cloud):
  """Returns the VmSpec class corresponding to 'cloud'."""
  return _VM_SPEC_REGISTRY.get(cloud, BaseVmSpec)


def GetVmClass(cloud, os_type):
  """Returns the VM class corresponding to 'cloud' and 'os_type'."""
  return _VM_REGISTRY.get((cloud, os_type))


class AutoRegisterVmSpecMeta(spec.BaseSpecMetaClass):
  """Metaclass which allows VmSpecs to automatically be registered."""

  def __init__(cls, name, bases, dct):
    super(AutoRegisterVmSpecMeta, cls).__init__(name, bases, dct)
    if cls.CLOUD in _VM_SPEC_REGISTRY:
      raise Exception('BaseVmSpec subclasses must have a CLOUD attribute.')
    _VM_SPEC_REGISTRY[cls.CLOUD] = cls


class AutoRegisterVmMeta(abc.ABCMeta):
  """Metaclass which allows VMs to automatically be registered."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD') and hasattr(cls, 'OS_TYPE'):
      if cls.CLOUD is None:
        raise Exception('BaseVirtualMachine subclasses must have a CLOUD '
                        'attribute.')
      elif cls.OS_TYPE is None:
        raise Exception('BaseOsMixin subclasses must have an OS_TYPE '
                        'attribute.')
      else:
        _VM_REGISTRY[(cls.CLOUD, cls.OS_TYPE)] = cls
    super(AutoRegisterVmMeta, cls).__init__(name, bases, dct)


class BaseVmSpec(spec.BaseSpec):
  """Storing various data about a single vm.

  Attributes:
    zone: The region / zone the in which to launch the VM.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    gpu_count: None or int. Number of gpus to attach to the VM.
    gpu_type: None or string. Type of gpus to attach to the VM.
    image: The disk image to boot from.
    install_packages: If false, no packages will be installed. This is
        useful if benchmark dependencies have already been installed.
    background_cpu_threads: The number of threads of background CPU usage
        while running the benchmark.
    background_network_mbits_per_sec: The number of megabits per second of
        background network traffic during the benchmark.
    background_network_ip_type: The IP address type (INTERNAL or
        EXTERNAL) to use for generating background network workload.
  """

  __metaclass__ = AutoRegisterVmSpecMeta
  CLOUD = None

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
    if flag_values['gpu_type'].present:
      config_values['gpu_type'] = flag_values.gpu_type
    if flag_values['gpu_count'].present:
      config_values['gpu_count'] = flag_values.gpu_count

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
        'image': (option_decoders.StringDecoder, {'none_ok': True,
                                                  'default': None}),
        'install_packages': (option_decoders.BooleanDecoder, {'default': True}),
        'machine_type': (option_decoders.StringDecoder, {'none_ok': True,
                                                         'default': None}),
        'gpu_type': (option_decoders.EnumDecoder, {
            'valid_values': VALID_GPU_TYPES,
            'default': None}),
        'gpu_count': (option_decoders.IntDecoder, {'min': 1, 'default': None}),
        'zone': (option_decoders.StringDecoder, {'none_ok': True,
                                                 'default': None}),
        'use_dedicated_host': (option_decoders.BooleanDecoder,
                               {'default': False}),
        'background_network_mbits_per_sec': (option_decoders.IntDecoder, {
            'none_ok': True, 'default': None}),
        'background_network_ip_type': (option_decoders.EnumDecoder, {
            'default': vm_util.IpAddressSubset.EXTERNAL,
            'valid_values': [vm_util.IpAddressSubset.EXTERNAL,
                             vm_util.IpAddressSubset.INTERNAL]}),
        'background_cpu_threads': (option_decoders.IntDecoder, {
            'none_ok': True, 'default': None})})
    return result


class BaseVirtualMachine(resource.BaseResource):
  """Base class for Virtual Machines.

  This class holds VM methods and attributes relating to the VM as a cloud
  resource. For methods and attributes that interact with the VM's guest
  OS, see BaseOsMixin and its subclasses.

  Attributes:
    image: The disk image used to boot.
    internal_ip: Internal IP address.
    ip_address: Public (external) IP address.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    project: The provider-specific project associated with the VM (e.g.
      artisanal-lightbulb-883).
    ssh_public_key: Path to SSH public key file.
    ssh_private_key: Path to SSH private key file.
    user_name: Account name for login. the contents of 'ssh_public_key' should
      be in .ssh/authorized_keys for this user.
    zone: The region / zone the VM was launched in.
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
  """

  __metaclass__ = AutoRegisterVmMeta
  is_static = False
  CLOUD = None

  _instance_counter_lock = threading.Lock()
  _instance_counter = 0

  def __init__(self, vm_spec):
    """Initialize BaseVirtualMachine class.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(BaseVirtualMachine, self).__init__()
    with self._instance_counter_lock:
      self.instance_number = self._instance_counter
      self.name = 'pkb-%s-%d' % (FLAGS.run_uri, self.instance_number)
      BaseVirtualMachine._instance_counter += 1
    self.zone = vm_spec.zone
    self.machine_type = vm_spec.machine_type
    self.gpu_count = vm_spec.gpu_count
    self.gpu_type = vm_spec.gpu_type
    self.image = vm_spec.image
    self.install_packages = vm_spec.install_packages
    self.ip_address = None
    self.internal_ip = None
    self.user_name = DEFAULT_USERNAME
    self.password = None
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

    self.network = None
    self.firewall = None
    self.tcp_congestion_control = None
    self.numa_node_count = None
    self.num_disable_cpus = None

  def __repr__(self):
    return '<BaseVirtualMachine [ip={0}, internal_ip={1}]>'.format(
        self.ip_address, self.internal_ip)

  def __str__(self):
    if self.ip_address:
      return self.ip_address
    return super(BaseVirtualMachine, self).__str__()

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
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
    """Opens the ICMP protocol on the firewall corresponding to the VM if
    one exists.
    """
    if self.firewall:
      self.firewall.AllowIcmp(self)

  def AllowPort(self, start_port, end_port=None):
    """Opens the port on the firewall corresponding to the VM if one exists."""
    if self.firewall:
      if end_port:
          for port in range(start_port, end_port + 1):
              self.firewall.AllowPort(self, port)
      else:
          self.firewall.AllowPort(self, start_port)

  def AllowRemoteAccessPorts(self):
    """Allow all ports in self.remote_access_ports."""
    for port in self.remote_access_ports:
      self.AllowPort(port)

  def AddMetadata(self, **kwargs):
    """Add key/value metadata to the instance.

    Adds metadata in the form of key value pairs to the instance. Useful for
    debugging / introspection.

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
    result = self.metadata.copy()
    result.update({
        'image': self.image,
        'zone': self.zone,
        'cloud': self.CLOUD,
    })
    if self.machine_type is not None:
      result['machine_type'] = self.machine_type
    if self.use_dedicated_host is not None:
      result['dedicated_host'] = self.use_dedicated_host
    if self.tcp_congestion_control is not None:
      result['tcp_congestion_control'] = self.tcp_congestion_control
    if self.numa_node_count is not None:
      result['numa_node_count'] = self.numa_node_count
    if self.num_disable_cpus is not None:
      result['num_disable_cpus'] = self.num_disable_cpus

    return result

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    raise NotImplementedError()


class BaseOsMixin(object):
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

  __metaclass__ = abc.ABCMeta
  OS_TYPE = None

  def __init__(self):
    super(BaseOsMixin, self).__init__()
    self._installed_packages = set()

    self.bootable_time = None
    self.hostname = None

    # Ports that will be opened by benchmark_spec to permit access to the VM.
    self.remote_access_ports = []

    # Cached values
    self._reachable = {}
    self._total_memory_kb = None
    self._num_cpus = None

  @abc.abstractmethod
  def RemoteCommand(self, command, should_log=False, ignore_failure=False,
                    suppress_warning=False, timeout=None, **kwargs):
    """Runs a command on the VM.

    Derived classes may add additional kwargs if necessary, but they should not
    be used outside of the class itself since they are non standard.

    Args:
      command: A valid bash command.
      should_log: A boolean indicating whether the command result should be
          logged at the info level. Even if it is false, the results will
          still be logged at the debug level.
      ignore_failure: Ignore any failure if set to true.
      suppress_warning: Suppress the result logging from IssueCommand when the
          return code is non-zero.
      timeout is the time to wait in seconds for the command before exiting.
          None means no timeout.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    raise NotImplementedError()

  def TryRemoteCommand(self, command, **kwargs):
    """Runs a remote command and returns True iff it succeeded."""
    try:
      self.RemoteCommand(command, **kwargs)
      return True
    except errors.VirtualMachine.RemoteCommandError:
      return False
    except:
      raise

  def Reboot(self):
    """Reboot the VM."""

    vm_bootable_time = None

    # Use self.bootable_time to determine if this is the first boot.
    # On the first boot, WaitForBootCompletion will only run once.
    # On subsequent boots, need to WaitForBootCompletion and ensure
    # the last boot time changed.
    if self.bootable_time is not None:
      vm_bootable_time = self.VMLastBootTime()

    self._Reboot()

    while True:
      self.WaitForBootCompletion()
      # WaitForBootCompletion ensures that the machine is up
      # this is sufficient check for the first boot - but not for a reboot
      if vm_bootable_time != self.VMLastBootTime():
        break

    self._AfterReboot()

  @abc.abstractmethod
  def _Reboot(self):
    """OS-specific implementation of reboot command."""
    raise NotImplementedError()

  def _AfterReboot(self):
    """Performs any OS-specific setup on the VM following reboot.

    This will be called after every call to Reboot().
    """
    pass

  @abc.abstractmethod
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

  @abc.abstractmethod
  def WaitForBootCompletion(self):
    """Waits until VM is has booted.

    Implementations of this method should set the 'bootable_time' attribute
    and the 'hostname' attribute.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def VMLastBootTime(self):
    """Returns the UTC time the VM was last rebooted as reported by the VM.
    """
    raise NotImplementedError()

  def OnStartup(self):
    """Performs any necessary setup on the VM specific to the OS.

    This will be called once immediately after the VM has booted.
    """
    pass

  def PrepareVMEnvironment(self):
    """Performs any necessary setup on the VM specific to the OS.

    This will be called once after setting up scratch disks.
    """
    pass

  @abc.abstractmethod
  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    raise NotImplementedError()

  @abc.abstractmethod
  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    raise NotImplementedError()

  @abc.abstractmethod
  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the temp directory, restores packages, and uninstalls all
    PerfKit packages.
    """
    raise NotImplementedError()

  def SetupLocalDisks(self):
    """Perform OS specific setup on any local disks that exist."""
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

  def PushDataFile(self, data_file, remote_path=''):
    """Upload a file in perfkitbenchmarker.data directory to the VM.

    Args:
      data_file: The filename of the file to upload.
      remote_path: The destination for 'data_file' on the VM. If not specified,
        the file will be placed in the user's home directory.
    Raises:
      perfkitbenchmarker.data.ResourceNotFound: if 'data_file' does not exist.
    """
    file_path = data.ResourcePath(data_file)
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
                                    delete=False) as tf:
      tf.write(template.render(vm=self, **context))
      tf.close()
      self.RemoteCopy(tf.name, remote_path)

  @abc.abstractmethod
  def _CreateScratchDiskFromDisks(self, disk_spec, disks):
    """Helper method to prepare data disks.

    Given a list of BaseDisk objects, this will do most of the work creating,
    attaching, striping, formatting, and mounting them. If multiple BaseDisk
    objects are passed to this method, it will stripe them, combining them
    into one 'logical' data disk (it will be treated as a single disk from a
    benchmarks perspective). This is intended to be called from within a cloud
    specific VM's CreateScratchDisk method.

    Args:
      disk_spec: The BaseDiskSpec object corresponding to the disk.
      disks: A list of the disk(s) to be created, attached, striped,
          formatted, and mounted. If there is more than one disk in
          the list, then they will be striped together.
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
    return self._num_cpus

  @abc.abstractmethod
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

  @abc.abstractmethod
  def _GetTotalFreeMemoryKb(self):
    """Returns the amount of free physical memory on the VM in Kilobytes."""
    raise NotImplementedError()

  @abc.abstractmethod
  def _GetTotalMemoryKb(self):
    """Returns the amount of physical memory on the VM in Kilobytes.

    This method does not cache results (unlike "total_memory_kb").
    """
    raise NotImplementedError()

  def IsReachable(self, target_vm):
    """Indicates whether the target VM can be reached from it's internal ip.

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

  @abc.abstractmethod
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

  @abc.abstractmethod
  def SetReadAhead(self, num_sectors, devices):
    """Set read-ahead value for block devices.

    Args:
      num_sectors: int. Number of sectors of read ahead.
      devices: list of strings. A list of block devices.
    """
    raise NotImplementedError()
