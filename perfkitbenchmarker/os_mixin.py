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

"""OS Mixin class, which defines abstract properties of an OS.

"Mixin" because most real implementations will inherit from a child of both
BaseOSMixin & BaseVirtualMachine. eg CentOs7BasedAzureVirtualMachine inherits
from AzureVirtualMachine which inherits from BaseVirtualMachine & CentOs7Mixin
which inherits from BaseOSMixin. Implementation & details are somewhat
confusingly coupled with BaseVirtualMachine.
"""

import abc
import contextlib
import logging
import os.path
import socket
import time
from typing import Any, Dict, List, Tuple, Union
import uuid

from absl import flags
import jinja2
from perfkitbenchmarker import background_workload
from perfkitbenchmarker import command_interface
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS


# pylint: disable=g-missing-from-attributes,invalid-name
class BaseOsMixin(command_interface.CommandInterface, metaclass=abc.ABCMeta):
  """The base class for OS Mixin classes.

  This class holds VM methods and attributes relating to the VM's guest OS.
  For methods and attributes that relate to the VM as a cloud resource,
  see BaseVirtualMachine and its subclasses.

  Attributes:
    bootable_time: The time when the VM finished booting.
    hostname: The VM's hostname.
    remote_access_ports: A list of ports which must be opened on the firewall in
      order to access the VM.
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
  proxy_jump: str  # mixed from BaseVirtualMachine
  user_name: str  # mixed from BaseVirtualMachine
  disable_interrupt_moderation: str  # mixed from BaseVirtualMachine
  disable_rss: str  # mixed from BaseVirtualMachine
  num_disable_cpus: int  # mixed from BaseVirtualMachine
  ip_address: str  # mixed from BaseVirtualMachine
  internal_ip: str  # mixed from BaseVirtualMachine
  can_connect_via_internal_ip: bool  # mixed from BaseVirtualMachine
  boot_completion_ip_subset: bool  # mixed from BaseVirtualMachine

  @abc.abstractmethod
  def GetConnectionIp(self):
    """See BaseVirtualMachine."""

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._installed_packages = set()
    self.startup_script_output = None
    self.postrun_script_output = None
    self.bootable_time = None
    self.port_listening_time = None
    self.hostname = None
    self.proxy_jump = None

    # Ports that will be opened by benchmark_spec to permit access to the VM.
    self.remote_access_ports = []
    # Port to be used to see if VM is ready to receive a remote command.
    self.primary_remote_access_port = None

    # Cached values
    self._reachable = {}
    self._total_memory_kb = None
    self.num_cpus: int = None
    self._is_smt_enabled = None
    # Update to Json type if ever available:
    # https://github.com/python/typing/issues/182
    self.os_metadata: Dict[str, Union[str, int, list[str]]] = {}
    assert (
        type(self).BASE_OS_TYPE in os_types.BASE_OS_TYPES
    ), '%s is not in %s' % (type(self).BASE_OS_TYPE, os_types.BASE_OS_TYPES)

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

  def RunCommand(
      self,
      command: str | list[str],
      ignore_failure: bool = False,
      should_pre_log: bool = True,
      stack_level: int = 1,
      timeout: float | None = None,
      **kwargs: Any,
  ) -> tuple[str, str, int]:
    """Runs a command.

    Additional args can be supplied & are passed to lower level functions but
    aren't required.

    Args:
      command: A valid bash command in string form.
      ignore_failure: Ignore any failure if set to true.
      should_pre_log: Whether to print the command being run or not.
      stack_level: Number of stack frames to skip & get an "interesting" caller,
        for logging. 1 skips this function, 2 skips this & its caller, etc..
      timeout: The time to wait in seconds for the command before exiting. None
        means no timeout.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    raise NotImplementedError()

  def RemoteCommand(
      self,
      command: str,
      ignore_failure: bool = False,
      timeout: float | None = None,
      **kwargs,
  ) -> Tuple[str, str]:
    """Runs a command on the VM.

    Derived classes may add additional kwargs if necessary, but they should not
    be used outside of the class itself since they are non standard.

    Args:
      command: A valid bash command.
      ignore_failure: Ignore any failure if set to true.
      timeout: The time to wait in seconds for the command before exiting. None
        means no timeout.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    raise NotImplementedError()

  def RemoteCommandWithReturnCode(
      self, *args, **kwargs
  ) -> Tuple[str, str, int]:
    """Runs a command on the VM & returns error code.

    Args:
      *args: Generic arguments, lining up with RemoteCommand.
      **kwargs: Additional arguments, including anything additional from child
        implementations.

    Returns:
      A tuple of stdout, stderr, return_code from running the command.

    Raises:
      RemoteCommandError: If there was a problem establishing the connection.
    """
    raise NotImplementedError()

  def RobustRemoteCommand(
      self,
      command: str,
      timeout: float | None = None,
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
    kwargs = vm_util.IncrementStackLevel(**kwargs)
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
          "Trying to reboot a VM that isn't rebootable."
      )

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
    """Returns the time the VM was last rebooted as reported by the VM."""
    raise NotImplementedError()

  def OnStartup(self):
    """Performs any necessary setup on the VM specific to the OS.

    This will be called once immediately after the VM has booted.
    """
    events.on_vm_startup.send(vm=self)
    # Resets the cached SMT enabled status and number cpus value.
    self._is_smt_enabled = None
    self.num_cpus = None

  def RecordAdditionalMetadata(self):
    """After the VM has been prepared, store VM metadata."""
    # Skip metadata capture if the VM lacks a boot time, indicating that prior
    # VM connection attempts failed.
    if not self.bootable_time:
      return
    if self.num_cpus is None:
      self.num_cpus = self._GetNumCpus()

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

  def InstallPackages(self, packages: str) -> None:
    """Installs packages using the OS's package manager."""
    pass

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
      remote_path: The destination of the file on the REMOTE machine, default is
        the home directory.
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

  def WriteTemporaryFile(self, file_contents: str) -> str:
    """Writes a temporary file to the VM.

    Args:
      file_contents: The contents of the file.

    Returns:
      The full filename.
    """
    name = '/tmp/' + str(uuid.uuid4())[-8:]
    vm_util.CreateRemoteFile(self, file_contents, name)
    return name

  def PrepareResourcePath(self, resource_name, search_user_paths=True) -> str:
    """Prepares a resource from local loaders & returns path on machine.

    Loaders are searched in order until the resource is found.
    If no loader provides 'resource_name', an exception is thrown.

    If 'search_user_paths' is true, the directories specified by
    "--data_search_paths" are consulted before the default paths.

    Args:
      resource_name: string. Name of a resource.
      search_user_paths: boolean. Whether paths from "--data_search_paths"
        should be searched before the default paths.

    Returns:
      A path to the resource on the local or remote machine's filesystem.

    Raises:
      ResourceNotFound: When resource was not found.
    """
    local_path = data.ResourcePath(resource_name, search_user_paths)
    destination_path = '/tmp/' + resource_name
    self.PushFile(local_path, destination_path)
    return destination_path

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
      copy_cmd = ' '.join(['cp', home_file_path, remote_path])
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

    with vm_util.NamedTemporaryFile(
        prefix=prefix, dir=vm_util.GetTempDir(), delete=False, mode='w'
    ) as tf:
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

  def PrepareScratchDisk(self, scratch_disk, disk_spec):
    """Expose internal prepare scratch disk."""
    self._PrepareScratchDisk(scratch_disk, disk_spec)

  def _PrepareScratchDisk(self, scratch_disk, disk_spec):
    """Helper method to format and mount scratch disk.

    Args:
      scratch_disk: Scratch disk to be formatted and mounted.
      disk_spec: The BaseDiskSpec object corresponding to the disk.
    """
    raise NotImplementedError()

  def _SetNumCpus(self):
    """Sets the number of CPUs on the VM.

    Returns:
      The number of CPUs on the VM.
    """
    if self.num_cpus is None:
      self.num_cpus = self._GetNumCpus()
    return self.num_cpus

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
      report_only_physical_cpus: Whether to report only the physical (non-SMT)
        CPUs.  Default is to report all vCPUs.

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

  def RecoverChunkedPreprovisionedData(self, path, filename):
    """Recover chunked preprovisioned data."""
    raise NotImplementedError()

  def CheckPreprovisionedData(
      self, install_path, module_name, filename, expected_sha256
  ):
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
          '(not recommended).'
          % (module_name, filename, actual_sha256, expected_sha256)
      )

  def TestConnectRemoteAccessPort(self, port=None, socket_timeout=0.5):
    """Tries to connect to remote access port and throw if it fails.

    Args:
      port: Integer of the port to connect to. Defaults to the default remote
        connection port of the VM.
      socket_timeout: The number of seconds to wait on the socket before failing
        and retrying. If this is too low, the connection may never succeed. If
        this is too high it will add latency (because the current connection may
        fail after a time that a new connection would succeed). Defaults to
        500ms.
    """
    if not port:
      port = self.primary_remote_access_port
    # TODO(user): refactor to reuse sockets?
    with contextlib.closing(
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ) as sock:
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

  # TODO(pclay): Implement on Windows, make abstract and non Optional
  @property
  def cpu_arch(self) -> str | None:
    """The basic CPU architecture of the VM."""
    return None

  def GetCPUVersion(self) -> str | None:
    """Get the CPU version of the VM."""
    return None

  def GenerateAndCaptureLogs(self) -> list[str]:
    """Generates and captures logs from the VM."""
    return []


class DeprecatedOsMixin(BaseOsMixin):
  """Class that adds a deprecation log message to OsBasedVms."""

  # The time or version in which this OS input will be removed
  END_OF_LIFE = None

  # Optional alternative to use instead.
  ALTERNATIVE_OS = None

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    assert self.OS_TYPE
    assert self.END_OF_LIFE
    warning = "os_type '%s' is deprecated and will be removed after %s." % (
        self.OS_TYPE,
        self.END_OF_LIFE,
    )
    if self.ALTERNATIVE_OS:
      warning += " Use '%s' instead." % self.ALTERNATIVE_OS
    logging.warning(warning)
