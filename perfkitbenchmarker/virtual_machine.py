# Copyright 2015 Google Inc. All rights reserved.
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

from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
DEFAULT_USERNAME = 'perfkit'


class BaseVirtualMachineSpec(object):
  """Storing various data about a single vm.

  Attributes:
    project: The provider-specific project to associate the VM with (e.g.
      artisanal-lightbulb-883).
    zone: The region / zone the in which to launch the VM.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    image: The disk image to boot from.
  """

  def __init__(self, project, zone, machine_type, image):
    self.project = project
    self.zone = zone
    self.machine_type = machine_type
    self.image = image


class BaseVirtualMachine(resource.BaseResource):
  """Base class for Virtual Machines.

  This class holds VM methods and attributes relating to the VM as a cloud
  resource. For methods and attributes that interact with the VM's guest
  OS, see BaseOsMixin and its subclasses.

  Attributes:
    image: The disk image used to boot.
    internal_ip: Internal IP address.
    ip: Public (external) IP address.
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
  """

  is_static = False

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
    self.project = vm_spec.project
    self.zone = vm_spec.zone
    self.machine_type = vm_spec.machine_type
    self.image = vm_spec.image
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

  @classmethod
  def SetVmSpecDefaults(cls, vm_spec):
    """Updates the VM spec with cloud specific defaults.

    If any of the VM spec attributes haven't been set by the time this is
    called, this should set them. This method should not override any
    attributes which have been set.
    """
    pass

  def __repr__(self):
    return '<BaseVirtualMachine [ip={0}, internal_ip={1}]>'.format(
        self.ip_address, self.internal_ip)

  def __str__(self):
    return self.ip_address

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

  def GetLocalDisks(self):
    # TODO(ehankland) This method should be removed as soon as raw/unmounted
    # scratch disks are supported in a different way. Only the Aerospike
    # benchmark currently accesses disks using this method.
    """Returns a list of local disks on the VM."""
    return []

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
    """Copies a file to the VM.

    Args:
      source_path: The location of the file on the LOCAL machine.
      remote_path: The destination of the file on the REMOTE machine, default
          is the home directory.
    """
    self.RemoteCopy(source_path, remote_path)

  def PullFile(self, source_path, remote_path=''):
    """Copies a file from the VM.

    Args:
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the LOCAL machine, default
          is the home directory.
    """
    self.RemoteCopy(source_path, remote_path, copy_to=False)

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
  def total_memory_kb(self):
    """Gets the amount of memory on the VM.

    Returns:
      The number of kilobytes of memory on the VM.
    """
    if not self._total_memory_kb:
      self._total_memory_kb = self._GetTotalMemoryKb()
    return self._total_memory_kb

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
