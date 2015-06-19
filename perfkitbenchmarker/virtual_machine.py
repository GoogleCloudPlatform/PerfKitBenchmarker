# Copyright 2014 Google Inc. All rights reserved.
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

import os
import os.path
import pipes
import threading
import time
import uuid

import jinja2

from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
REMOTE_KEY_PATH = '.ssh/id_rsa'
DEFAULT_USERNAME = 'perfkit'
SSH_RETRIES = 10
DEFAULT_SSH_PORT = 22
STRIPED_DEVICE = '/dev/md0'
LOCAL_MOUNT_PATH = '/local'

# This pair of scripts used for executing long-running commands, which will be
# resilient in the face of SSH connection errors.
# EXECUTE_COMMAND runs a command, streaming stdout / stderr to a file, then
# writing the return code to a file. An exclusive lock is acquired on the return
# code file, so that other processes may wait for completion.
EXECUTE_COMMAND = 'execute_command.py'
# WAIT_FOR_COMMAND waits on the file lock created by EXECUTE_COMMAND,
# then copies the stdout and stderr, exiting with the status of the command run
# by EXECUTE_COMMAND.
WAIT_FOR_COMMAND = 'wait_for_command.py'


class BaseVirtualMachineSpec(object):
  """Storing various data about a single vm.

  Attributes:
    project: The provider-specific project to associate the VM with (e.g.
      artisanal-lightbulb-883).
    zone: The region / zone the in which to launch the VM.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    image: The disk image to boot from.
    network: A BaseNetwork instance. The VM will be launched within this
      network.
  """

  def __init__(self, project, zone, machine_type, image, network):
    self.project = project
    self.zone = zone
    self.machine_type = machine_type
    self.image = image
    self.network = network


class BaseVirtualMachine(resource.BaseResource):
  """Base class for Virtual Machines.

  Attributes:
    hostname: The VM hostname.
    image: The disk image used to boot.
    internal_ip: Internal IP address.
    ip: Public (external) IP address.
    machine_type: The provider-specific instance type (e.g. n1-standard-8).
    network: A BaseNetwork instance.
    project: The provider-specific project associated with the VM (e.g.
      artisanal-lightbulb-883).
    ssh_port: Port number to use for SSH and SCP commands
    ssh_public_key: Path to SSH public key file.
    ssh_private_key: Path to SSH private key file.
    total_memory_kb: The number of kilobytes of memory on the VM.
    user_name: Account name for login. the contents of 'ssh_public_key' should
      be in .ssh/authorized_keys for this user.
    zone: The region / zone the VM was launched in.
    num_scratch_disks: int. Number of attached scratch disks.
    disk_specs: list of BaseDiskSpec objects. Specifications for disks attached
      to the VM.
    scratch_disks: list of BaseDisk objects. Scratch disks attached to the VM.
    max_local_disks: The number of local disks on the VM that can be used as
      scratch disks or that can be striped together.
  """

  is_static = False

  # If multiple ssh calls are made in parallel using -t it will mess
  # the stty settings up and the terminal will become very hard to use.
  # Serializing calls to ssh with the -t option fixes the problem.
  pseudo_tty_lock = threading.Lock()

  _instance_counter_lock = threading.Lock()
  _instance_counter = 0

  def __init__(self, vm_spec):
    """Initialize BaseVirtualMachine class.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(BaseVirtualMachine, self).__init__()
    with self._instance_counter_lock:
      self.name = 'perfkit-%s-%d' % (FLAGS.run_uri, self._instance_counter)
      BaseVirtualMachine._instance_counter += 1
    self.bootable_time = None
    self.project = vm_spec.project
    self.zone = vm_spec.zone
    self.machine_type = vm_spec.machine_type
    self.image = vm_spec.image
    self.network = vm_spec.network
    self.ip_address = None
    self.internal_ip = None
    self.user_name = None
    self.ssh_port = DEFAULT_SSH_PORT
    self.ssh_public_key = None
    self.ssh_private_key = None
    self.has_private_key = False
    self.user_name = DEFAULT_USERNAME
    self.ssh_public_key = vm_util.GetPublicKeyPath()
    self.ssh_private_key = vm_util.GetPrivateKeyPath()
    self.num_scratch_disks = 0
    self.disk_specs = []
    self.scratch_disks = []
    self.hostname = None
    self.max_local_disks = 0

    # Cached values
    self._reachable = {}
    self._total_memory_kb = None
    self._num_cpus = None

    self._remote_command_script_upload_lock = threading.Lock()
    self._has_remote_command_script = False

  def __repr__(self):
    return '<BaseVirtualMachine [ip={0}, internal_ip={1}]>'.format(
        self.ip_address, self.internal_ip)

  def __str__(self):
    return self.ip_address

  def __getstate__(self):
    """Get state for pickling."""
    d = self.__dict__.copy()
    # Locks cannot be pickled, so we just drop it and create a new one in
    # __setstate__.
    del d['_remote_command_script_upload_lock']
    return d

  def __setstate__(self, state):
    """Restores state after unpickling."""
    self.__dict__ = state
    # Locks cannot be pickled, so we create a new one after deserialization.
    self._remote_command_script_upload_lock = threading.Lock()

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    pass

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
    if len(disks) > 1:
      # If the disk_spec called for a striped disk, create one.
      device_path = '/dev/md%d' % len(self.scratch_disks)
      data_disk = disk.StripedDisk(disk_spec, disks, device_path)
    else:
      data_disk = disks[0]

    self.scratch_disks.append(data_disk)

    if data_disk.disk_type != disk.LOCAL:
      data_disk.Create()
      data_disk.Attach(self)

    if data_disk.is_striped:
      device_paths = [d.GetDevicePath() for d in data_disk.disks]
      self.StripeDisks(device_paths, data_disk.GetDevicePath())

    if disk_spec.mount_point:
      self.FormatDisk(data_disk.GetDevicePath())
      self.MountDisk(data_disk.GetDevicePath(), disk_spec.mount_point)


  def DeleteScratchDisks(self):
    """Delete a VM's scratch disks."""
    for scratch_disk in self.scratch_disks:
      scratch_disk.Delete()

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    resp, _ = self.RemoteCommand('hostname', retries=1, suppress_warning=True)
    if self.bootable_time is None:
      self.bootable_time = time.time()
    if self.hostname is None:
      self.hostname = resp[:-1]

  @vm_util.Retry()
  def FormatDisk(self, device_path):
    """Formats a disk attached to the VM."""
    fmt_cmd = ('sudo mke2fs -F -E lazy_itable_init=0 -O '
               '^has_journal -t ext4 -b 4096 %s' % device_path)
    self.RemoteCommand(fmt_cmd)

  def MountDisk(self, device_path, mount_path):
    """Mounts a formatted disk in the VM."""
    mnt_cmd = ('sudo mkdir -p {1};sudo mount {0} {1};'
               'sudo chown -R $USER:$USER {1};').format(device_path, mount_path)
    self.RemoteCommand(mnt_cmd)

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
      SshConnectionError: If there was a problem copying the file.
    """
    with open(template_path) as fp:
      template_contents = fp.read()

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = environment.from_string(template_contents)
    prefix = 'pkb-' + os.path.basename(template_path)

    with vm_util.NamedTemporaryFile(prefix=prefix) as tf:
      tf.write(template.render(vm=self, **context))
      tf.close()
      self.RemoteCopy(tf.name, remote_path)

  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      SshConnectionError: If there was a problem copying the file.
    """
    if vm_util.RunningOnWindows():
      if ':' in file_path:
        # scp doesn't like colons in paths.
        file_path = file_path.split(':', 1)[1]
      # Replace the last instance of '\' with '/' to make scp happy.
      file_path = '/'.join(file_path.rsplit('\\', 1))

    remote_location = '%s@%s:%s' % (
        self.user_name, self.ip_address, remote_path)
    scp_cmd = ['scp', '-P', str(self.ssh_port), '-pr']
    scp_cmd.extend(vm_util.GetSshOptions(self.ssh_private_key))
    if copy_to:
      scp_cmd.extend([file_path, remote_location])
    else:
      scp_cmd.extend([remote_location, file_path])

    stdout, stderr, retcode = vm_util.IssueCommand(scp_cmd, timeout=None)

    if retcode:
      full_cmd = ' '.join(scp_cmd)
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, full_cmd, stdout, stderr))
      raise errors.VmUtil.SshConnectionError(error_text)

  def _PushRobustCommandScripts(self):
    """Pushes the scripts required by RobustRemoteCommand to this VM.

    If the scripts have already been placed on the VM, this is a noop.
    """
    with self._remote_command_script_upload_lock:
      if not self._has_remote_command_script:
        for f in (EXECUTE_COMMAND, WAIT_FOR_COMMAND):
          self.PushDataFile(f, os.path.join(vm_util.VM_TMP_DIR,
                                            os.path.basename(f)))
        self._has_remote_command_script = True

  def RobustRemoteCommand(self, command):
    """Runs a command on the VM in a more robust way than RemoteCommand.

    Executes a command via a pair of scripts on the VM:

    * EXECUTE_COMMAND, which runs 'command' in a nohupped background process.
    * WAIT_FOR_COMMAND, which waits on a file lock held by EXECUTE_COMMAND until
      'command' completes, then returns with the stdout, stderr, and exit status
      of 'command'.

    Temporary SSH failures (where ssh returns a 255) while waiting for the
    command to complete will be tolerated and safely retried.

    Args:
      command: A valid bash command.

    Returns:
      A tuple of stdout and stderr from running the command.
    """
    self._PushRobustCommandScripts()

    execute_path = os.path.join(vm_util.VM_TMP_DIR,
                                os.path.basename(EXECUTE_COMMAND))
    wait_path = os.path.join(vm_util.VM_TMP_DIR,
                             os.path.basename(WAIT_FOR_COMMAND))

    uid = uuid.uuid4()
    file_base = os.path.join(vm_util.VM_TMP_DIR, 'cmd%s' % uid)
    stdout_file = file_base + '.stdout'
    stderr_file = file_base + '.stderr'
    status_file = file_base + '.status'

    if not isinstance(command, basestring):
      command = ' '.join(command)

    start_command = ['nohup', 'python', execute_path,
                     '--stdout', stdout_file,
                     '--stderr', stderr_file,
                     '--status', status_file,
                     '--command', pipes.quote(command)]

    start_command = '%s 1> /dev/null 2> /dev/null &' % ' '.join(start_command)
    self.RemoteCommand(start_command)

    wait_command = ['python', wait_path, '--stdout', stdout_file,
                    '--stderr', stderr_file,
                    '--status', status_file,
                    '--delete']
    return self.RemoteCommand(' '.join(wait_command), should_log=False)

  def RemoteCommand(self, command,
                    should_log=False, retries=SSH_RETRIES,
                    ignore_failure=False, login_shell=False,
                    suppress_warning=False):
    """Runs a command on the VM.

    Args:
      command: A valid bash command.
      should_log: A boolean indicating whether the command result should be
          logged at the info level. Even if it is false, the results will
          still be logged at the debug level.
      retries: The maximum number of times RemoteCommand should retry SSHing
          when it receives a 255 return code.
      ignore_failure: Ignore any failure if set to true.
      login_shell: Run command in a login shell.
      suppress_warning: Suppress the result logging from IssueCommand when the
          return code is non-zero.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      SshConnectionError: If there was a problem establishing the connection.
    """
    if vm_util.RunningOnWindows():
      # Multi-line commands passed to ssh won't work on Windows unless the
      # newlines are escaped.
      command = command.replace('\n', '\\n')

    user_host = '%s@%s' % (self.user_name, self.ip_address)
    ssh_cmd = ['ssh', '-A', '-p', str(self.ssh_port), user_host]
    ssh_cmd.extend(vm_util.GetSshOptions(self.ssh_private_key))
    try:
      if login_shell:
        ssh_cmd.extend(['-t', '-t', 'bash -l -c "%s"' % command])
        self.pseudo_tty_lock.acquire()
      else:
        ssh_cmd.append(command)

      for _ in range(retries):
        stdout, stderr, retcode = vm_util.IssueCommand(
            ssh_cmd, force_info_log=should_log,
            suppress_warning=suppress_warning,
            timeout=None)
        if retcode != 255:  # Retry on 255 because this indicates an SSH failure
          break
    finally:
      if login_shell:
        self.pseudo_tty_lock.release()

    if retcode:
      full_cmd = ' '.join(ssh_cmd)
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'Full command: %s\nSTDOUT: %sSTDERR: %s' %
                    (retcode, command, full_cmd, stdout, stderr))
      if not ignore_failure:
        raise errors.VmUtil.SshConnectionError(error_text)

    return stdout, stderr

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

  def MoveFile(self, target, source_path, remote_path=''):
    """Copies a file from one VM to a target VM.

    Args:
      target: The target BaseVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default
          is the home directory.
    """
    if not self.has_private_key:
      self.PushFile(target.ssh_private_key, REMOTE_KEY_PATH)
      self.has_private_key = True

    # TODO(user): For security we may want to include
    #     -o UserKnownHostsFile=/dev/null in the scp command
    #     however for the moment, this has happy side effects
    #     ie: the key is added to know known_hosts which allows
    #     OpenMPI to operate correctly.
    remote_location = '%s@%s:%s' % (
        target.user_name, target.ip_address, remote_path)
    self.RemoteCommand('scp -o StrictHostKeyChecking=no -i %s %s %s' %
                       (REMOTE_KEY_PATH, source_path, remote_location))

  def AuthenticateVm(self):
    """Authenticate a remote machine to access all peers."""
    self.PushFile(vm_util.GetPrivateKeyPath(),
                  REMOTE_KEY_PATH)

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

  def CheckJavaVersion(self):
    """Check the version of java on remote machine.

    Returns:
      The version of Java installed on remote machine.
    """
    version, _ = self.RemoteCommand('java -version 2>&1 >/dev/null | '
                                    'grep version | '
                                    'awk \'{print $3}\'')
    return version[:-1]

  def RemoveFile(self, filename):
    """Deletes a file on a remote machine.

    Args:
      filename: Path to the the file to delete.
    """
    self.RemoteCommand('sudo rm -rf %s' % filename)

  def GetDeviceSizeFromPath(self, path):
    """Gets the size of the a drive that contains the path specified.

    Args:
      path: The function will return the amount of space on the file system
            that contains this file name.

    Returns:
      The size in 1K blocks of the file system containing the file.
    """
    df_command = "df -k -P %s | tail -n +2 | awk '{ print $2 }'" % path
    stdout, _ = self.RemoteCommand(df_command)
    return int(stdout)

  @property
  def total_memory_kb(self):
    """Gets the amount of memory on the VM.

    Returns:
      The number of kilobytes of memory on the VM.
    """
    if not self._total_memory_kb:
      meminfo_command = 'cat /proc/meminfo | grep MemTotal | awk \'{print $2}\''
      stdout, _ = self.RemoteCommand(meminfo_command)
      self._total_memory_kb = int(stdout)
    return self._total_memory_kb

  def DropCaches(self):
    """Drops the VM's caches."""
    drop_caches_command = 'sudo /sbin/sysctl vm.drop_caches=3'
    self.RemoteCommand(drop_caches_command)

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

  @property
  def num_cpus(self):
    """Gets the number of CPUs on the VM.

    Returns:
      The number of CPUs on the vm.
    """
    if self._num_cpus is None:
      stdout, _ = self.RemoteCommand(
          'cat /proc/cpuinfo | grep processor | wc -l')
      self._num_cpus = int(stdout)
    return self._num_cpus

  def TimeToBoot(self):
    """Gets the time it took to boot this VM.

    Returns:
      Boot time (in seconds).
    """
    assert self.bootable_time, 'VM must have booted to get boot time.'
    assert self.create_start_time, 'VM must be created to get boot time.'
    assert self.bootable_time >= self.create_start_time
    return self.bootable_time - self.create_start_time

  def IsReachable(self, target_vm):
    """Indicates whether the target VM can be reached from it's internal ip.

    Args:
      target_vm: The VM whose reachability is being tested.

    Returns:
      True if the internal ip address of the target VM can be reached, false
      otherwise.
    """
    if target_vm not in self._reachable and target_vm.internal_ip:
      try:
        self.RemoteCommand('ping -c 1 %s' % target_vm.internal_ip)
      except errors.VmUtil.SshConnectionError:
        self._reachable[target_vm] = False
      else:
        self._reachable[target_vm] = True
    return self._reachable[target_vm]

  def StripeDisks(self, devices, striped_device):
    """Raids disks together using mdadm.

    Args:
      devices: A list of device paths that should be striped together.
      striped_device: The path to the device that will be created.
    """
    self.Install('mdadm')
    stripe_cmd = ('yes | sudo mdadm --create %s --level=stripe --raid-devices='
                  '%s %s' % (striped_device, len(devices), ' '.join(devices)))
    self.RemoteCommand(stripe_cmd)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM."""
    return []

  def SetupLocalDisks(self):
    """Perform cloud specific setup on any local disks that exist."""
    pass

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
