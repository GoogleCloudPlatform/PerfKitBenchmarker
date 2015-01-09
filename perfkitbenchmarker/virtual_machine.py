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

import logging
import os.path
import tempfile
import time
import uuid

import jinja2

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
REMOTE_KEY_PATH = '.ssh/id_rsa'
DEFAULT_USERNAME = 'perfkit'
SSH_RETRIES = 10
STRIPED_DEVICE = '/dev/md0'
LOCAL_MOUNT_PATH = '/local'


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
  """

  is_static = False

  def __init__(self, vm_spec):
    """Initialize BaseVirtualMachine class.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(BaseVirtualMachine, self).__init__()
    self.create_time = None
    self.bootable_time = None
    self.project = vm_spec.project
    self.zone = vm_spec.zone
    self.machine_type = vm_spec.machine_type
    self.image = vm_spec.image
    self.network = vm_spec.network
    self.ip_address = None
    self.internal_ip = None
    self.user_name = None
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

    # Cached values
    self._reachable = {}
    self._total_memory_kb = None
    self._num_cpus = None
    self._installed_packages = set()

  def _Create(self):
    self.create_time = time.time()

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
      scratch_disk.Delete()

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    resp, _ = self.RemoteCommand('hostname', retries=1)
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

  def RenderTemplate(self, template_path, remote_path, context, remote_port=22):
    """Renders a local Jinja2 template and copies it to the remote host.

    The template will be provided variables defined in 'context', as well as a
    variable named 'vm' referencing this object.

    Args:
      template_path: string. Local path to jinja2 template.
      remote_path: string. Remote path for rendered file on the remote vm.
      context: dict. Variables to pass to the Jinja2 template during rendering.
      remote_port: SSH port on the VM.

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

    with tempfile.NamedTemporaryFile(prefix=prefix) as tf:
      tf.write(template.render(vm=self, **context))
      tf.flush()
      self.RemoteCopy(tf.name, remote_path, remote_port=remote_port)

  def RemoteCopy(self, file_path, remote_path='', copy_to=True, remote_port=22):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.
      remote_port: Ssh port on the VM.

    Raises:
      SshConnectionError: If there was a problem copying the file.
    """
    remote_location = '%s@%s:%s' % (
        self.user_name, self.ip_address, remote_path)
    scp_cmd = ['/usr/bin/scp', '-P', str(remote_port), '-pr']
    scp_cmd.extend(vm_util.GetSshOptions(self.ssh_private_key))
    if copy_to:
      scp_cmd.extend([file_path, remote_location])
    else:
      scp_cmd.extend([remote_location, file_path])

    stdout, stderr, retcode = vm_util.IssueCommand(scp_cmd)

    if retcode:
      full_cmd = ' '.join(scp_cmd)
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, full_cmd, stdout, stderr))
      raise errors.VmUtil.SshConnectionError(error_text)

  def LongRunningRemoteCommand(self, command, remote_port=22):
    """Runs a long running command on the VM in a robust way.

    Args:
      command: A valid bash command.
      remote_port: Ssh port on the VM.

    Returns:
      A tuple of stdout and stderr from running the command.
    """
    uid = uuid.uuid4()
    stdout_file = '/tmp/stdout%s' % uid
    stderr_file = '/tmp/stderr%s' % uid
    long_running_cmd = ('nohup %s 1> %s 2> %s &' %
                        (command, stdout_file, stderr_file))
    self.RemoteCommand(long_running_cmd, remote_port)
    get_pid_cmd = 'pgrep %s' % command.split()[0]
    pid, _ = self.RemoteCommand(get_pid_cmd, remote_port)
    pid = pid.strip()
    check_process_cmd = ('if ! ps -p %s >/dev/null; then echo "Stopped"; fi' %
                         pid)
    while True:
      stdout, _ = self.RemoteCommand(check_process_cmd, remote_port)
      if stdout.strip() == 'Stopped':
        break
      time.sleep(60)

    stdout, _ = self.RemoteCommand('cat %s' % stdout_file, remote_port)
    stderr, _ = self.RemoteCommand('cat %s' % stderr_file, remote_port)

    return stdout, stderr

  def RemoteCommand(self, command, remote_port=22,
                    should_log=False, retries=SSH_RETRIES,
                    ignore_failure=False, login_shell=False):
    """Runs a command on the VM.

    Args:
      command: A valid bash command.
      remote_port: Ssh port on the VM.
      should_log: A boolean indicating whether the command result should be
          logged at the info level. Even if it is false, the results will
          still be logged at the debug level.
      retries: The maximum number of times RemoteCommand should retry SSHing
          when it receives a 255 return code.
      ignore_failure: Ignore any failure if set to true.
      login_shell: Run command in a login shell.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      SshConnectionError: If there was a problem establishing the connection.
    """
    user_host = '%s@%s' % (self.user_name, self.ip_address)
    ssh_cmd = ['/usr/bin/ssh', '-A', '-p', str(remote_port), user_host]
    ssh_cmd.extend(vm_util.GetSshOptions(self.ssh_private_key))
    if login_shell:
      ssh_cmd.extend(['-t', 'bash -l -c "%s"' % command])
    else:
      ssh_cmd.append(command)

    for _ in range(retries):
      stdout, stderr, retcode = vm_util.IssueCommand(
          ssh_cmd, should_log=should_log)
      if retcode != 255:  # Retry on 255 because this indicates an SSH failure
        break

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

  def PushDataFile(self, data_file):
    """Upload a file in perfkitbenchmarker.data directory to the VM.

    Args:
      data_file: The filename of the file to upload.
    Raises:
      perfkitbenchmarker.data.ResourceNotFound: if 'data_file' does not exist.
    """
    file_path = data.ResourcePath(data_file)
    self.PushFile(file_path)

  def CheckJavaVersion(self):
    """Check the version of java on remote machine.

    Returns:
      The version of Java installed on remote machine.
    """
    version, _ = self.RemoteCommand('java -version 2>&1 >/dev/null | '
                                    'grep version | '
                                    'awk \'{print $3}\'')
    return version[:-1]

  def PrepareJava(self, tarball, expected_version):
    """Install Java on a remote machine.

    Args:
      tarball: The Java tarball on local machine to install.
      expected_version: The expected Java version after installiation.

    Raises:
      ValueError: This is used to alert benchmarks that wont work yet.
    """

    # TODO(user): 10/28/2014 - Make this work with other OSes
    #if FLAGS.guest_os not in [GUEST_OS_DEBIAN]:
    #  raise ValueError('JAVA is only supported on Debian based images.')

    self.InstallPackage('libjna-java')
    version = self.CheckJavaVersion()
    if version != '"%s"' % expected_version:
      self.PushDataFile(tarball)
      self.RemoteCommand('sudo rm -rf /usr/lib/jvm', ignore_failure=True)
      self.RemoteCommand('sudo mkdir /usr/lib/jvm', ignore_failure=True)
      self.RemoteCommand('sudo tar -xzmpf %s -C /usr/lib/jvm' % tarball)
      self.RemoteCommand(
          'sudo update-alternatives --install '
          '"/usr/bin/java" "java" '
          '"/usr/lib/jvm/jdk%s/bin/java" 1' % expected_version)
      self.RemoteCommand('sudo update-alternatives --set java '
                         '/usr/lib/jvm/jdk%s/bin/java' % expected_version)
      version = self.CheckJavaVersion()
      if version != '"%s"' % expected_version:
        logging.warning('Failed to update Java to version %s on vm %s. '
                        'Current Java version is %s. '
                        'This will likely fail.',
                        expected_version, self.hostname, version)

  def UninstallPackage(self, package_name, force=False):
    """Uninstalls a package on a remote machine.

    Args:
      package_name: A string containing space-delimited package names understood
          by debian APT.
      force: Remove the package even if it was not installed by this VM.
    """
    for package in package_name.split():
      if force or package_name in self._installed_packages:
        if FLAGS.guest_os in [GUEST_OS_DEBIAN]:
          uninstall_command = 'sudo apt-get -y --purge remove {0}'.format(
              package)
        elif FLAGS.guest_os in [GUEST_OS_CENTOS]:
          uninstall_command = 'sudo yum -y remove {0}'.format(package_name)
        self.RemoteCommand(uninstall_command)
      else:
        logging.info('Not removing pre-existing package {0}'.format(
            package))

  def PackageIsInstalled(self, package_name):
    """Indicates if a package is installed on the vm or not.

    Args:
      package_name: A package name which is understood by debian APT.
    Returns:
      True if the package is installed, false otherwise.
    """
    if FLAGS.guest_os in [GUEST_OS_DEBIAN]:
      cmd = 'dpkg -s %s 2> /dev/null | grep Status' % package_name
    elif FLAGS.guest_os in [GUEST_OS_CENTOS]:
      cmd = 'rpm -q  %s 2> /dev/null | grep -v not' % package_name
    ret = self.RemoteCommand(cmd, ignore_failure=True)

    if ret[0]:
      return True
    else:
      return False

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
            that conatins this file name.

    Returns:
      The size in 1K blocks of the file system containing the file.
    """
    df_command = 'df -k | grep %s | awk \'{ print $2 }\'' % (path)
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
      Boot time (in seconds), or None if the boot is incomplete.
    """
    if not self.bootable_time or not self.create_time:
      return None
    assert self.bootable_time >= self.create_time
    return self.bootable_time - self.create_time

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

  def StripeDrives(self, devices, striped_device):
    """Raids drives together using mdadm.

    Args:
      devices: A list of device paths that should be striped together.
      striped_device: The path to the device that will be created.
    """
    self.InstallPackage('mdadm')
    stripe_cmd = ('yes | sudo mdadm --create %s --level=stripe --raid-devices='
                  '%s %s' % (striped_device, len(devices), ' '.join(devices)))
    self.RemoteCommand(stripe_cmd)

  def GetLocalDrives(self):
    """Returns a list of local drives on the VM."""
    return []

  def SetupLocalDrives(self, mount_path=LOCAL_MOUNT_PATH):
    """Set up any local drives that exist.

    Gets all local drives, stripes them together (if possible), formats,
    and mounts them at 'mount_path'. If there are no local drives to set up,
    this method will return False. If this method does set up local drives,
    then it will return True.

    Args:
      mount_path: The path where the local drives should be mounted. If this
          is None, then the device won't be formatted or mounted.

    Returns:
      A boolean indicating whether the setup occured.
    """
    devices = self.GetLocalDrives()
    if not devices:
      return False

    if len(devices) > 1:
      device_path = STRIPED_DEVICE
      self.StripeDrives(devices, device_path)
    else:
      device_path = devices[0]

    if mount_path:
      self.FormatDisk(device_path)
      self.MountDisk(device_path, mount_path)
    return True
