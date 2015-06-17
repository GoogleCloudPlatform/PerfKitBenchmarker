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

"""Module containing mixin classes for linux virtual machines.

These classes allow installation on both Debian and RHEL based linuxes.
They also handle some intial setup (especially on RHEL based linuxes
since by default sudo commands without a tty don't work) and
can restore the VM to the state it was in before packages were
installed.

To install a package on a VM, just call vm.Install(package_name).
The package name is just the name of the package module (i.e. the
file name minus .py). The framework will take care of all cleanup
for you.
"""

import logging
import re
import threading
import time
import uuid

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import packages
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


EPEL6_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '6/x86_64/epel-release-6-8.noarch.rpm')
EPEL7_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '7/x86_64/e/epel-release-7-5.noarch.rpm')

UPDATE_RETRIES = 5
SSH_RETRIES = 10
DEFAULT_SSH_PORT = 22
REMOTE_KEY_PATH = '.ssh/id_rsa'

FLAGS = flags.FLAGS

flags.DEFINE_bool('install_packages', True,
                  'Override for determining whether packages should be '
                  'installed. If this is false, no packages will be installed '
                  'on any VMs. This option should probably only ever be used '
                  'if you have already created an image with all relevant '
                  'packages installed.')


class BaseLinuxMixin(virtual_machine.BaseOsMixin):
  """Class that holds Linux related VM methods and attributes."""

  # If multiple ssh calls are made in parallel using -t it will mess
  # the stty settings up and the terminal will become very hard to use.
  # Serializing calls to ssh with the -t option fixes the problem.
  pseudo_tty_lock = threading.Lock()

  def __init__(self):
    super(BaseLinuxMixin, self).__init__()
    self.ssh_port = DEFAULT_SSH_PORT

  def Startup(self):
    if self.is_static and self.install_packages:
      self.SnapshotPackages()
    self.BurnCpu()

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    resp, _ = self.RemoteCommand('hostname', retries=1, suppress_warning=True)
    if self.bootable_time is None:
      self.bootable_time = time.time()
    if self.hostname is None:
      self.hostname = resp[:-1]

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    pass

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    pass

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the temp directory, restores packages, and uninstalls all
    PerfKit packages.
    """
    for package_name in self._installed_packages:
      self.Uninstall(package_name)
    self.RestorePackages()
    self.RemoteCommand('rm -rf %s' % vm_util.VM_TMP_DIR)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    pass

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    pass

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

  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
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
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  def LongRunningRemoteCommand(self, command):
    """Runs a long running command on the VM in a robust way.

    Args:
      command: A valid bash command.

    Returns:
      A tuple of stdout and stderr from running the command.
    """
    uid = uuid.uuid4()
    stdout_file = '/tmp/stdout%s' % uid
    stderr_file = '/tmp/stderr%s' % uid
    long_running_cmd = ('nohup %s 1> %s 2> %s &' %
                        (command, stdout_file, stderr_file))
    self.RemoteCommand(long_running_cmd)
    get_pid_cmd = 'pgrep %s' % command.split()[0]
    pid, _ = self.RemoteCommand(get_pid_cmd)
    pid = pid.strip()
    check_process_cmd = ('if ! ps -p %s >/dev/null; then echo "Stopped"; fi' %
                         pid)
    while True:
      stdout, _ = self.RemoteCommand(check_process_cmd)
      if stdout.strip() == 'Stopped':
        break
      time.sleep(60)

    stdout, _ = self.RemoteCommand('cat %s' % stdout_file)
    stderr, _ = self.RemoteCommand('cat %s' % stderr_file)

    return stdout, stderr

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
      RemoteCommandError: If there was a problem establishing the connection.
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
        raise errors.VirtualMachine.RemoteCommandError(error_text)

    return stdout, stderr

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

  def DropCaches(self):
    """Drops the VM's caches."""
    drop_caches_command = 'sudo /sbin/sysctl vm.drop_caches=3'
    self.RemoteCommand(drop_caches_command)

  def _GetNumCpus(self):
    """Returns the number of logical CPUs on the VM.

    This method does not cache results (unlike "num_cpus").
    """
    stdout, _ = self.RemoteCommand(
        'cat /proc/cpuinfo | grep processor | wc -l')
    return int(stdout)

  def _TotalMemoryKb(self):
    """Returns the amount of physical memory on the VM in Kilobytes.

    This method does not cache results (unlike "total_memory_kb").
    """
    meminfo_command = 'cat /proc/meminfo | grep MemTotal | awk \'{print $2}\''
    stdout, _ = self.RemoteCommand(meminfo_command)
    return int(stdout)

  def _TestReachable(self, ip):
    """Returns True if the VM can reach the ip address and False otherwise."""
    try:
      self.RemoteCommand('ping -c 1 %s' % ip)
    except errors.VirtualMachine.RemoteCommandError:
      return False
    return True

  def SetupLocalDisks(self):
    """Performs Linux specific setup of local disks."""
    # Some images may automount one local disk, but we don't
    # want to fail if this wasn't the case.
    self.RemoteCommand('sudo umount /mnt', ignore_failure=True)

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

  def BurnCpu(self, burn_cpu_threads=None, burn_cpu_seconds=None):
    """Burns vm cpu for some amount of time and dirty cache.

    Args:
      vm: The target vm.
      burn_cpu_threads: Number of threads to burn cpu.
      burn_cpu_seconds: Amount of time in seconds to burn cpu.
    """
    burn_cpu_threads = burn_cpu_threads or FLAGS.burn_cpu_threads
    burn_cpu_seconds = burn_cpu_seconds or FLAGS.burn_cpu_seconds
    if burn_cpu_seconds:
      self.Install('sysbench')
      end_time = time.time() + burn_cpu_seconds
      self.RemoteCommand(
          'nohup sysbench --num-threads=%s --test=cpu --cpu-max-prime=10000000 '
          'run 1> /dev/null 2> /dev/null &' % burn_cpu_threads)
      if time.time() < end_time:
        time.sleep(end_time - time.time())
      self.RemoteCommand('pkill -9 sysbench')


class RhelMixin(BaseLinuxMixin):
  """Class holding RHEL specific VM methods and attributes."""

  def Startup(self):
    """Eliminates the need to have a tty to run sudo commands."""
    super(RhelMixin, self).Startup()
    self.RemoteCommand('echo \'Defaults:%s !requiretty\' | '
                       'sudo tee /etc/sudoers.d/pkb' % self.user_name,
                       login_shell=True)
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  def InstallEpelRepo(self):
    """Installs the Extra Packages for Enterprise Linux repository."""
    try:
      self.InstallPackages('epel-release')
    except errors.VirtualMachine.RemoteCommandError as e:
      stdout, _ = self.RemoteCommand('cat /etc/redhat-release')
      major_version = int(re.search('release ([0-9])', stdout).group(1))
      if major_version == 6:
        epel_rpm = EPEL6_RPM
      elif major_version == 7:
        epel_rpm = EPEL7_RPM
      else:
        raise e
      self.RemoteCommand('sudo rpm -ivh --force %s' % epel_rpm)

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Performs the normal package cleanup, then deletes the file
    added to the /etc/sudoers.d directory during startup.
    """
    super(RhelMixin, self).PackageCleanup()
    self.RemoteCommand('sudo rm /etc/sudoers.d/pkb')

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand('rpm -qa > %s/rpm_package_list' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand(
        'rpm -qa | grep --fixed-strings --line-regexp --invert-match --file '
        '%s/rpm_package_list | xargs --no-run-if-empty sudo rpm -e' %
        vm_util.VM_TMP_DIR,
        ignore_failure=True)

  def InstallPackages(self, packages):
    """Installs packages using the yum package manager."""
    self.RemoteCommand('sudo yum install -y %s' % packages)

  def InstallPackageGroup(self, package_group):
    """Installs a 'package group' using the yum package manager."""
    self.RemoteCommand('sudo yum groupinstall -y "%s"' % package_group)

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if ((self.is_static and not self.install_packages) or
        not FLAGS.install_packages):
      return
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.YumInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'YumUninstall'):
      package.YumUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetServiceName(self)


class DebianMixin(BaseLinuxMixin):
  """Class holding Debian specific VM methods and attributes."""

  def Startup(self):
    """Runs apt-get update so InstallPackages shouldn't need to."""
    super(DebianMixin, self).Startup()
    self.AptUpdate()
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  @vm_util.Retry(max_retries=UPDATE_RETRIES)
  def AptUpdate(self):
    """Updates the package lists on VMs using apt."""
    try:
      self.RemoteCommand('sudo apt-get update')
    except errors.VirtualMachine.RemoteCommandError as e:
      # If there is a problem, remove the lists in order to get rid of
      # "Hash Sum mismatch" errors (the files will be restored when
      # apt-get update is run again).
      self.RemoteCommand('sudo rm -r /var/lib/apt/lists/*')
      raise e

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand(
        'dpkg --get-selections > %s/dpkg_selections' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand('sudo dpkg --clear-selections')
    self.RemoteCommand(
        'sudo dpkg --set-selections < %s/dpkg_selections' % vm_util.VM_TMP_DIR)
    self.RemoteCommand('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                       'apt-get --purge -y dselect-upgrade')

  @vm_util.Retry()
  def InstallPackages(self, packages):
    """Installs packages using the apt package manager."""
    try:
      install_command = ('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                         '/usr/bin/apt-get -y install %s' % (packages))
      self.RemoteCommand(install_command)
    except errors.VirtualMachine.RemoteCommandError as e:
      # TODO(user): Remove code below after Azure fix their package repository,
      # or add code to recover the sources.list
      self.RemoteCommand(
          'sudo sed -i.bk "s/azure.archive.ubuntu.com/archive.ubuntu.com/g" '
          '/etc/apt/sources.list')
      logging.info('Installing "%s" failed on %s. This may be transient. '
                   'Updating package list.', packages, self)
      self.AptUpdate()
      raise e

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if ((self.is_static and not self.install_packages) or
        not FLAGS.install_packages):
      return
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.AptInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'AptUninstall'):
      package.AptUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)
