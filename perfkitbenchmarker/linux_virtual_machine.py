# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
import os
import pipes
import posixpath
import re
import threading
import time
import uuid

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

EPEL6_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '6/x86_64/epel-release-6-8.noarch.rpm')
EPEL7_RPM = ('http://dl.fedoraproject.org/pub/epel/'
             '7/x86_64/e/epel-release-7-5.noarch.rpm')

UPDATE_RETRIES = 5
SSH_RETRIES = 10
DEFAULT_SSH_PORT = 22
REMOTE_KEY_PATH = '.ssh/id_rsa'
CONTAINER_MOUNT_DIR = '/mnt'
CONTAINER_WORK_DIR = '/root'

BACKGROUND_IPERF_PORT = 20001
BACKGROUND_IPERF_SECONDS = 2147483647

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

flags.DEFINE_bool('setup_remote_firewall', False,
                  'Whether PKB should configure the firewall of each remote'
                  'VM to make sure it accepts all internal connections.')


class BaseLinuxMixin(virtual_machine.BaseOsMixin):
  """Class that holds Linux related VM methods and attributes."""

  # If multiple ssh calls are made in parallel using -t it will mess
  # the stty settings up and the terminal will become very hard to use.
  # Serializing calls to ssh with the -t option fixes the problem.
  _pseudo_tty_lock = threading.Lock()

  def __init__(self):
    super(BaseLinuxMixin, self).__init__()
    self.ssh_port = DEFAULT_SSH_PORT
    self.remote_access_ports = [self.ssh_port]
    self.has_private_key = False

    self._remote_command_script_upload_lock = threading.Lock()
    self._has_remote_command_script = False

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

  def RobustRemoteCommand(self, command, should_log=False):
    """Runs a command on the VM in a more robust way than RemoteCommand.

    Executes a command via a pair of scripts on the VM:

    * EXECUTE_COMMAND, which runs 'command' in a nohupped background process.
    * WAIT_FOR_COMMAND, which waits on a file lock held by EXECUTE_COMMAND until
      'command' completes, then returns with the stdout, stderr, and exit status
      of 'command'.

    Temporary SSH failures (where ssh returns a 255) while waiting for the
    command to complete will be tolerated and safely retried.

    If should_log is True, log the command's output at the info
    level. If False, log the command's output at the debug level.
    """
    self._PushRobustCommandScripts()

    execute_path = os.path.join(vm_util.VM_TMP_DIR,
                                os.path.basename(EXECUTE_COMMAND))
    wait_path = os.path.join(vm_util.VM_TMP_DIR,
                             os.path.basename(WAIT_FOR_COMMAND))

    uid = uuid.uuid4()
    file_base = os.path.join(vm_util.VM_TMP_DIR, 'cmd%s' % uid)
    wrapper_log = file_base + '.log'
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

    start_command = '%s 1> %s 2>&1 &' % (' '.join(start_command),
                                         wrapper_log)
    self.RemoteCommand(start_command)

    wait_command = ['python', wait_path, '--stdout', stdout_file,
                    '--stderr', stderr_file,
                    '--status', status_file,
                    '--delete']
    try:
      return self.RemoteCommand(' '.join(wait_command), should_log=should_log)
    except:
      # In case the error was with the wrapper script itself, print the log.
      stdout, _ = self.RemoteCommand('cat %s' % wrapper_log, should_log=False)
      if stdout.strip():
        logging.warn('Exception during RobustRemoteCommand. '
                     'Wrapper script log:\n%s', stdout)
      raise

  def SetupRemoteFirewall(self):
    """Sets up IP table configurations on the VM."""
    self.RemoteHostCommand('sudo iptables -A INPUT -j ACCEPT')
    self.RemoteHostCommand('sudo iptables -A OUTPUT -j ACCEPT')

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""
    env_file = "/etc/environment"
    commands = []

    if FLAGS.http_proxy:
      commands.append("echo 'http_proxy=%s' | sudo tee -a %s" % (
          FLAGS.http_proxy, env_file))

    if FLAGS.https_proxy:
      commands.append("echo 'https_proxy=%s' | sudo tee -a %s" % (
          FLAGS.https_proxy, env_file))

    if FLAGS.ftp_proxy:
      commands.append("echo 'ftp_proxy=%s' | sudo tee -a %s" % (
          FLAGS.ftp_proxy, env_file))

    if commands:
      self.RemoteCommand(";".join(commands))

  def SetupPackageManager(self):
    """Specific Linux flavors should override this."""
    pass

  def PrepareVMEnvironment(self):
    self.SetupProxy()
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)
    if FLAGS.setup_remote_firewall:
      self.SetupRemoteFirewall()
    if self.install_packages:
      if self.is_static:
        self.SnapshotPackages()
      self.SetupPackageManager()
    self.BurnCpu()

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    resp, _ = self.RemoteHostCommand('hostname', retries=1,
                                     suppress_warning=True)
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
    # Some images may automount one local disk, but we don't
    # want to fail if this wasn't the case.
    fmt_cmd = ('[[ -d /mnt ]] && sudo umount /mnt; '
               'sudo mke2fs -F -E lazy_itable_init=0,discard -O '
               '^has_journal -t ext4 -b 4096 %s' % device_path)
    self.RemoteHostCommand(fmt_cmd)

  def MountDisk(self, device_path, mount_path):
    """Mounts a formatted disk in the VM."""
    mnt_cmd = ('sudo mkdir -p {1};sudo mount -o discard {0} {1};'
               'sudo chown -R $USER:$USER {1};').format(device_path, mount_path)
    self.RemoteHostCommand(mnt_cmd)

  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    self.RemoteHostCopy(file_path, remote_path, copy_to)

  def RemoteHostCopy(self, file_path, remote_path='', copy_to=True):
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

  def RemoteCommand(self, command,
                    should_log=False, retries=SSH_RETRIES,
                    ignore_failure=False, login_shell=False,
                    suppress_warning=False, timeout=None):
    return self.RemoteHostCommand(command, should_log, retries,
                                  ignore_failure, login_shell,
                                  suppress_warning, timeout)

  def RemoteHostCommand(self, command,
                        should_log=False, retries=SSH_RETRIES,
                        ignore_failure=False, login_shell=False,
                        suppress_warning=False, timeout=None):
    """Runs a command on the VM.

    This is guaranteed to run on the host VM, whereas RemoteCommand might run
    within i.e. a container in the host VM.

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
        self._pseudo_tty_lock.acquire()
      else:
        ssh_cmd.append(command)

      for _ in range(retries):
        stdout, stderr, retcode = vm_util.IssueCommand(
            ssh_cmd, force_info_log=should_log,
            suppress_warning=suppress_warning,
            timeout=timeout)
        if retcode != 255:  # Retry on 255 because this indicates an SSH failure
          break
    finally:
      if login_shell:
        self._pseudo_tty_lock.release()

    if retcode:
      full_cmd = ' '.join(ssh_cmd)
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'Full command: %s\nSTDOUT: %sSTDERR: %s' %
                    (retcode, command, full_cmd, stdout, stderr))
      if not ignore_failure:
        raise errors.VirtualMachine.RemoteCommandError(error_text)

    return stdout, stderr

  def MoveFile(self, target, source_path, remote_path=''):
    self.MoveHostFile(target, source_path, remote_path)

  def MoveHostFile(self, target, source_path, remote_path=''):
    """Copies a file from one VM to a target VM.

    Args:
      target: The target BaseVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default
          is the home directory.
    """
    if not self.has_private_key:
      self.RemoteHostCopy(target.ssh_private_key, REMOTE_KEY_PATH)
      self.has_private_key = True

    # TODO(user): For security we may want to include
    #     -o UserKnownHostsFile=/dev/null in the scp command
    #     however for the moment, this has happy side effects
    #     ie: the key is added to know known_hosts which allows
    #     OpenMPI to operate correctly.
    remote_location = '%s@%s:%s' % (
        target.user_name, target.ip_address, remote_path)
    self.RemoteHostCommand('scp -P %s -o StrictHostKeyChecking=no -i %s %s %s' %
                           (target.ssh_port, REMOTE_KEY_PATH, source_path,
                            remote_location))

  def AuthenticateVm(self):
    """Authenticate a remote machine to access all peers."""
    self.RemoteHostCopy(vm_util.GetPrivateKeyPath(),
                        REMOTE_KEY_PATH)
    self.has_private_key = True

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

  def _GetTotalMemoryKb(self):
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
      disk_spec.device_path = '/dev/md%d' % len(self.scratch_disks)
      data_disk = disk.StripedDisk(disk_spec, disks)
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
    self.RemoteHostCommand(stripe_cmd)

  def BurnCpu(self, burn_cpu_threads=None, burn_cpu_seconds=None):
    """Burns vm cpu for some amount of time and dirty cache.

    Args:
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

  def PrepareBackgroundWorkload(self):
    """Install packages needed for the background workload """
    if self.background_cpu_threads:
      self.Install('sysbench')
    if self.background_network_mbits_per_sec:
      self.Install('iperf')

  def StartBackgroundWorkload(self):
    """Starts the blackground workload."""
    if self.background_cpu_threads:
      self.RemoteCommand(
          'nohup sysbench --num-threads=%s --test=cpu --cpu-max-prime=10000000 '
          'run 1> /dev/null 2> /dev/null &' % self.background_cpu_threads)
    if self.background_network_mbits_per_sec:
      self.AllowPort(BACKGROUND_IPERF_PORT)
      self.RemoteCommand('nohup iperf --server --port %s &> /dev/null &' %
                         BACKGROUND_IPERF_PORT)
      stdout, _ = self.RemoteCommand('pgrep iperf -n')
      self.server_pid = stdout.strip()

      if self.background_network_ip_type == vm_util.IpAddressSubset.EXTERNAL:
        ip_address = self.ip_address
      else:
        ip_address = self.internal_ip
      iperf_cmd = ('nohup iperf --client %s --port %s --time %s -u -b %sM '
                   '&> /dev/null &' % (ip_address, BACKGROUND_IPERF_PORT,
                                       BACKGROUND_IPERF_SECONDS,
                                       self.background_network_mbits_per_sec))

      self.RemoteCommand(iperf_cmd)
      stdout, _ = self.RemoteCommand('pgrep iperf -n')
      self.client_pid = stdout.strip()

  def StopBackgroundWorkload(self):
    """Stops the background workload."""
    if self.background_cpu_threads:
      self.RemoteCommand('pkill -9 sysbench')
    if self.background_network_mbits_per_sec:
      self.RemoteCommand('kill -9 ' + self.client_pid)
      self.RemoteCommand('kill -9 ' + self.server_pid)


class RhelMixin(BaseLinuxMixin):
  """Class holding RHEL specific VM methods and attributes."""

  OS_TYPE = os_types.RHEL

  def OnStartup(self):
    """Eliminates the need to have a tty to run sudo commands."""
    self.RemoteHostCommand('echo \'Defaults:%s !requiretty\' | '
                           'sudo tee /etc/sudoers.d/pkb' % self.user_name,
                           login_shell=True)


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
    if not self.install_packages:
      return
    if package_name not in self._installed_packages:
      package = linux_packages.PACKAGES[package_name]
      package.YumInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = linux_packages.PACKAGES[package_name]
    if hasattr(package, 'YumUninstall'):
      package.YumUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = linux_packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = linux_packages.PACKAGES[package_name]
    return package.YumGetServiceName(self)

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""
    super(RhelMixin, self).SetupProxy()
    yum_proxy_file = "/etc/yum.conf"

    if FLAGS.http_proxy:
      self.RemoteCommand("echo -e 'proxy= \"%s\";' | sudo tee -a %s" % (
          FLAGS.http_proxy, yum_proxy_file))


class DebianMixin(BaseLinuxMixin):
  """Class holding Debian specific VM methods and attributes."""

  OS_TYPE = os_types.DEBIAN

  def __init__(self, *args, **kwargs):
    super(DebianMixin, self).__init__(*args, **kwargs)

    # Whether or not apt-get update has been called.
    # We defer running apt-get update until the first request to install a
    # package.
    self._apt_updated = False

  @vm_util.Retry(max_retries=UPDATE_RETRIES)
  def AptUpdate(self):
    """Updates the package lists on VMs using apt."""
    try:
      # setting the timeout on the apt-get to 5 minutes because
      # it is known to get stuck.  In a normal update this
      # takes less than 30 seconds.
      self.RemoteCommand('sudo apt-get update', timeout=300)
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
    if not self.install_packages:
      return

    if not self._apt_updated:
      self.AptUpdate()
      self._apt_updated = True

    if package_name not in self._installed_packages:
      package = linux_packages.PACKAGES[package_name]
      package.AptInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = linux_packages.PACKAGES[package_name]
    if hasattr(package, 'AptUninstall'):
      package.AptUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = linux_packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = linux_packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)

  def SetupProxy(self):
    """Sets up proxy configuration variables for the cloud environment."""
    super(DebianMixin, self).SetupProxy()
    apt_proxy_file = "/etc/apt/apt.conf"
    commands = []

    if FLAGS.http_proxy:
      commands.append("echo -e 'Acquire::http::proxy \"%s\";' |"
                      'sudo tee -a %s' % (FLAGS.http_proxy, apt_proxy_file))

    if FLAGS.https_proxy:
      commands.append("echo -e 'Acquire::https::proxy \"%s\";' |"
                      'sudo tee -a %s' % (FLAGS.https_proxy, apt_proxy_file))

    if commands:
      self.RemoteCommand(";".join(commands))


class ContainerizedDebianMixin(DebianMixin):
  """Class representing a Containerized Virtual Machine.

  A Containerized Virtual Machine is a VM that runs remote commands
  within a Docker Container.
  Any call to RemoteCommand() will be run within the container
  whereas any call to RemoteHostCommand() will be run in the VM itself.
  """

  OS_TYPE = os_types.UBUNTU_CONTAINER

  def _CheckDockerExists(self):
    """Returns whether docker is installed or not."""
    resp, _ = self.RemoteHostCommand('command -v docker', ignore_failure=True,
                                     suppress_warning=True)
    if resp.rstrip() == "":
      return False
    return True

  def PrepareVMEnvironment(self):
    """Initializes docker before proceeding with preparation."""
    self.RemoteHostCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)
    if not self._CheckDockerExists():
      self.Install('docker')
    self.InitDocker()

    # Python is needed for RobustRemoteCommands
    self.Install('python')
    super(ContainerizedDebianMixin, self).PrepareVMEnvironment()

  def InitDocker(self):
    """Initializes the docker container daemon."""
    self.CONTAINER_IMAGE = 'ubuntu:latest'
    init_docker_cmd = ['sudo docker run -d '
                       '--net=host '
                       '--workdir=%s '
                       '-v %s:%s ' % (CONTAINER_WORK_DIR,
                                      vm_util.VM_TMP_DIR,
                                      CONTAINER_MOUNT_DIR)]
    for sd in self.scratch_disks:
      init_docker_cmd.append('-v %s:%s ' % (sd.mount_point, sd.mount_point))
    init_docker_cmd.append('%s sleep infinity ' % self.CONTAINER_IMAGE)
    init_docker_cmd = ''.join(init_docker_cmd)

    resp, _ = self.RemoteHostCommand(init_docker_cmd)
    self.docker_id = resp.rstrip()
    return self.docker_id

  def RemoteCommand(self, command,
                    should_log=False, retries=SSH_RETRIES,
                    ignore_failure=False, login_shell=False,
                    suppress_warning=False, timeout=None):
    """Runs a command inside the container.

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
    """
    # Escapes bash sequences
    command = command.replace("'", r"'\''")

    logging.info('Docker running: %s' % command)
    command = "sudo docker exec %s bash -c '%s'" % (self.docker_id, command)
    return self.RemoteHostCommand(command, should_log, retries,
                                  ignore_failure, login_shell, suppress_warning)

  def ContainerCopy(self, file_name, container_path='', copy_to=True):
    """Copies a file to and from container_path to the host's vm_util.VM_TMP_DIR.

    Args:
      file_name: Name of the file in the host's vm_util.VM_TMP_DIR.
      container_path: Optional path of where to copy file on container.
      copy_to: True to copy to container, False to copy from container.
    Raises:
      RemoteExceptionError: If the source container_path is blank.
    """
    if copy_to:
      if container_path == '':
        container_path = CONTAINER_WORK_DIR

      # Everything in vm_util.VM_TMP_DIR is directly accessible
      # both in the host and in the container
      source_path = posixpath.join(CONTAINER_MOUNT_DIR, file_name)
      command = 'cp %s %s' % (source_path, container_path)
      self.RemoteCommand(command)
    else:
      if container_path == '':
        raise errors.VirtualMachine.RemoteExceptionError('Cannot copy '
                                                         'from blank target')
      destination_path = posixpath.join(CONTAINER_MOUNT_DIR, file_name)
      command = 'cp %s %s' % (container_path, destination_path)
      self.RemoteCommand(command)

  @vm_util.Retry(
      poll_interval=1, max_retries=3,
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,))
  def RemoteCopy(self, file_path, remote_path='', copy_to=True):
    """Copies a file to or from the container in the remote VM.

    Args:
      file_path: Local path to file.
      remote_path: Optional path of where to copy file inside the container.
      copy_to: True to copy to VM, False to copy from VM.
    """
    if copy_to:
      file_name = os.path.basename(file_path)
      tmp_path = posixpath.join(vm_util.VM_TMP_DIR, file_name)
      self.RemoteHostCopy(file_path, tmp_path, copy_to)
      self.ContainerCopy(file_name, remote_path, copy_to)
    else:
      file_name = posixpath.basename(remote_path)
      tmp_path = posixpath.join(vm_util.VM_TMP_DIR, file_name)
      self.ContainerCopy(file_name, remote_path, copy_to)
      self.RemoteHostCopy(file_path, tmp_path, copy_to)

  def MoveFile(self, target, source_path, remote_path=''):
    """Copies a file from one VM to a target VM.

    Copies a file from a container in the source VM to a container
    in the target VM.

    Args:
      target: The target ContainerizedVirtualMachine object.
      source_path: The location of the file on the REMOTE machine.
      remote_path: The destination of the file on the TARGET machine, default
          is the root directory.
    """
    file_name = posixpath.basename(source_path)

    # Copies the file to vm_util.VM_TMP_DIR in source
    self.ContainerCopy(file_name, source_path, copy_to=False)

    # Moves the file to vm_util.VM_TMP_DIR in target
    source_host_path = posixpath.join(vm_util.VM_TMP_DIR, file_name)
    target_host_dir = vm_util.VM_TMP_DIR
    self.MoveHostFile(target, source_host_path, target_host_dir)

    # Copies the file to its final destination in the container
    target.ContainerCopy(file_name, remote_path)
