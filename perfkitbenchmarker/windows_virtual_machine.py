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
"""Module containing mixin classes for Windows virtual machines."""

import base64
import logging
import ntpath
import os
import time
from typing import Tuple, cast
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import log_util
from perfkitbenchmarker import os_mixin
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_packages
import requests
import six
import timeout_decorator
import winrm


FLAGS = flags.FLAGS

flags.DEFINE_bool(
    'log_windows_password',
    False,
    'Whether to log passwords for Windows machines. This can be useful in '
    'the event of needing to manually RDP to the instance.',
)

flags.DEFINE_bool(
    'set_cpu_priority_high',
    False,
    'Allows executables to be set to High (up from Normal) CPU priority '
    'through the SetProcessPriorityToHigh function.',
)

flags.DEFINE_integer(
    'winrm_retries',
    3,
    'Default number of times to retry transient failures on WinRM commands.',
    lower_bound=1,
)

flags.DEFINE_integer(
    'winrm_retry_interval',
    10,
    'Default time to wait between retries on WinRM commands.',
    lower_bound=1,
)

# Windows disk letter starts from C, use a larger disk letter for attached disk
# to avoid conflict. On Azure, D is reserved for DvD drive.
ATTACHED_DISK_LETTER = 'F'
SMB_PORT = 445
WINRM_PORT = 5986
RDP_PORT = 3389

START_COMMAND_RETRIES = 10
# This startup script enables remote mangement of the instance. It does so
# by creating a WinRM listener (using a self-signed cert) and opening
# the WinRM port in the Windows firewall.
# This script also disables the windows defender to avoid
# interferring with the benchmarks
_STARTUP_SCRIPT = """
New-ItemProperty -Path "HKLM:\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Policies\\System" -Name 'EnableLUA' -Value 0 -PropertyType DWORD -Force
Set-MpPreference -DisableRealtimeMonitoring $true
Set-MpPreference -DisableBehaviorMonitoring $true
Set-MpPreference -DisableBlockAtFirstSeen $true
Set-ExecutionPolicy RemoteSigned
Enable-PSRemoting -Force
$cert = New-SelfSignedCertificate -DnsName hostname -CertStoreLocation `
    Cert:\\LocalMachine\\My\\
New-Item WSMan:\\localhost\\Listener -Transport HTTPS -Address * `
    -CertificateThumbPrint $cert.Thumbprint -Force
Set-Item -Path 'WSMan:\\localhost\\Service\\Auth\\Basic' -Value $true
netsh advfirewall firewall add rule name='Allow WinRM' dir=in action=allow `
    protocol=TCP localport={winrm_port}
""".format(winrm_port=WINRM_PORT)
STARTUP_SCRIPT = 'powershell -EncodedCommand {encoded_command}'.format(
    encoded_command=six.ensure_str(
        base64.b64encode(_STARTUP_SCRIPT.encode('utf-16-le'))
    )
)


class WaitTimeoutError(Exception):
  """Exception thrown if a wait operation takes too long."""


def winrm_retry(func):
  """A decorator that applies vm_util.Retry with WinRM-specific flag settings.

  This decorator is a "decorator factory" that, when applied to a method,
  returns a vm_util.Retry decorator configured with the *runtime* values
  of absl flags.

  Args:
    func: The function to decorate vm_util.Retry.

  Returns:
    A function that wraps functions in retry logic. It can be used as a
    decorator.
  """

  def wrapper(self, *args, **kwargs):

    retried_func = vm_util.Retry(
        log_errors=False,
        max_retries=FLAGS.winrm_retries,
        poll_interval=FLAGS.winrm_retry_interval,
        retryable_exceptions=(
            WaitTimeoutError,
            requests.exceptions.ConnectionError,
        ),
    )(func)

    try:
      return retried_func(self, *args, **kwargs)
    except requests.exceptions.ConnectionError:
      logging.warning(
          'ConnectionError detected. The WinRM service on the VM'
          ' may be down or restarting. Please check Windows Event Logs'
          ' on the VM for details.'
      )
      raise

  return wrapper


class BaseWindowsMixin(os_mixin.BaseOsMixin):
  """Class that holds Windows related VM methods and attributes."""

  OS_TYPE = os_types.WINDOWS
  BASE_OS_TYPE = os_types.WINDOWS

  def __init__(self):
    super().__init__()
    self.winrm_port = WINRM_PORT
    self.smb_port = SMB_PORT
    self.remote_access_ports = [self.winrm_port, self.smb_port, RDP_PORT]
    self.primary_remote_access_port = self.winrm_port
    self.rdp_port_listening_time = None
    self.temp_dir: str = None
    self.home_dir: str = None
    self.system_drive: str = None
    self.assigned_disk_letter = ATTACHED_DISK_LETTER

  def RobustRemoteCommand(
      self,
      command: str,
      ignore_failure: bool = False,
      timeout: float | None = None,
  ) -> Tuple[str, str]:
    """Runs a powershell command on the VM.

    We have attempted using Invoke-WmiMethod and other long running
    command and failed. Revert to just use RemoteCommand on windows.

    Args:
      command: A valid powershell command.
      ignore_failure: Ignore any failure if set to true.
      timeout: Float. A timeout in seconds for the command. If None is passed,
        no timeout is applied. Timeout kills the winrm session which then kills
        the process being executed.

    Returns:
      A tuple of stdout and stderr from running the command.
    """
    return self.RemoteCommand(command, ignore_failure, timeout)

  @winrm_retry
  def RemoteCommand(
      self,
      command: str,
      ignore_failure: bool = False,
      timeout: float | None = None,
  ) -> Tuple[str, str]:
    """Runs a powershell command on the VM.

    Args:
      command: A valid powershell command.
      ignore_failure: Ignore any failure if set to true.
      timeout: Float. A timeout in seconds for the command. If None is passed,
        no timeout is applied. Timeout kills the winrm session which then kills
        the process being executed.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command or the
          command timed out.
    """
    log_util.LogToShortLogAndRoot(
        f'Running command on {self}: {command}', stacklevel=2
    )
    if timeout is None:
      s = winrm.Session(
          'https://%s:%s' % (self.GetConnectionIp(), self.winrm_port),
          auth=(self.user_name, self.password),
          server_cert_validation='ignore',
      )
    else:
      s = winrm.Session(
          'https://%s:%s' % (self.GetConnectionIp(), self.winrm_port),
          auth=(self.user_name, self.password),
          server_cert_validation='ignore',
          read_timeout_sec=timeout + 10,
          operation_timeout_sec=timeout,
      )

    encoded_command = six.ensure_str(
        base64.b64encode(command.encode('utf_16_le'))
    )

    @timeout_decorator.timeout(
        timeout,
        use_signals=False,
        timeout_exception=WaitTimeoutError,
    )
    def run_command():
      return s.run_cmd('powershell -encodedcommand %s' % encoded_command)

    r = run_command()
    retcode, stdout, stderr = (
        r.status_code,
        six.ensure_str(r.std_out),
        six.ensure_str(r.std_err),
    )

    debug_text = 'Ran %s on %s. Return code (%s).\nSTDOUT: %s\nSTDERR: %s' % (
        command,
        self,
        retcode,
        stdout,
        stderr,
    )
    logging.info(debug_text, stacklevel=2)

    if retcode and not ignore_failure:
      error_text = (
          'Got non-zero return code (%s) executing %s\nSTDOUT: %sSTDERR: %s'
          % (retcode, command, stdout, stderr)
      )
      raise errors.VirtualMachine.RemoteCommandError(error_text)

    return stdout, stderr

  def PartitionDisk(self, dev_name, dev_path, num_partitions, partition_size):
    raise NotImplementedError()

  def IsDiskFormatted(self, dev_name, num_partitions):
    raise NotImplementedError()

  def hasStripedDiskDevice(self, dev_name: str) -> bool:
    raise NotImplementedError()

  def RemoteCopy(self, local_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      local_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    # Join with '' to get a trailing \
    remote_path = remote_path or ntpath.join(self.home_dir, '')
    # In order to expand "~" and "~user" we use ntpath.expanduser(),
    # but it relies on environment variables being set. This modifies
    # the USERPROFILE environment variable in order to use that function, and
    # then restores it to its previous value.
    home = os.environ.get('USERPROFILE')
    try:
      os.environ['USERPROFILE'] = self.home_dir
      remote_path = ntpath.expanduser(remote_path)
      # Some Windows path's like C:\Users\ADMINI~1 contain ~,
      # but they should not start with ~
      for dir_part in remote_path.split(ntpath.sep):
        assert not dir_part.startswith('~'), f'Failed to expand {remote_path}'
    finally:
      if home is None:
        del os.environ['USERPROFILE']
      else:
        os.environ['USERPROFILE'] = home
    drive, remote_path = ntpath.splitdrive(remote_path)
    remote_drive = (drive or self.system_drive).rstrip(':')
    network_drive = '\\\\%s\\%s$' % (self.GetConnectionIp(), remote_drive)

    if vm_util.RunningOnWindows():
      self._PsDriveRemoteCopy(local_path, remote_path, copy_to, network_drive)
    else:
      self._SmbclientRemoteCopy(local_path, remote_path, copy_to, network_drive)

  def _SmbclientRemoteCopy(
      self, local_path, remote_path, copy_to, network_drive
  ):
    """Copies a file to or from the VM using smbclient.

    Args:
      local_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.
      network_drive: The smb specification for the remote drive
        (//{ip_address}/{share_name}).

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    local_directory, local_file = os.path.split(local_path)
    remote_directory, remote_file = ntpath.split(remote_path)

    smb_command = 'cd %s; lcd %s; ' % (remote_directory, local_directory)
    if copy_to:
      smb_command += 'put %s %s' % (local_file, remote_file)
    else:
      smb_command += 'get %s %s' % (remote_file, local_file)
    smb_copy = [
        'smbclient',
        network_drive,
        '--max-protocol',
        'SMB3',
        '--user',
        '%s%%%s' % (self.user_name, self.password),
        '--port',
        str(self.smb_port),
        '--command',
        smb_command,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(
        smb_copy, raise_on_failure=False
    )
    if retcode:
      error_text = (
          'Got non-zero return code (%s) executing %s\nSTDOUT: %sSTDERR: %s'
          % (retcode, smb_copy, stdout, stderr)
      )
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  def _PsDriveRemoteCopy(self, local_path, remote_path, copy_to, network_drive):
    """Copies a file to or from the VM using New-PSDrive and Copy-Item.

    Args:
      local_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.
      network_drive: The smb specification for the remote drive
        (//{ip_address}/{share_name}).

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    set_error_pref = '$ErrorActionPreference="Stop"'

    password = self.password.replace("'", "''")
    create_cred = (
        "$pw = convertto-securestring -AsPlainText -Force '%s';"
        '$cred = new-object -typename System.Management.Automation'
        '.PSCredential -argumentlist %s,$pw' % (password, self.user_name)
    )

    psdrive_name = self.name
    create_psdrive = (
        'New-PSDrive -Name %s -PSProvider filesystem -Root %s -Credential $cred'
        % (psdrive_name, network_drive)
    )

    remote_path = '%s:%s' % (psdrive_name, remote_path)
    if copy_to:
      from_path, to_path = local_path, remote_path
    else:
      from_path, to_path = remote_path, local_path

    copy_item = 'Copy-Item -Path %s -Destination %s' % (from_path, to_path)

    delete_connection = 'net use %s /delete' % network_drive

    cmd = ';'.join([
        set_error_pref,
        create_cred,
        create_psdrive,
        copy_item,
        delete_connection,
    ])

    stdout, stderr, retcode = vm_util.IssueCommand(
        ['powershell', '-Command', cmd], timeout=None, raise_on_failure=False
    )

    if retcode:
      error_text = (
          'Got non-zero return code (%s) executing %s\nSTDOUT: %sSTDERR: %s'
          % (retcode, cmd, stdout, stderr)
      )
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    to_wait_for = [self._WaitForWinRmCommand]
    if FLAGS.cluster_boot_test_rdp_port_listening:
      to_wait_for.append(self._WaitForRdpPort)
    background_tasks.RunParallelThreads(
        [(method, [], {}) for method in to_wait_for], 2
    )

  @vm_util.Retry(log_errors=False, poll_interval=1, timeout=2400)
  def _WaitForRdpPort(self):
    self.TestConnectRemoteAccessPort(RDP_PORT)
    if self.rdp_port_listening_time is None:
      self.rdp_port_listening_time = time.time()

  @vm_util.Retry(log_errors=False, poll_interval=1, timeout=2400)
  def _WaitForWinRmCommand(self):
    """Waits for WinRM command and optionally for the WinRM port to listen."""
    # Test for listening on the port first, because this will happen strictly
    # first.
    if (
        FLAGS.cluster_boot_test_port_listening
        and self.port_listening_time is None
    ):
      self.TestConnectRemoteAccessPort()
      self.port_listening_time = time.time()

    # Always wait for remote host command to succeed, because it is necessary to
    # run benchmarks.
    self._WaitForSSH()
    if self.bootable_time is None:
      self.bootable_time = time.time()
    if FLAGS.log_windows_password:
      logging.info('Password for %s: %s', self, self.password)

  @vm_util.Retry(log_errors=False, poll_interval=1, timeout=2400)
  def _WaitForSSH(self):
    """Waits for the VMs to be ready."""
    stdout, _ = self.RemoteCommand('hostname', timeout=5)
    if self.hostname is None:
      self.hostname = stdout.rstrip()

  @vm_util.Retry(poll_interval=1, max_retries=15)
  def OnStartup(self):
    # Log driver information so that the user has a record of which drivers
    # were used.
    # TODO(user): put the driver information in the metadata.
    stdout, _ = self.RemoteCommand('dism /online /get-drivers', timeout=10)
    logging.info(stdout)
    stdout, _ = self.RemoteCommand('echo $env:TEMP', timeout=10)
    self.temp_dir = ntpath.join(stdout.strip(), 'pkb')
    stdout, _ = self.RemoteCommand('echo $env:USERPROFILE', timeout=10)
    self.home_dir = stdout.strip()
    stdout, _ = self.RemoteCommand('echo $env:SystemDrive', timeout=10)
    self.system_drive = stdout.strip()
    self.RemoteCommand('mkdir %s -Force' % self.temp_dir, timeout=10)
    self.DisableGuestFirewall()

  def _Reboot(self):
    """OS-specific implementation of reboot command."""
    self.RemoteCommand('shutdown -t 0 -r -f', ignore_failure=True)

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def VMLastBootTime(self):
    """Returns the time the VM was last rebooted as reported by the VM."""
    resp, _ = self.RemoteCommand('systeminfo | find /i "Boot Time"', timeout=10)
    return resp

  def _AfterReboot(self):
    """Performs any OS-specific setup on the VM following reboot.

    This will be called after every call to Reboot().
    """
    pass

  def InstallPackages(self, packages: str) -> None:
    """Installs packages using the OS's package manager."""
    pass

  @vm_util.Retry(poll_interval=10, max_retries=5)
  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if not self.install_packages:
      return
    if package_name not in self._installed_packages:
      package = windows_packages.PACKAGES[package_name]
      package.Install(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a Perfkit package on the VM."""
    package = windows_packages.PACKAGES[package_name]
    if hasattr(package, 'Uninstall'):
      package.Uninstall()

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the Perfkit Benchmarker temp directory on the VM
    and uninstalls all PerfKit packages.
    """
    for package_name in self._installed_packages:
      self.Uninstall(package_name)
    self.RemoteCommand('rm -recurse -force %s' % self.temp_dir)
    self.EnableGuestFirewall()

  def GetSha256sum(self, path, filename):
    """Gets the sha256sum hash for a filename in a path on the VM.

    Args:
      path: string; Path on the VM.
      filename: string; Name of the file in the path.

    Returns:
      string; The sha256sum hash.
    """
    file_path = ntpath.join(path, filename)
    stdout, _ = self.RemoteCommand(f'certUtil -hashfile {file_path} SHA256')
    return stdout.splitlines()[1]

  def WaitForProcessRunning(self, process, timeout):
    """Blocks until either the timeout passes or the process is running.

    Args:
      process: string name of the process.
      timeout: number of seconds to block while the process is not running.

    Raises:
      WaitTimeoutError: raised if the process does not run within "timeout"
                        seconds.
    """
    command = (
        '$count={timeout};'
        'while( (ps | select-string {process} | measure-object).Count '
        '-eq 0 -and $count -gt 0) {{sleep 1; $count=$count-1}}; '
        'if ($count -eq 0) {{echo "FAIL"}}'
    ).format(timeout=timeout, process=process)
    stdout, _ = self.RemoteCommand(command)
    if 'FAIL' in stdout:
      raise WaitTimeoutError()

  def IsProcessRunning(self, process):
    """Checks if a given process is running on the system.

    Args:
      process: string name of the process.

    Returns:
      Whether the process name is in the PS output.
    """
    stdout, _ = self.RemoteCommand('ps')
    return process in stdout

  @vm_util.Retry(poll_interval=1, max_retries=5)
  def _GetNumCpus(self):
    """Returns the number of logical CPUs on the VM.

    This method does not cache results (unlike "num_cpus").

    Returns:
      int. Number of logical CPUs.
    """
    stdout, _ = self.RemoteCommand(
        'Get-WmiObject -class Win32_processor | '
        'Select-Object -ExpandProperty NumberOfLogicalProcessors'
    )
    # In the case that there are multiple Win32_processor instances, the result
    # of this command can be a string like '4  4  '.
    return sum(int(i) for i in stdout.split())

  def _GetTotalFreeMemoryKb(self):
    """Returns the amount of free physical memory on the VM in Kilobytes."""
    raise NotImplementedError()

  def _GetTotalMemoryKb(self):
    """Returns the amount of physical memory on the VM in Kilobytes.

    This method does not cache results (unlike "total_memory_kb").
    """
    stdout, _ = self.RemoteCommand(
        'Get-WmiObject -class Win32_PhysicalMemory | select -exp Capacity'
    )
    result = sum(int(capacity) for capacity in stdout.split('\n') if capacity)
    return result / 1024

  def GetNVMEDeviceInfo(self):
    """Windows VMs rely on disk number instead of NVME info to prepare disks."""
    return []

  def GetTotalMemoryMb(self):
    return self._GetTotalMemoryKb() / 1024

  def _TestReachable(self, ip):
    """Returns True if the VM can reach the ip address and False otherwise."""
    return self.TryRemoteCommand('ping -n 1 %s' % ip)

  @vm_util.Retry(poll_interval=10, max_retries=5)
  def DownloadFile(self, url, dest):
    """Downloads the content at the url to the specified destination."""

    # Allow more security protocols to make it easier to download from
    # sites where we don't know the security protocol beforehand
    command = (
        '[Net.ServicePointManager]::SecurityProtocol = '
        '[System.Net.SecurityProtocolType] '
        '"tls, tls11, tls12";'
        'Invoke-WebRequest {url} -OutFile {dest}'
    ).format(url=url, dest=dest)
    self.RemoteCommand(command, timeout=5 * 60)

  def UnzipFile(self, zip_file, dest):
    """Unzips the file with the given path."""
    command = (
        'Add-Type -A System.IO.Compression.FileSystem; '
        "[IO.Compression.ZipFile]::ExtractToDirectory('{zip_file}', "
        "'{dest}')"
    ).format(zip_file=zip_file, dest=dest)
    self.RemoteCommand(command, timeout=5 * 60)

  def DisableGuestFirewall(self):
    """Disables the guest firewall."""
    command = 'netsh advfirewall set allprofiles state off'
    self.RemoteCommand(command, timeout=10)

  def EnableGuestFirewall(self):
    """Enables the guest firewall."""
    command = 'netsh advfirewall set allprofiles state on'
    self.RemoteCommand(command)

  def RunDiskpartScript(self, script):
    """Runs the supplied Diskpart script on the VM."""
    logging.info('Writing diskpart script \n %s', script)
    with vm_util.NamedTemporaryFile(prefix='diskpart', mode='w') as tf:
      tf.write(script)
      tf.close()
      script_path = ntpath.join(self.temp_dir, os.path.basename(tf.name))
      self.RemoteCopy(tf.name, script_path)
      self.RemoteCommand(
          'diskpart /s {script_path}'.format(script_path=script_path),
          timeout=60,
      )

  def DiskDriveIsLocal(self, device, model):
    """Helper method to determine if a disk drive is a local ssd to stripe."""

  def SetReadAhead(self, num_sectors, devices):
    """Set read-ahead value for block devices.

    Args:
      num_sectors: int. Number of sectors of read ahead.
      devices: list of strings. A list of block devices.
    """
    raise NotImplementedError()

  def SetProcessPriorityToHighByFlag(self, executable_name: str):
    """Sets the CPU priority for a given executable name.

    Note this only sets the CPU priority if FLAGS.set_cpu_priority_high is set.

    Args:
      executable_name: string. The executable name.
    """
    if not FLAGS.set_cpu_priority_high:
      return

    command = (
        "New-Item 'HKLM:\\SOFTWARE\\Microsoft\\Windows "
        "NT\\CurrentVersion\\Image File Execution Options\\{exe}\\PerfOptions' "
        '-Force | New-ItemProperty -Name CpuPriorityClass -Value 3 -Force'
    ).format(exe=executable_name)
    self.RemoteCommand(command)
    executables = self.os_metadata.get('high_cpu_priority')
    if executables:
      cast(list[str], executables).append(executable_name)
    else:
      self.os_metadata['high_cpu_priority'] = [executable_name]

  def _IsSmtEnabled(self):
    """Whether SMT is enabled on the vm."""
    # TODO(user): find way to do this in Windows
    raise NotImplementedError('SMT detection currently not implemented')


class Windows2016CoreMixin(BaseWindowsMixin):
  """Class holding Windows Server 2016 Server Core VM specifics."""

  OS_TYPE = os_types.WINDOWS2016_CORE


class Windows2019CoreMixin(BaseWindowsMixin):
  """Class holding Windows Server 2019 Server Core VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_CORE


class Windows2022CoreMixin(BaseWindowsMixin):
  """Class holding Windows Server 2022 Server Core VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_CORE


class Windows2025CoreMixin(BaseWindowsMixin):
  """Class holding Windows Server 2025 Server Core VM specifics."""

  OS_TYPE = os_types.WINDOWS2025_CORE


class Windows2016DesktopMixin(BaseWindowsMixin):
  """Class holding Windows Server 2016 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2016_DESKTOP


class Windows2019DesktopMixin(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_DESKTOP


class Windows2022DesktopMixin(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_DESKTOP


class Windows2025DesktopMixin(BaseWindowsMixin):
  """Class holding Windows Server 2025 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2025_DESKTOP


class Windows2019SQLServer2017Standard(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_SQLSERVER_2017_STANDARD


class Windows2019SQLServer2017Enterprise(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_SQLSERVER_2017_ENTERPRISE


class Windows2019SQLServer2019Standard(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_SQLSERVER_2019_STANDARD


class Windows2019SQLServer2019Enterprise(BaseWindowsMixin):
  """Class holding Windows Server 2019 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2019_SQLSERVER_2019_ENTERPRISE


class Windows2022SQLServer2019Standard(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_SQLSERVER_2019_STANDARD


class Windows2022SQLServer2019Enterprise(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_SQLSERVER_2019_ENTERPRISE


class Windows2022SQLServer2022Standard(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_SQLSERVER_2022_STANDARD


class Windows2022SQLServer2022Enterprise(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2022_SQLSERVER_2022_ENTERPRISE


class Windows2025SQLServer2022Standard(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2025_SQLSERVER_2022_STANDARD


class Windows2025SQLServer2022Enterprise(BaseWindowsMixin):
  """Class holding Windows Server 2022 with Desktop Experience VM specifics."""

  OS_TYPE = os_types.WINDOWS2025_SQLSERVER_2022_ENTERPRISE
