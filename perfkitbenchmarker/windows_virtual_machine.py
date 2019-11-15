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
import uuid

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_packages

import six
import timeout_decorator
import winrm

FLAGS = flags.FLAGS

flags.DEFINE_bool(
    'log_windows_password', False,
    'Whether to log passwords for Windows machines. This can be useful in '
    'the event of needing to manually RDP to the instance.')

flags.DEFINE_bool(
    'set_cpu_priority_high', False,
    'Allows executables to be set to High (up from Normal) CPU priority '
    'through the SetProcessPriorityToHigh function.')

SMB_PORT = 445
WINRM_PORT = 5986
RDP_PORT = 3389
# This startup script enables remote mangement of the instance. It does so
# by creating a WinRM listener (using a self-signed cert) and opening
# the WinRM port in the Windows firewall.
_STARTUP_SCRIPT = """
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
        base64.b64encode(_STARTUP_SCRIPT.encode('utf-16-le'))))

# Cygwin constants for installing and running commands through Cygwin.
# _CYGWIN_FORMAT provides a format string to transform a bash command into one
# that runs under Cygwin.
_CYGWIN32_URL = 'https://cygwin.com/setup-x86.exe'
_CYGWIN64_URL = 'https://cygwin.com/setup-x86_64.exe'
_CYGWIN_MIRROR = 'https://mirrors.kernel.org/sourceware/cygwin/'
_CYGWIN_ROOT = r'%PROGRAMFILES%\cygwinx86\cygwin'
_CYGWIN_FORMAT = (r"%s\bin\bash.exe -c 'export PATH=$PATH:/usr/bin && "
                  "{command}'" % _CYGWIN_ROOT)


class WaitTimeoutError(Exception):
  """Exception thrown if a wait operation takes too long."""


class WindowsMixin(virtual_machine.BaseOsMixin):
  """Class that holds Windows related VM methods and attributes."""

  OS_TYPE = os_types.WINDOWS
  BASE_OS_TYPE = os_types.WINDOWS

  def __init__(self):
    super(WindowsMixin, self).__init__()
    self.winrm_port = WINRM_PORT
    self.smb_port = SMB_PORT
    self.remote_access_ports = [self.winrm_port, self.smb_port, RDP_PORT]
    self.primary_remote_access_port = self.winrm_port
    self.rdp_port_listening_time = None
    self.temp_dir = None
    self.home_dir = None
    self.system_drive = None
    self._send_remote_commands_to_cygwin = False

  def RobustRemoteCommand(self, command, should_log=False, ignore_failure=False,
                          suppress_warning=False, timeout=None):
    """Runs a powershell command on the VM.

    Should be more robust than its counterpart, RemoteCommand. In the event of
    network failure, the process will continue on the VM, and we continually
    reconnect to check if it has finished. The tradeoff is this is noticeably
    slower than the normal RemoteCommand.

    The algorithm works as follows:
      1. Create a "command started" file
      2. Run the command
      3. Create a "command done" file

    If we fail to run step 1, we raise a RemoteCommandError. If we have network
    failure during step 2, the command will continue running on the VM and we
    will spin inside this function waiting for the "command done" file to be
    created.

    Args:
      command: A valid powershell command.
      should_log: A boolean indicating whether the command result should be
        logged at the info level. Even if it is false, the results will still be
        logged at the debug level.
      ignore_failure: Ignore any failure if set to true.
      suppress_warning: Suppress the result logging from IssueCommand when the
        return code is non-zero.
      timeout: Float. A timeout in seconds for the command. If None is passed,
        no timeout is applied. Timeout kills the winrm session which then kills
        the process being executed.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command or the
          command timed out.
    """

    logging.info('Running robust command on %s: %s', self, command)
    command_id = uuid.uuid4()
    logged_command = ('New-Item -Path %s.start -ItemType File; powershell "%s" '
                      '2> %s.err 1> %s.out; New-Item -Path %s.done -ItemType '
                      'File') % (command_id, command, command_id, command_id,
                                 command_id)
    start_command_time = time.time()
    try:
      self.RemoteCommand(
          logged_command,
          should_log=should_log,
          ignore_failure=ignore_failure,
          suppress_warning=suppress_warning,
          timeout=timeout)
    except errors.VirtualMachine.RemoteCommandError:
      logging.exception(
          'Exception while running %s on %s, waiting for command to finish',
          command, self)
    start_out, _ = self.RemoteCommand('Test-Path %s.start' % (command_id,))
    if 'True' not in start_out:
      raise errors.VirtualMachine.RemoteCommandError(
          'RobustRemoteCommand did not start on VM.')

    end_command_time = time.time()

    @timeout_decorator.timeout(
        timeout - (end_command_time - start_command_time),
        use_signals=False,
        timeout_exception=errors.VirtualMachine.RemoteCommandError)
    def wait_for_done_file():
      # Spin on the VM until the "done" file is created. It is better to spin
      # on the VM rather than creating a new session for each test.
      done_out = ''
      while 'True' not in done_out:
        done_out, _ = self.RemoteCommand(
            '$retries=0; while ((-not (Test-Path %s.done)) -and '
            '($retries -le 60)) { Start-Sleep -Seconds 1; $retries++ }; '
            'Test-Path %s.done' % (command_id, command_id))

    wait_for_done_file()
    stdout, _ = self.RemoteCommand('Get-Content %s.out' % (command_id,))
    _, stderr = self.RemoteCommand('Get-Content %s.err' % (command_id,))

    return stdout, stderr

  def RemoteCommand(self, command, should_log=False, ignore_failure=False,
                    suppress_warning=False, timeout=None):
    """Runs a powershell command on the VM.

    Args:
      command: A valid powershell command.
      should_log: A boolean indicating whether the command result should be
          logged at the info level. Even if it is false, the results will
          still be logged at the debug level.
      ignore_failure: Ignore any failure if set to true.
      suppress_warning: Suppress the result logging from IssueCommand when the
          return code is non-zero.
      timeout: Float. A timeout in seconds for the command. If None is passed,
          no timeout is applied. Timeout kills the winrm session which then
          kills the process being executed.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command or the
          command timed out.
    """
    logging.info('Running command on %s: %s', self, command)
    s = winrm.Session('https://%s:%s' % (self.ip_address, self.winrm_port),
                      auth=(self.user_name, self.password),
                      server_cert_validation='ignore')
    encoded_command = six.ensure_str(
        base64.b64encode(command.encode('utf_16_le')))

    @timeout_decorator.timeout(timeout, use_signals=False,
                               timeout_exception=errors.VirtualMachine.
                               RemoteCommandError)
    def run_command():
      return s.run_cmd('powershell -encodedcommand %s' % encoded_command)

    r = run_command()
    retcode, stdout, stderr = r.status_code, six.ensure_str(
        r.std_out), six.ensure_str(r.std_err)

    debug_text = ('Ran %s on %s. Return code (%s).\nSTDOUT: %s\nSTDERR: %s' %
                  (command, self, retcode, stdout, stderr))
    if should_log or (retcode and not suppress_warning):
      logging.info(debug_text)
    else:
      logging.debug(debug_text)

    if retcode and not ignore_failure:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, command, stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)

    return stdout, stderr

  def InstallCygwin(self, bit64=True, packages=None):
    """Downloads and installs cygwin on the Windows instance.

    TODO(deitz): Support installing packages via vm.Install calls where the VM
    would look in Linux packages and try to find a CygwinInstall function to
    call. Alternatively, consider using cyg-apt as an installation method. With
    this additional change, we could use similar code to run benchmarks under
    both Windows and Linux (if necessary and useful).

    Args:
      bit64: Whether to use 64-bit Cygwin (default) or 32-bit Cygwin.
      packages: List of packages to install on Cygwin.
    """
    url = _CYGWIN64_URL if bit64 else _CYGWIN32_URL
    setup_exe = url.split('/')[-1]
    self.DownloadFile(url, setup_exe)
    self.RemoteCommand(
        r'.\{setup_exe} --quiet-mode --site {mirror} --root "{cygwin_root}" '
        '--packages {packages}'.format(
            setup_exe=setup_exe,
            mirror=_CYGWIN_MIRROR,
            cygwin_root=_CYGWIN_ROOT,
            packages=','.join(packages)))

  def RemoteCommandCygwin(self, command, *args, **kwargs):
    """Runs a Cygwin command on the VM.

    Args:
      command: A valid bash command to run under Cygwin.
      *args: Arguments passed directly to RemoteCommandWithReturnCode.
      **kwargs: Keyword arguments passed directly to
          RemoteCommandWithReturnCode.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command or the
          command timed out.
    """
    # Wrap the command to be executed via bash.exe under Cygwin. Escape quotes
    # since they are executed in a string.
    cygwin_command = _CYGWIN_FORMAT.format(command=command.replace('"', r'\"'))
    return self.RemoteCommand(cygwin_command, *args, **kwargs)

  def RemoteCopy(self, local_path, remote_path='', copy_to=True):
    """Copies a file to or from the VM.

    Args:
      local_path: Local path to file.
      remote_path: Optional path of where to copy file on remote host.
      copy_to: True to copy to vm, False to copy from vm.

    Raises:
      RemoteCommandError: If there was a problem copying the file.
    """
    remote_path = remote_path or '~/'
    # In order to expand "~" and "~user" we use ntpath.expanduser(),
    # but it relies on environment variables being set. This modifies
    # the HOME environment variable in order to use that function, and then
    # restores it to its previous value.
    home = os.environ.get('HOME')
    try:
      os.environ['HOME'] = self.home_dir
      remote_path = ntpath.expanduser(remote_path)
    finally:
      if home is None:
        del os.environ['HOME']
      else:
        os.environ['HOME'] = home

    drive, remote_path = ntpath.splitdrive(remote_path)
    remote_drive = (drive or self.system_drive).rstrip(':')
    network_drive = '\\\\%s\\%s$' % (self.ip_address, remote_drive)

    if vm_util.RunningOnWindows():
      self._PsDriveRemoteCopy(local_path, remote_path, copy_to, network_drive)
    else:
      self._SmbclientRemoteCopy(local_path, remote_path, copy_to, network_drive)

  def _SmbclientRemoteCopy(self, local_path, remote_path,
                           copy_to, network_drive):
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
        'smbclient', network_drive,
        '--max-protocol', 'SMB3',
        '--user', '%s%%%s' % (self.user_name, self.password),
        '--port', str(self.smb_port),
        '--command', smb_command
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(smb_copy,
                                                   raise_on_failure=False)
    if retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, smb_copy, stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  def _PsDriveRemoteCopy(self, local_path, remote_path,
                         copy_to, network_drive):
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
        '$pw = convertto-securestring -AsPlainText -Force \'%s\';'
        '$cred = new-object -typename System.Management.Automation'
        '.PSCredential -argumentlist %s,$pw' % (password, self.user_name))

    psdrive_name = self.name
    create_psdrive = (
        'New-PSDrive -Name %s -PSProvider filesystem -Root '
        '%s -Credential $cred' % (psdrive_name, network_drive))

    remote_path = '%s:%s' % (psdrive_name, remote_path)
    if copy_to:
      from_path, to_path = local_path, remote_path
    else:
      from_path, to_path = remote_path, local_path

    copy_item = 'Copy-Item -Path %s -Destination %s' % (from_path, to_path)

    delete_connection = 'net use %s /delete' % network_drive

    cmd = ';'.join([set_error_pref, create_cred, create_psdrive,
                    copy_item, delete_connection])

    stdout, stderr, retcode = vm_util.IssueCommand(
        ['powershell', '-Command', cmd], timeout=None, raise_on_failure=False)

    if retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, cmd, stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    to_wait_for = [self._WaitForWinRmCommand]
    if FLAGS.cluster_boot_test_rdp_port_listening:
      to_wait_for.append(self._WaitForRdpPort)
    vm_util.RunParallelThreads([(method, [], {}) for method in to_wait_for], 2)

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
    if (FLAGS.cluster_boot_test_port_listening and
        self.port_listening_time is None):
      self.TestConnectRemoteAccessPort()
      self.port_listening_time = time.time()

    # Always wait for remote host command to succeed, because it is necessary to
    # run benchmarks.
    stdout, _ = self.RemoteCommand('hostname', suppress_warning=True)
    if self.bootable_time is None:
      self.bootable_time = time.time()
    if self.hostname is None:
      self.hostname = stdout.rstrip()
    if FLAGS.log_windows_password:
      logging.info('Password for %s: %s', self, self.password)

  @vm_util.Retry(poll_interval=1, max_retries=15)
  def OnStartup(self):
    # Log driver information so that the user has a record of which drivers
    # were used.
    # TODO(user): put the driver information in the metadata.
    stdout, _ = self.RemoteCommand('dism /online /get-drivers')
    logging.info(stdout)
    stdout, _ = self.RemoteCommand('echo $env:TEMP')
    self.temp_dir = ntpath.join(stdout.strip(), 'pkb')
    stdout, _ = self.RemoteCommand('echo $env:USERPROFILE')
    self.home_dir = stdout.strip()
    stdout, _ = self.RemoteCommand('echo $env:SystemDrive')
    self.system_drive = stdout.strip()
    self.RemoteCommand('mkdir %s' % self.temp_dir)
    self.DisableGuestFirewall()

  def _Reboot(self):
    """OS-specific implementation of reboot command."""
    self.RemoteCommand('shutdown -t 0 -r -f', ignore_failure=True)

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def VMLastBootTime(self):
    """Returns the time the VM was last rebooted as reported by the VM."""
    resp, _ = self.RemoteCommand(
        'systeminfo | find /i "Boot Time"', suppress_warning=True)
    return resp

  def _AfterReboot(self):
    """Performs any OS-specific setup on the VM following reboot.

    This will be called after every call to Reboot().
    """
    pass

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

  def WaitForProcessRunning(self, process, timeout):
    """Blocks until either the timeout passes or the process is running.

    Args:
      process: string name of the process.
      timeout: number of seconds to block while the process is not running.

    Raises:
      WaitTimeoutError: raised if the process does not run within "timeout"
                        seconds.
    """
    command = ('$count={timeout};'
               'while( (ps | select-string {process} | measure-object).Count '
               '-eq 0 -and $count -gt 0) {{sleep 1; $count=$count-1}}; '
               'if ($count -eq 0) {{echo "FAIL"}}').format(
                   timeout=timeout, process=process)
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

  def _GetNumCpus(self):
    """Returns the number of logical CPUs on the VM.

    This method does not cache results (unlike "num_cpus").

    Returns:
      int. Number of logical CPUs.
    """
    stdout, _ = self.RemoteCommand(
        'Get-WmiObject -class Win32_processor | '
        'Select-Object -ExpandProperty NumberOfLogicalProcessors')
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
        'Get-WmiObject -class Win32_PhysicalMemory | '
        'select -exp Capacity')
    result = sum(int(capacity) for capacity in stdout.split('\n') if capacity)
    return result / 1024

  def GetTotalMemoryMb(self):
    return self._GetTotalMemoryKb() / 1024

  def _TestReachable(self, ip):
    """Returns True if the VM can reach the ip address and False otherwise."""
    return self.TryRemoteCommand('ping -n 1 %s' % ip)

  def DownloadFile(self, url, dest):
    """Downloads the content at the url to the specified destination."""

    # Allow more security protocols to make it easier to download from
    # sites where we don't know the security protocol beforehand
    command = ('[Net.ServicePointManager]::SecurityProtocol = '
               '[System.Net.SecurityProtocolType] '
               '"tls, tls11, tls12";'
               'Invoke-WebRequest {url} -OutFile {dest}').format(
                   url=url, dest=dest)
    self.RemoteCommand(command)

  def UnzipFile(self, zip_file, dest):
    """Unzips the file with the given path."""
    command = ('Add-Type -A System.IO.Compression.FileSystem; '
               '[IO.Compression.ZipFile]::ExtractToDirectory(\'{zip_file}\', '
               '\'{dest}\')').format(zip_file=zip_file, dest=dest)
    self.RemoteCommand(command)

  def DisableGuestFirewall(self):
    """Disables the guest firewall."""
    command = 'netsh advfirewall set allprofiles state off'
    self.RemoteCommand(command)

  def EnableGuestFirewall(self):
    """Enables the guest firewall."""
    command = 'netsh advfirewall set allprofiles state on'
    self.RemoteCommand(command)

  def _RunDiskpartScript(self, script):
    """Runs the supplied Diskpart script on the VM."""
    with vm_util.NamedTemporaryFile(prefix='diskpart') as tf:
      tf.write(script)
      tf.close()
      script_path = ntpath.join(self.temp_dir, os.path.basename(tf.name))
      self.RemoteCopy(tf.name, script_path)
      self.RemoteCommand('diskpart /s {script_path}'.format(
          script_path=script_path))

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
      data_disk = disk.StripedDisk(disk_spec, disks)
    else:
      data_disk = disks[0]

    self.scratch_disks.append(data_disk)

    if data_disk.disk_type != disk.LOCAL:
      data_disk.Create()
      data_disk.Attach(self)

    # Create and then run a Diskpart script that will initialize the disks,
    # create a volume, and then format and mount the volume.
    script = ''

    disk_numbers = [str(d.disk_number) for d in disks]
    for disk_number in disk_numbers:
      # For each disk, set the status to online (if it is not already),
      # remove any formatting or partitioning on the disks, and convert
      # it to a dynamic disk so it can be used to create a volume.
      script += ('select disk %s\n'
                 'online disk noerr\n'
                 'attributes disk clear readonly\n'
                 'clean\n'
                 'convert dynamic\n' % disk_number)

    # Create a volume out of the disk(s).
    if data_disk.is_striped:
      script += 'create volume stripe disk=%s\n' % ','.join(disk_numbers)
    else:
      script += 'create volume simple\n'

    # If a mount point has been specified, create the directory where it will be
    # mounted, format the volume, and assign the mount point to the volume.
    if disk_spec.mount_point:
      self.RemoteCommand('mkdir %s' % disk_spec.mount_point)
      script += ('format quick\n'
                 'assign mount=%s\n' % disk_spec.mount_point)

    self._RunDiskpartScript(script)

  def SetReadAhead(self, num_sectors, devices):
    """Set read-ahead value for block devices.

    Args:
      num_sectors: int. Number of sectors of read ahead.
      devices: list of strings. A list of block devices.
    """
    raise NotImplementedError()

  def SetProcessPriorityToHighByFlag(self, executable_name):
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
      executables.append(executable_name)
    else:
      self.os_metadata['high_cpu_priority'] = [executable_name]


class Windows2012CoreMixin(WindowsMixin):
  """Class holding Windows Server 2012 Server Core VM specifics."""
  OS_TYPE = os_types.WINDOWS2012_CORE


class Windows2016CoreMixin(WindowsMixin):
  """Class holding Windows Server 2016 Server Core VM specifics."""
  OS_TYPE = os_types.WINDOWS2016_CORE


class Windows2019CoreMixin(WindowsMixin):
  """Class holding Windows Server 2019 Server Core VM specifics."""
  OS_TYPE = os_types.WINDOWS2019_CORE


class Windows2012BaseMixin(WindowsMixin):
  """Class holding Windows Server 2012 Server Base VM specifics."""
  OS_TYPE = os_types.WINDOWS2012_BASE


class Windows2016BaseMixin(WindowsMixin):
  """Class holding Windows Server 2016 Server Base VM specifics."""
  OS_TYPE = os_types.WINDOWS2016_BASE


class Windows2019BaseMixin(WindowsMixin):
  """Class holding Windows Server 2019 Server Base VM specifics."""
  OS_TYPE = os_types.WINDOWS2019_BASE
