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

import base64
import logging
import ntpath
import os
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_packages

import winrm

FLAGS = flags.FLAGS

SMB_PORT = 445
WINRM_PORT = 5986
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
    protocol=TCP localport={port}
""".format(port=WINRM_PORT)
STARTUP_SCRIPT = 'powershell -EncodedCommand {encoded_command}'.format(
    encoded_command=base64.b64encode(_STARTUP_SCRIPT.encode('utf-16-le')))


class WindowsMixin(virtual_machine.BaseOsMixin):

  OS_TYPE = os_types.WINDOWS

  def __init__(self):
    super(WindowsMixin, self).__init__()
    self.winrm_port = WINRM_PORT
    self.smb_port = SMB_PORT
    self.remote_access_ports = [self.winrm_port, self.smb_port]
    self.temp_dir = None
    self.home_dir = None
    self.system_drive = None

  def RemoteCommand(self, command, should_log=False, ignore_failure=False,
                    suppress_warning=False, timeout=None):
    """Runs a powershell command on the VM.

    Args:
      command: A valid bash command.
      should_log: A boolean indicating whether the command result should be
          logged at the info level. Even if it is false, the results will
          still be logged at the debug level.
      ignore_failure: Ignore any failure if set to true.
      suppress_warning: Suppress the result logging from IssueCommand when the
          return code is non-zero.
      timeout: A timeout in seconds for the command. This argument is currently
          unused.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    logging.info('Running command on %s: %s', self, command)
    s = winrm.Session('https://%s:%s' % (self.ip_address, self.winrm_port),
                      auth=(self.user_name, self.password),
                      server_cert_validation='ignore')
    encoded_command = base64.b64encode(command.encode('utf_16_le'))
    r = s.run_cmd('powershell -encodedcommand %s' % encoded_command)
    retcode, stdout, stderr = r.status_code, r.std_out, r.std_err

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
    home = os.environ['HOME']
    try:
      os.environ['HOME'] = self.home_dir
      remote_path = ntpath.expanduser(remote_path)
    finally:
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
        '--user', '%s%%%s' % (self.user_name, self.password),
        '--port', str(self.smb_port),
        '--command', smb_command
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(smb_copy)
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
        ['powershell', '-Command', cmd], timeout=None)

    if retcode:
      error_text = ('Got non-zero return code (%s) executing %s\n'
                    'STDOUT: %sSTDERR: %s' %
                    (retcode, cmd, stdout, stderr))
      raise errors.VirtualMachine.RemoteCommandError(error_text)

  @vm_util.Retry(log_errors=False, poll_interval=1)
  def WaitForBootCompletion(self):
    """Waits until VM is has booted."""
    stdout, _ = self.RemoteCommand('hostname', suppress_warning=True)
    if self.bootable_time is None:
      self.bootable_time = time.time()
    if self.hostname is None:
      self.hostname = stdout.rstrip()

  def OnStartup(self):
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

  def VMLastBootTime(self):
    """Returns the UTC time the VM was last rebooted as reported by the VM."""
    resp, _ = self.RemoteHostCommand('systeminfo | find /i "Boot Time"',
                                     suppress_warning=True)
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
    return int(stdout) / 1024

  def _TestReachable(self, ip):
    """Returns True if the VM can reach the ip address and False otherwise."""
    return self.TryRemoteCommand('ping -n 1 %s' % ip)

  def DownloadFile(self, url, dest):
    """Downloads the content at the url to the specified destination."""

    command = 'Invoke-WebRequest {url} -OutFile {dest}'.format(
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
      self.RemoteCopy(tf.name, self.temp_dir)
      script_path = ntpath.join(self.temp_dir, os.path.basename(tf.name))
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
