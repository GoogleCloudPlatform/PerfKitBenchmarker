# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Class to represent a GCE Windows Virtual Machine object."""

import json
import ntpath
from typing import Any, Dict, Tuple

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

_WINDOWS_SHUTDOWN_SCRIPT_PS1 = 'Write-Host | gsutil cp - {preempt_marker}'

_METADATA_PREEMPT_CMD_WIN = (
    'Invoke-RestMethod -Uri'
    f' {gce_virtual_machine.METADATA_PREEMPT_URI} '
    '-Headers @{"Metadata-Flavor"="Google"}'
)

FLAGS = flags.FLAGS
ATTACHED_DISK_LETTER = 'F'
BAT_SCRIPT = """
:WAIT
echo waiting for MSSQLSERVER
sc start MSSQLSERVER
ping 127.0.0.1 -t 1 > NUL
for /f "tokens=4" %%s in ('sc query MSSQLSERVER ^| find "STATE"') do if NOT "%%s"=="RUNNING" goto WAIT
echo MSSQLSERVER is now running!
sqlcmd.exe -Q "CREATE LOGIN [%COMPUTERNAME%\\perfkit] from windows;"
sqlcmd.exe -Q "ALTER SERVER ROLE [sysadmin] ADD MEMBER [%COMPUTERNAME%\\perfkit]"
"""
UPDATE_NETWORK_DRIVERS = """
googet -noconfirm update
"""


class GceUnexpectedWindowsAdapterOutputError(Exception):
  """Raised when querying the status of a windows adapter failed."""


class GceDriverDoesntSupportFeatureError(Exception):
  """Raised if there is an attempt to set a feature not supported."""


class WindowsGceVirtualMachine(
    gce_virtual_machine.GceVirtualMachine,
    windows_virtual_machine.BaseWindowsMixin,
):
  """Class supporting Windows GCE virtual machines."""

  DEFAULT_X86_IMAGE_FAMILY = {
      os_types.WINDOWS2016_CORE: 'windows-2016-core',
      os_types.WINDOWS2019_CORE: 'windows-2019-core',
      os_types.WINDOWS2022_CORE: 'windows-2022-core',
      os_types.WINDOWS2025_CORE: 'windows-2025-core',
      os_types.WINDOWS2016_DESKTOP: 'windows-2016',
      os_types.WINDOWS2019_DESKTOP: 'windows-2019',
      os_types.WINDOWS2022_DESKTOP: 'windows-2022',
      os_types.WINDOWS2025_DESKTOP: 'windows-2025',
  }

  GVNIC_DISABLED_OS_TYPES = []

  NVME_START_INDEX = 0
  OS_TYPE = os_types.WINDOWS_CORE_OS_TYPES + os_types.WINDOWS_DESKOP_OS_TYPES

  def __init__(self, vm_spec):
    """Initialize a Windows GCE virtual machine.

    Args:
      vm_spec: virtual_machine_spec.BaseVmSpec object of the vm.
    """
    super().__init__(vm_spec)
    self.boot_metadata['windows-startup-script-ps1'] = (
        windows_virtual_machine.STARTUP_SCRIPT + UPDATE_NETWORK_DRIVERS
    )

  def DownloadPreprovisionedData(
      self,
      install_path,
      module_name,
      filename,
      timeout=virtual_machine.PREPROVISIONED_DATA_TIMEOUT,
  ):
    """Downloads a data file from a GCS bucket with pre-provisioned data.

    Use --gce_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisionedData, Five minutes by
        default.
    """
    # TODO(deitz): Add retry logic.
    temp_local_path = vm_util.GetTempDir()
    vm_util.IssueCommand(
        gce_virtual_machine.GenerateDownloadPreprovisionedDataCommand(
            temp_local_path, module_name, filename
        ).split(' '),
        timeout=timeout,
    )
    self.PushFile(
        vm_util.PrependTempDir(filename), ntpath.join(install_path, filename)
    )
    vm_util.IssueCommand(['rm', vm_util.PrependTempDir(filename)])

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    return (
        FLAGS.gcp_preprovisioned_data_bucket
        and vm_util.IssueCommand(
            gce_virtual_machine.GenerateStatPreprovisionedDataCommand(
                module_name, filename
            ).split(' '),
            raise_on_failure=False,
        )[-1]
        == 0
    )

  def _GetWindowsPassword(self):
    """Generates a command to get a VM user's password.

    Returns:
      Password for the windows user.
    """
    cmd = util.GcloudCommand(
        self, 'compute', 'reset-windows-password', self.name
    )
    cmd.flags['user'] = self.user_name
    stdout, _ = cmd.IssueRetryable()
    response = json.loads(stdout)
    return response['password']

  def Create(self, restore: bool = False) -> None:
    """Get the windows password from the VM after VM is ready."""
    super().Create(restore=restore)
    self.password = self._GetWindowsPassword()

  def _PreemptibleMetadataKeyValue(self) -> Tuple[str, str]:
    """See base class."""
    return 'windows-shutdown-script-ps1', _WINDOWS_SHUTDOWN_SCRIPT_PS1.format(
        preempt_marker=self.preempt_marker
    )

  @vm_util.Retry(
      max_retries=10,
      retryable_exceptions=(
          GceUnexpectedWindowsAdapterOutputError,
          errors.VirtualMachine.RemoteCommandError,
      ),
  )
  def GetResourceMetadata(self) -> Dict[str, Any]:
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping metadata key to value.
    """
    result = super().GetResourceMetadata()
    result['disable_rss'] = self.disable_rss
    return result

  def DisableRSS(self):
    """Disables RSS on the GCE VM.

    Raises:
      GceDriverDoesntSupportFeatureError: If RSS is not supported.
      GceUnexpectedWindowsAdapterOutputError: If querying the RSS state
        returns unexpected output.
    """
    # First ensure that the driver supports interrupt moderation
    net_adapters, _ = self.RemoteCommand('Get-NetAdapter')
    if 'Red Hat VirtIO Ethernet Adapter' not in net_adapters:
      raise GceDriverDoesntSupportFeatureError(
          'Driver not tested with RSS disabled in PKB.'
      )

    command = 'netsh int tcp set global rss=disabled'
    self.RemoteCommand(command)
    try:
      self.RemoteCommand('Restart-NetAdapter -Name "Ethernet"')
    except OSError:
      # Restarting the network adapter will always fail because
      # the winrm connection used to issue the command will be
      # broken.
      pass

    # Verify the setting went through
    stdout, _ = self.RemoteCommand('netsh int tcp show global')
    if 'Receive-Side Scaling State          : enabled' in stdout:
      raise GceUnexpectedWindowsAdapterOutputError('RSS failed to disable.')

  def _AcquireWritePermissionsLinux(self):
    gcs.GoogleCloudStorageService.AcquireWritePermissionsWindows(self)

  def SupportGVNIC(self) -> bool:
    return self.OS_TYPE not in self.GVNIC_DISABLED_OS_TYPES

  def GetDefaultImageFamily(self, is_arm: bool) -> str:
    assert not is_arm
    return self.DEFAULT_X86_IMAGE_FAMILY[self.OS_TYPE]

  def GetDefaultImageProject(self) -> str:
    if self.OS_TYPE in os_types.WINDOWS_SQLSERVER_OS_TYPES:
      return 'windows-sql-cloud'
    return 'windows-cloud'

  @property
  def _MetadataPreemptCmd(self) -> str:
    return _METADATA_PREEMPT_CMD_WIN

  def DiskDriveIsLocal(self, device, model):
    """Helper method to determine if a disk drive is a local ssd to stripe."""
    if (
        model.lower().strip() == 'nvme_card'
        or model.lower().strip() == 'google ephemeraldisk'
    ):
      return True
    return False


class WindowsGceSqlServerVirtualMachine(WindowsGceVirtualMachine):
  """Class supporting Windows GCE sql server virtual machines."""

  DEFAULT_X86_IMAGE_FAMILY = {
      os_types.WINDOWS2019_SQLSERVER_2017_STANDARD: 'sql-std-2017-win-2019',
      os_types.WINDOWS2019_SQLSERVER_2017_ENTERPRISE: 'sql-ent-2017-win-2019',
      os_types.WINDOWS2019_SQLSERVER_2019_STANDARD: 'sql-std-2019-win-2019',
      os_types.WINDOWS2019_SQLSERVER_2019_ENTERPRISE: 'sql-ent-2019-win-2019',
      os_types.WINDOWS2019_SQLSERVER_2025_STANDARD: 'sql-std-2025-win-2019',
      os_types.WINDOWS2019_SQLSERVER_2025_ENTERPRISE: 'sql-ent-2025-win-2019',
      os_types.WINDOWS2022_SQLSERVER_2019_STANDARD: 'sql-std-2019-win-2022',
      os_types.WINDOWS2022_SQLSERVER_2019_ENTERPRISE: 'sql-ent-2019-win-2022',
      os_types.WINDOWS2022_SQLSERVER_2022_STANDARD: 'sql-std-2022-win-2022',
      os_types.WINDOWS2022_SQLSERVER_2022_ENTERPRISE: 'sql-ent-2022-win-2022',
      os_types.WINDOWS2022_SQLSERVER_2025_STANDARD: 'sql-std-2025-win-2022',
      os_types.WINDOWS2022_SQLSERVER_2025_ENTERPRISE: 'sql-ent-2025-win-2022',
      os_types.WINDOWS2025_SQLSERVER_2019_STANDARD: 'sql-std-2019-win-2025',
      os_types.WINDOWS2025_SQLSERVER_2019_ENTERPRISE: 'sql-ent-2019-win-2025',
      os_types.WINDOWS2025_SQLSERVER_2022_STANDARD: 'sql-std-2022-win-2025',
      os_types.WINDOWS2025_SQLSERVER_2022_ENTERPRISE: 'sql-ent-2022-win-2025',
      os_types.WINDOWS2025_SQLSERVER_2025_STANDARD: 'sql-std-2025-win-2025',
      os_types.WINDOWS2025_SQLSERVER_2025_ENTERPRISE: 'sql-ent-2025-win-2025',
  }

  OS_TYPE = os_types.WINDOWS_SQLSERVER_OS_TYPES

  def __init__(self, vm_spec):
    super().__init__(vm_spec)
    self.boot_metadata['windows-startup-script-bat'] = "'" + BAT_SCRIPT + "'"
