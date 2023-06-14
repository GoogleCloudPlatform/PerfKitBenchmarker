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

"""Module containing mssqltools windows installation and cleanup functions."""
import ntpath

# ODBC driver ver17 download link
ODBC_17_DOWNLOAD_LINK = 'https://go.microsoft.com/fwlink/?linkid=2200731'
ODBC_17_INSTALLER = 'msodbcsql.msi'

# Visual C++ 2017 Redistribute
VC_REDIST_DOWNLOAD_LINK = 'https://aka.ms/vs/15/release/vc_redist.x64.exe'
VC_REDIST_INSTALLER = 'VC_redist.x64.exe'

# Microsoft Commandline Utility
SQLCMD_DOWNLOAD_LINK = 'https://go.microsoft.com/fwlink/?linkid=2142258'
SQLCMD_INSTALLER = 'MsSqlCmdLnUtils.msi'


def Install(vm):
  """Installs the mssql-tools package on the VM for Debian systems."""
  # Download Visual C++ 2017 Redistributable (needed on Azure)
  download_path = ntpath.join(vm.temp_dir, VC_REDIST_INSTALLER)
  vm.DownloadFile(VC_REDIST_DOWNLOAD_LINK, download_path)
  vm.RemoteCommand(f'{download_path} /q /norestart')

  # Downloading and installing odbc driver 17.
  download_path = ntpath.join(vm.temp_dir, ODBC_17_INSTALLER)
  vm.DownloadFile(ODBC_17_DOWNLOAD_LINK, download_path)
  vm.RemoteCommand(
      f'msiexec /i {download_path} IACCEPTMSODBCSQLLICENSETERMS=YES /passive'
  )

  # Downloading and installing MSSQL.
  download_path = ntpath.join(vm.temp_dir, SQLCMD_INSTALLER)
  vm.DownloadFile(SQLCMD_DOWNLOAD_LINK, download_path)
  vm.RemoteCommand(
      f'msiexec /i {download_path} IACCEPTMSSQLCMDLNUTILSLICENSETERMS=YES'
      ' /passive'
  )
