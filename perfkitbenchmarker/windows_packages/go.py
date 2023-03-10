# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing go installation and path-setting functions."""
import ntpath

GO_VERSION = "20.2"
GO_MSI = f"go1.{GO_VERSION}.windows-amd64.msi"
GO_MSI_URL = f"https://go.dev/dl/{GO_MSI}"


def Install(vm):
  """Installs the go package on the VM."""
  go_download_path = ntpath.join(vm.temp_dir, "go", "")
  vm.RemoteCommand(f"New-Item -Path {go_download_path} -ItemType Directory")
  go_msi = ntpath.join(go_download_path, GO_MSI)

  vm.DownloadFile(GO_MSI_URL, go_msi)
  vm.RemoteCommand(
      f"Start-Process msiexec -Wait -ArgumentList /i, {go_msi},"
      f' TARGETDIR="{go_download_path}", /qn'
  )
  vm.RemoteCommand(f"dir {go_download_path}")
  AddtoPath(vm)


def AddtoPath(vm):
  """Adds the go path to the path environment variable."""
  go_exe_path = ntpath.join(vm.temp_dir, "go")
  vm.RemoteCommand(f'setx PATH "$env:path;{go_exe_path}" -m')
