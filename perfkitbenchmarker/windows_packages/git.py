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

"""Module containing git installation and path-setting functions."""
import ntpath

GIT_VERSION = "2.39.2"
GIT_EXE = f"Git-{GIT_VERSION}-32-bit.exe"
GIT_EXE_URL = f"https://github.com/git-for-windows/git/releases/download/v{GIT_VERSION}.windows.1/{GIT_EXE}"


def Install(vm):
  """Installs the git package on the VM."""
  git_download_path = ntpath.join(vm.temp_dir, "git", "")
  vm.RemoteCommand(f"New-Item -Path {git_download_path} -ItemType Directory")
  git_exe = ntpath.join(git_download_path, GIT_EXE)

  vm.DownloadFile(GIT_EXE_URL, git_exe)
  vm.RemoteCommand(f"{git_exe} /VERYSILENT /NORESTART Dir={git_download_path}")
  AddtoPath(vm)


def AddtoPath(vm):
  """Adds the git path to the path environment variable."""
  git_exe_path = ntpath.join(vm.temp_dir, "git")
  vm.RemoteCommand(f'setx PATH "$env:path;{git_exe_path}" -m')
