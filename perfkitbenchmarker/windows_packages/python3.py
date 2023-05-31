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

"""Module containing python installation and path-setting functions."""
import ntpath

PYTHON_VERSION = "3.10.10"
PYTHON_EXE_URL = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}.exe"
PYTHON_EXE = f"python-{PYTHON_VERSION}.exe"


def Install(vm):
  """Installs the python3 package on the VM."""
  python_download_path = ntpath.join(vm.temp_dir, "python", "")
  vm.RemoteCommand(f"New-Item -Path {python_download_path} -ItemType Directory")
  python_exe = ntpath.join(python_download_path, PYTHON_EXE)

  vm.DownloadFile(PYTHON_EXE_URL, python_exe)
  vm.RemoteCommand(f"{python_exe} /quiet TargetDir={python_download_path}")
  AddtoPath(vm)


def AddtoPath(vm):
  """Adds the python and scripts paths to the path environment variable."""
  python_exe_path = ntpath.join(vm.temp_dir, "python")
  # Include scripts path to enable usage of pip
  scripts_path = ntpath.join(python_exe_path, "scripts")
  vm.RemoteCommand(f'setx PATH "$env:path;{python_exe_path};{scripts_path}" -m')
