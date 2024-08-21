# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing python 3 installation and cleanup functions."""


# Gets major.minor version of python
GET_VERSION = (
    'import sys; print(".".join(str(v) for v in sys.version_info[:2]))'
)


def Install(vm):
  """Installs the package on the VM."""
  vm.InstallPackages('python3')


def GetPythonVersion(vm) -> str:
  """Get the major.minor version of Python on the vm."""
  python_version, _ = vm.RemoteCommand(f"python3 -c '{GET_VERSION}'")
  return python_version.strip()
