# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing Python 3 pip installation and cleanup functions.

Uninstalling the pip package will also remove all python packages
added after installation.
"""

from perfkitbenchmarker.linux_packages import pip


def Install(vm):
  """Install pip3 on the VM."""
  # Work around Ubuntu distutils weirdness.
  # https://github.com/pypa/get-pip/issues/44
  if vm.HasPackage('python3-distutils'):
    vm.InstallPackages('python3-distutils')
  pip.Install(vm, pip_cmd='pip3', python_cmd='python3')


def Uninstall(vm):
  """Uninstalls the pip package on the VM."""
  pip.Uninstall(vm, pip_cmd='pip3')
