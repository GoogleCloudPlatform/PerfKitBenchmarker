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
  # This is also run in pip.py, but it must come before removing
  # EXTERNALLY-MANAGED below.
  vm.Install('python3_dev')
  # Python 3.11 added https://peps.python.org/pep-0668/, which allows the OS to
  # disable any pip installation outside of a virtual env. This is good in
  # long-lived systems, but counter-productive in short lived test
  # environmements (it is possible a system command or daemon does fail,
  # because PKB installs a root pip module, but relatively unlikely).
  # Delete the file so that pip will continue to install root packages.
  # https://discuss.python.org/t/pep-668-marking-python-base-environments-as-externally-managed/10302/80
  vm.RemoteCommand('sudo rm -f /usr/lib/python3*/EXTERNALLY-MANAGED')
  # Work around Ubuntu distutils weirdness.
  # https://github.com/pypa/get-pip/issues/44
  if vm.HasPackage('python3-distutils'):
    vm.InstallPackages('python3-distutils')
  pip.Install(vm, pip_cmd='pip3', python_cmd='python3')


def Uninstall(vm):
  """Uninstalls the pip package on the VM."""
  pip.Uninstall(vm, pip_cmd='pip3')
