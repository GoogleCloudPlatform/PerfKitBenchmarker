# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing pip installation and cleanup functions.

Uninstalling the pip package will also remove all python packages
added after installation.
"""

from perfkitbenchmarker import vm_util


def _Install(vm):
  """Install pip on the VM."""
  vm.InstallPackages('python-pip')
  vm.RemoteCommand('sudo pip install -U pip')  # Make pip upgrade pip
  vm.RemoteCommand('mkdir -p {0} && pip freeze > {0}/requirements.txt'.format(
      vm_util.VM_TMP_DIR))


def YumInstall(vm):
  """Installs the pip package on the VM."""
  vm.InstallEpelRepo()
  _Install(vm)


def AptInstall(vm):
  """Installs the pip package on the VM."""
  _Install(vm)


def _Uninstall(vm):
  """Uninstalls the pip package on the VM."""
  vm.RemoteCommand('pip freeze | grep --fixed-strings --line-regexp '
                   '--invert-match --file {0}/requirements.txt | '
                   'xargs --no-run-if-empty sudo pip uninstall -y'.format(
                       vm_util.VM_TMP_DIR))


def YumUninstall(vm):
  """Uninstalls the pip package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the pip package on the VM."""
  _Uninstall(vm)
