# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing texinfo installation and cleanup functions."""

from perfkitbenchmarker import os_types


def YumInstall(vm):
  """Installs the texinfo package on the VM."""
  if vm.OS_TYPE == os_types.ROCKY_LINUX8:
    vm.RemoteCommand('sudo dnf config-manager --set-enabled powertools')
  elif vm.OS_TYPE == os_types.ROCKY_LINUX9:
    vm.RemoteCommand('sudo dnf config-manager --set-enabled crb')
  elif vm.OS_TYPE == os_types.RHEL8:
    vm.RemoteCommand(
        'sudo dnf config-manager --set-enabled'
        ' rhui-codeready-builder-for-rhel-8-x86_64-rhui-rpms'
    )
  elif vm.OS_TYPE == os_types.RHEL9:
    if vm.is_aarch64:
      vm.RemoteCommand(
          'sudo dnf config-manager --set-enabled'
          ' rhui-codeready-builder-for-rhel-9-aarch64-rhui-rpms'
      )
    else:
      vm.RemoteCommand(
          'sudo dnf config-manager --set-enabled'
          ' rhui-codeready-builder-for-rhel-9-x86_64-rhui-rpms'
      )

  vm.InstallPackages('texinfo')


def AptInstall(vm):
  """Installs the texinfo package on the VM."""
  vm.InstallPackages('texinfo')
