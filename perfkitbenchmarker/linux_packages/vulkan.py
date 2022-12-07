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
"""Module containing Vulkan Driver installation."""

from absl import flags
from perfkitbenchmarker import virtual_machine


_VERSION = flags.DEFINE_string(
    'vulkan_version', '1.2.131-bionic', 'The Vulkan version to install.'
)


def AptInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Install AMD GPU driver on the vm.

  Args:
    vm: The virtual machine to install AMD driver on.
  """
  vm.RemoteCommand(
      'wget -O - https://packages.lunarg.com/lunarg-signing-key-pub.asc | '
      'sudo apt-key add -'
  )
  vulkan_version, _ = _VERSION.value.split('-')
  vm.RemoteCommand(
      'sudo wget -O '
      f'/etc/apt/sources.list.d/lunarg-vulkan-{_VERSION.value}.list '
      f'https://packages.lunarg.com/vulkan/{vulkan_version}/lunarg-vulkan-{_VERSION.value}.list'
  )
  vm.RemoteCommand('sudo apt update')
  vm.InstallPackages('vulkan-sdk')


def YumInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  del vm  # unused
  raise NotImplementedError()
