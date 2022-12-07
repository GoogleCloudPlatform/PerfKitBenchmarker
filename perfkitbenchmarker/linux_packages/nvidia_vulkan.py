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
"""Module containing NVIDIA Vulkan Driver installation."""

import os
import posixpath
from perfkitbenchmarker import virtual_machine

# NVIDIA Vulkan Driver https://developer.nvidia.com/vulkan-driver

_NV_VK_DRIVER = 'https://us.download.nvidia.com/XFree86/Linux-x86_64/525.60.11/NVIDIA-Linux-x86_64-525.60.11.run'
_CONF = 'xorg_nvidia.conf'


def AptInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Install AMD GPU driver on the vm.

  Args:
    vm: The virtual machine to install AMD driver on.
  """
  tmp_dir = vm.scratch_disks[0].mount_point
  vm.RemoteCommand(f'cd {tmp_dir} && wget -c {_NV_VK_DRIVER}')
  vm.InstallPackages('build-essential')
  vm.RemoteCommand(
      f'sudo sh {posixpath.join(tmp_dir, os.path.basename(_NV_VK_DRIVER))} -s'
  )
  vm.InstallPackages('xterm')
  vm.InstallPackages('xorg')
  vm.RemoteCommand('sudo nvidia-xconfig')

  vm.RemoteCommand(
      'sudo nohup xinit -- :1 1> /tmp/stdout.log 2> /tmp/stderr.log &'
  )


def YumInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  del vm  # unused
  raise NotImplementedError()
