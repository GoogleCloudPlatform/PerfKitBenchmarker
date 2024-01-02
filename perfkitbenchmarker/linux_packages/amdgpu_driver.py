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
"""Module containing AMD Driver installation."""

import posixpath
from perfkitbenchmarker import virtual_machine

# Instructure about the AMD driver address
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/install-amd-driver.html#preinstalled-amd-driver
_AMD_DRIVER = 'amdgpu-pro-20.20-1184451-ubuntu-18.04.tar.xz'


def AptInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Install AMD GPU driver on the vm.

  Args:
    vm: The virtual machine to install AMD driver on.
  """
  vm.RemoteCommand('sudo dpkg --add-architecture i386')
  tmp_dir = vm.scratch_disks[0].mount_point
  vm.DownloadPreprovisionedData(tmp_dir, 'amdgpu', _AMD_DRIVER)
  vm.RemoteCommand(
      f'tar -xf {posixpath.join(tmp_dir, _AMD_DRIVER)} -C {tmp_dir}'
  )
  vm.InstallPackages('linux-modules-extra-$(uname -r)')
  install_file = posixpath.join(
      tmp_dir, 'amdgpu-pro-20.20-1184451-ubuntu-18.04', 'amdgpu-pro-install'
  )
  vm.RemoteCommand(f'{install_file} -y --opencl=pal,legacy')
  vm.RemoteCommand(
      'echo -e "Section \\"Device\\"\n  Identifier  \\"AMD\\"\n'
      '  Driver  \\"amdgpu\\"\nEndSection" | sudo tee /etc/X11/xorg.conf'
  )
  vm.RemoteCommand(
      'echo "allowed_users=anybody" | sudo tee /etc/X11/Xwrapper.config'
  )
  vm.InstallPackages('x11vnc xorg xserver-xorg-video-amdgpu')
  vm.RemoteCommand(
      'wget https://github.com/GPUOpen-Drivers/AMDVLK/releases/download/v-2021.Q2.5/amdvlk_2021.Q2.5_amd64.deb'
  )
  vm.RemoteCommand('sudo dpkg -i amdvlk_2021.Q2.5_amd64.deb')
  vm.RemoteCommand('sudo usermod -aG tty ubuntu')
  vm.RemoteCommand('sudo usermod -aG video ubuntu')
  vm.RemoteCommand('sudo modprobe amdgpu')
  vm.RemoteCommand('sudo modprobe drm')
  vm.RemoteCommand('sudo chown ubuntu /dev/tty2')
  vm.RemoteCommand(
      'sudo nohup Xorg :0 -verbose 2 vt2 '
      '1> /tmp/stdout.log 2> /tmp/stderr.log &'
  )


def YumInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  del vm  # unused
  raise NotImplementedError()
