# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing NVIDIA Container Toolkit installation.

https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
"""
_VERSION = '1.17.8-1'


def AptInstall(vm):
  """Install Nvidia container toolkit on vm."""
  vm.RemoteCommand(
      'curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | '
      'sudo gpg --dearmor -o '
      '/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg '
      '&& curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/'
      'nvidia-container-toolkit.list | '
      "sed 's#deb https://#deb [signed-by=/usr/share/keyrings/"
      "nvidia-container-toolkit-keyring.gpg] https://#g' | "
      'sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand(
      'sudo apt-get install -y '
      f'nvidia-container-toolkit={_VERSION} '
      f'nvidia-container-toolkit-base={_VERSION} '
      f'libnvidia-container-tools={_VERSION} '
      f'libnvidia-container1={_VERSION}')
  vm.RemoteCommand('sudo systemctl restart docker', ignore_failure=True)
