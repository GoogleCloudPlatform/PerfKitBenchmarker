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
"""Module containing redis cli installation and cleanup functions."""


def _Install(vm) -> None:
  """Installs the redis package on the VM."""
  vm.RemoteCommand(
      'curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o'
      ' /usr/share/keyrings/redis-archive-keyring.gpg'
  )
  vm.RemoteCommand(
      'echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg]'
      ' https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee'
      ' /etc/apt/sources.list.d/redis.list'
  )
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y redis')


def AptInstall(vm) -> None:
  """Installs the redis package on the VM."""
  _Install(vm)


def YumInstall(vm) -> None:
  """Installs the redis package on the VM."""
  del vm  # unused
  raise NotImplementedError()

