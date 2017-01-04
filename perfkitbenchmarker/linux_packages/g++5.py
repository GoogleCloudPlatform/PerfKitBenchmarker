# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing g++5 installation and cleanup functions."""


def _Install(vm):
  """Installs the g++-5 package on the VM."""
  pass


def YumInstall(vm):
  """Installs the g++-5 package on the VM."""
  # TODO: Figure out how to install gcc/g++5 with yum
  raise NotImplementedError


def AptInstall(vm):
  """Installs the g++-5 package on the VM."""
  vm.RemoteCommand(
      'sudo add-apt-repository ppa:ubuntu-toolchain-r/test -y; '
      'sudo apt-get update')
  vm.InstallPackages('g++-5')
