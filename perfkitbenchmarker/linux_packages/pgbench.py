# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing pgbench installation and cleanup functions.

  On Ubuntu 16.04 this will install pgbench 9.5.
"""


APT_PACKAGES = (
    'postgresql-client-common',
    'postgresql-client',
    'postgresql-contrib',
)


def AptInstall(vm):
  """Installs pgbench on the Debian VM."""
  for package in APT_PACKAGES:
    vm.InstallPackages(package)


def YumInstall(vm):
  """Raises exception when trying to install on yum-based VMs"""
  raise NotImplementedError(
      'PKB currently only supports the installation of pgbench on '
      'Debian-based VMs')


def AptUninstall(vm):
  """Removes pgbench from the Debian VM."""
  remove_str = 'sudo apt-get --purge autoremove -y '
  for package in APT_PACKAGES:
    vm.RemoteCommand(remove_str + package)
