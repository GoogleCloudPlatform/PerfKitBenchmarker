# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing dstat installation and cleanup functions."""

SUSE11_MONITORING_REPO = 'http://download.opensuse.org/repositories/' \
                         'server:/monitoring/SLE_11_SP3/server:monitoring.repo'
SUSE12_MONITORING_REPO = 'http://download.opensuse.org/repositories/' \
                         'server:/monitoring/SLE_12/server:monitoring.repo'


def _Install(vm):
  """Installs the dstat package on the VM."""
  vm.InstallPackages('dstat')


def YumInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the dstat package on the VM."""
  _Install(vm)


def ZypperInstall(vm):
  """Installs the dstat package on the VM."""
  if vm.GetSUSEVersion() >= 12:
    vm.AddRepository(SUSE12_MONITORING_REPO)
    _Install(vm)
  elif vm.GetSUSEVersion() == 11:
    vm.AddRepository(SUSE11_MONITORING_REPO)
    _Install(vm)
