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


"""Module containing sysbench installation and cleanup functions."""

SUSE12_BENCHMARK_REPO = 'http://download.opensuse.org/repositories/' \
                        'benchmark/SLE_12/benchmark.repo'


def _Install(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages('sysbench')


def YumInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallEpelRepo()
  _Install(vm)


def AptInstall(vm):
  """Installs the sysbench package on the VM."""
  _Install(vm)


def ZypperInstall(vm):
  """Installs the sysbench package on the VM."""
  if vm.GetSUSEVersion() >= 12:
    vm.AddRepository(SUSE12_BENCHMARK_REPO)
  _Install(vm)
