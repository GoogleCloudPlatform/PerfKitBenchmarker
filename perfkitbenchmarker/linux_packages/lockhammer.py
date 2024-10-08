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
"""Installation and compilation of the lockhammer benchmark.

TODO(user): Implement me
"""

from perfkitbenchmarker import virtual_machine


def Install(vm: virtual_machine.BaseVirtualMachine):
  """Installs and builds the lockhammer benchmark on the VM."""
  # Install build dependencies.
  # https://github.com/ARM-software/synchronization-benchmarks/tree/master/benchmarks/lockhammer#guide-for-ubuntu-1904
  vm.Install('build_tools')
  vm.Install('pip')
  vm.InstallPackages('hwloc')
  vm.RemoteCommand('sudo pip3 install sh')
  # TODO(user): Clone and build the benchmark.
