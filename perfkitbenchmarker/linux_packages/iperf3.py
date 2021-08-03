# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing iperf3 installation functions."""


def _Install(vm):
  """Install Cloud Harmory iperf benchmark on VM."""
  # Follows instructions from https://software.es.net/iperf/obtaining.html.
  vm.Install('wget')
  vm.Install('build_tools')
  vm.InstallPackages('lib32z1')
  vm.RemoteCommand(
      'wget https://downloads.es.net/pub/iperf/iperf-3.9.tar.gz --no-check-certificate'
  )
  vm.RemoteCommand('tar -xf iperf-3.9.tar.gz')
  vm.RemoteCommand('cd iperf-3.9 && ./configure')
  vm.RemoteCommand('cd iperf-3.9 && make')
  vm.RemoteCommand('cd iperf-3.9 && sudo make install')
