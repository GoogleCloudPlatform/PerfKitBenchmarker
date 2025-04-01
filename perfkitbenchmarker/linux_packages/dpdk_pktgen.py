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

"""Module containing dpdk-pktgen installation."""


DPDK_PKTGEN_GIT_REPO = 'https://github.com/pktgen/Pktgen-DPDK'
DPDK_PKTGEN_GIT_REPO_DIR = 'Pktgen-DPDK'
DPDK_PKTGEN_GIT_REPO_TAG = '0e3a9c50daedccc7a83597f187d96288264edac0'


def _Install(vm):
  vm.InstallPackages('libpcap-dev libbsd-dev')
  vm.Install('dpdk')
  vm.RobustRemoteCommand(f'git clone {DPDK_PKTGEN_GIT_REPO}')
  vm.RobustRemoteCommand(
      f'cd {DPDK_PKTGEN_GIT_REPO_DIR} && git checkout'
      f' {DPDK_PKTGEN_GIT_REPO_TAG} && make'
  )


def AptInstall(vm):
  """Install dependencies on APT-based systems."""
  _Install(vm)


def YumInstall(vm):
  """Install dependencies on YUM-based systems."""
  _Install(vm)
