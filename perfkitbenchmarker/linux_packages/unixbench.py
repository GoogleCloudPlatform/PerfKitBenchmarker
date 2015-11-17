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


"""Module containing UnixBench installation and cleanup functions."""

from perfkitbenchmarker import vm_util

UNIXBENCH_TAR = 'v5.1.3.tar.gz'
UNIXBENCH_URL = ('https://github.com/kdlucas/byte-unixbench/archive/'
                 + UNIXBENCH_TAR)
UNIXBENCH_DIR = '%s/byte-unixbench-5.1.3/UnixBench' % vm_util.VM_TMP_DIR


def _Install(vm):
  """Installs the UnixBench package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget {0} -P {1}'.format(UNIXBENCH_URL, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd {0} && tar xvzf {1}'.format(vm_util.VM_TMP_DIR,
                                                   UNIXBENCH_TAR))


def YumInstall(vm):
  """Installs the UnixBench package on the VM."""
  vm.InstallPackages('libX11-devel mesa-libGL-devel '
                     'perl-Time-HiRes libXext-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the UnixBench package on the VM."""
  vm.InstallPackages('libx11-dev libgl1-mesa-dev libxext-dev')
  _Install(vm)
