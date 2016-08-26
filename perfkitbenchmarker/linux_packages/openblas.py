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

"""Module containing OpenBLAS installation and cleanup functions."""

from perfkitbenchmarker import vm_util

OPENBLAS_DIR = '%s/OpenBLAS' % vm_util.VM_TMP_DIR
GIT_REPO = 'https://github.com/xianyi/OpenBLAS'
GIT_TAG = 'v0.2.15'


def _Install(vm):
  """Installs the OpenBLAS package on the VM."""
  vm.Install('build_tools')
  vm.Install('fortran')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, OPENBLAS_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(OPENBLAS_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && make USE_THREAD=0'.format(OPENBLAS_DIR))


def YumInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the OpenBLAS package on the VM."""
  _Install(vm)
