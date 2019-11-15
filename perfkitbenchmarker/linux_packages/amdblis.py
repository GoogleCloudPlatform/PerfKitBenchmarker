# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing AMD BLIS installation and cleanup functions."""

from perfkitbenchmarker.linux_packages import INSTALL_DIR

AMDBLIS_DIR = '%s/amdblis' % INSTALL_DIR
GIT_REPO = 'https://github.com/amd/blis'
GIT_TAG = '1.3'


def _Install(vm):
  """Installs the AMD BLIS package on the VM."""
  vm.Install('build_tools')
  vm.Install('fortran')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, AMDBLIS_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(AMDBLIS_DIR, GIT_TAG))
  vm.RemoteCommand(
      'cd {0} && ./configure --enable-cblas zen'.format(AMDBLIS_DIR))
  vm.RemoteCommand('cd {0} && make -j'.format(AMDBLIS_DIR))


def Install(vm):
  """Installs the AMD BLIS package on the VM."""
  _Install(vm)
