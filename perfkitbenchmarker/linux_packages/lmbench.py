# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing LMbench installation and cleanup functions."""

import posixpath
from perfkitbenchmarker.linux_packages import INSTALL_DIR

LMBENCH_DIR = posixpath.join(INSTALL_DIR, 'lmbench')
GIT = 'https://github.com/intel/lmbench.git'
COMMIT = '4e4efa113b244b70a1faafd13744578b4edeaeb3'


def _Install(vm):
  """Installs the Lmbench package on the VM."""

  vm.Install('build_tools')
  vm.RemoteCommand('cd %s && git clone %s && cd %s && git checkout %s' % (
      INSTALL_DIR, GIT, 'lmbench', COMMIT))


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)
