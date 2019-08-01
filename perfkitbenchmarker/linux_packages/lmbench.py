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

LMBENCH_DIR = posixpath.join(INSTALL_DIR, 'lmbench-master')
PACKAGE_NAME = 'lmbench'
LMBENCH_ZIP = 'master.zip'
PREPROVISIONED_DATA = {
    LMBENCH_ZIP:
        '85ea189c2a7adf1e2c9c136eb9775ff8fc3c6cd873d1354b10deb33148a19913'
}
PACKAGE_DATA_URL = {
    LMBENCH_ZIP: 'https://github.com/intel/lmbench/archive/master.zip'
}


def _Install(vm):
  """Installs the Lmbench package on the VM."""

  vm.Install('build_tools')
  vm.Install('unzip')
  vm.InstallPreprovisionedPackageData(PACKAGE_NAME, [LMBENCH_ZIP], INSTALL_DIR)
  vm.RemoteCommand('cd %s && unzip %s' % (INSTALL_DIR, LMBENCH_ZIP))
  # Fix the bug in the source code
  # See more in:
  # https://github.com/zhanglongqi/linux-tips/blob/master/tools/benchmark.md
  vm.RemoteCommand(
      'cd {0} && mkdir ./SCCS && touch ./SCCS/s.ChangeSet'.format(LMBENCH_DIR))


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)
