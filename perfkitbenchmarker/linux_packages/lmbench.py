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

from perfkitbenchmarker.linux_packages import INSTALL_DIR

# LMBENCH_DIR = '%s/LMBENCH' % INSTALL_DIR
LMBENCH_TAG = 'lmbench3'
LMBENCH_TAR = '%s.tar.gz' % LMBENCH_TAG
LMBENCH_DIR = '%s/%s' % (INSTALL_DIR, LMBENCH_TAG)
LMBENCH_URL = 'http://www.bitmover.com/lmbench/' + LMBENCH_TAR


def _Install(vm):
  """Installs the Lmbench package on the VM."""

  vm.Install('build_tools')
  vm.RemoteCommand('wget %s -P %s' % (LMBENCH_URL, INSTALL_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (INSTALL_DIR, LMBENCH_TAR))
  # Fix the bug in the source code
  # See more in:
  # https://github.com/zhanglongqi/linux-tips/blob/master/tools/benchmark.md
  vm.RemoteCommand(
      'cd {0} && mkdir ./SCCS && touch ./SCCS/s.ChangeSet'.format(LMBENCH_DIR))


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)
