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


"""Module containing Silo installation and cleanup functions."""

from perfkitbenchmarker.linux_packages import INSTALL_DIR

GIT_REPO = 'https://github.com/stephentu/silo.git'
GIT_TAG = '62d2d498984bf69d3b46a74e310e1fd12fd1f692'
SILO_DIR = '%s/silo' % INSTALL_DIR
APT_PACKAGES = ('libjemalloc-dev libnuma-dev libdb++-dev '
                'libmysqld-dev libaio-dev libssl-dev')
YUM_PACKAGES = ('jemalloc-devel numactl-devel libdb-cxx-devel mysql-devel '
                'libaio-devel openssl-devel')


def _Install(vm):
  """Installs the Silo package on the VM."""
  nthreads = vm.num_cpus * 2
  vm.Install('build_tools')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, SILO_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(SILO_DIR,
                                                       GIT_TAG))
  # This is due to a failing clone command when executing behind a proxy.
  # Replacing the protocol to https instead of git fixes the issue.
  vm.RemoteCommand('git config --global url."https://".insteadOf git://')
  # Disable -Wmaybe-uninitialized errors when GCC has the option to workaround
  # a spurious error in masstree.
  cxx = '"g++ -std=gnu++0x \
          $(echo | gcc -Wmaybe-uninitialized -E - >/dev/null 2>&1 && \
            echo -Wno-error=maybe-uninitialized)"'
  vm.RemoteCommand('cd {0} && CXX={2} MODE=perf DEBUG=0 CHECK_INVARIANTS=0 make\
          -j{1} dbtest'.format(SILO_DIR, nthreads, cxx))


def YumInstall(vm):
  """Installs the Silo package on the VM."""
  vm.InstallEpelRepo()
  vm.InstallPackages(YUM_PACKAGES)
  _Install(vm)


def AptInstall(vm):
  """Installs the Silo package on the VM."""
  vm.InstallPackages(APT_PACKAGES)
  _Install(vm)
