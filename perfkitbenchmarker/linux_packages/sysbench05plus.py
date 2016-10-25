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


"""Module containing sysbench 0.5 (and later) installation and cleanup logic.

Sysbench 0.5 contains breaking changes from previous versions. All existing
oltp benchmarks depending on older version of sysbench will break if we
install 0.5 or later for them. Therefore, it's necessary that we have a
separate installer here for 0.5 and later.
"""
from perfkitbenchmarker import os_types
from perfkitbenchmarker.linux_packages import INSTALL_DIR


def PathPrefix(vm):
  """Determines the prefix for a sysbench command based on the operating system.

  Args:
    vm: VM  on which the sysbench command will be executed.

  Returns:
    A string representing the sysbench command prefix.
  """
  if vm.OS_TYPE == os_types.RHEL:
    return INSTALL_DIR
  else:
    return '/usr/'


def YumInstall(vm):
  """ Installs SysBench 0.5 for Rhel/CentOS. We have to build from source!"""
  vm.Install('build_tools')
  vm.InstallPackages('bzr')
  vm.InstallPackages('mysql mysql-server mysql-devel')
  vm.RemoteCommand('cd ~ && bzr branch lp:sysbench')
  vm.RemoteCommand(('cd ~/sysbench && ./autogen.sh &&'
                    ' ./configure --prefix=%s --mandir=%s/share/man &&'
                    ' make') % (INSTALL_DIR, INSTALL_DIR))
  vm.RemoteCommand('cd ~/sysbench && sudo make install')
  vm.RemoteCommand('sudo mkdir %s/share/doc/sysbench/tests/db -p' %
                   INSTALL_DIR)
  vm.RemoteCommand('sudo cp ~/sysbench/sysbench/tests/db/*'
                   ' %s/share/doc/sysbench/tests/db/' % INSTALL_DIR)
  vm.RemoteCommand('echo "export PATH=$PATH:%s/bin" >> ~/.bashrc && '
                   'source ~/.bashrc' % INSTALL_DIR)

  # Cleanup the source code enlisthment from bzr, we don't need it anymore.
  vm.RemoteCommand('cd ~ && rm -fr ./sysbench')


def AptInstall(vm):
  """ Installs the sysbench 0.5 or later versions via APT Install """
  vm.Install('wget')
  vm.RemoteCommand('wget https://repo.percona.com/apt/'
                   'percona-release_0.1-4.$(lsb_release -sc)_all.deb')
  vm.RemoteCommand(
      'sudo dpkg -i percona-release_0.1-4.$(lsb_release -sc)_all.deb')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('libc6')
  vm.InstallPackages('mysql-client')
  vm.InstallPackages('sysbench')


def AptUninstall(vm):
  vm.RemoteCommand('sudo dpkg --purge percona-release')
  vm.RemoteCommand('sudo apt-get update')
