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
from perfkitbenchmarker import vm_util


def YumInstall(vm):
  """ Installs SysBench 0.5 for Rhel/CentOS. We have to build from source!"""
  vm.Install('build_tools')
  vm.InstallPackages('bzr')
  vm.InstallPackages('mysql mysql-server mysql-devel')
  vm.RemoteCommand('cd ~ && bzr branch lp:sysbench')
  vm.RemoteCommand(('cd ~/sysbench && ./autogen.sh &&'
                    ' ./configure --prefix=%s --mandir=%s/share/man &&'
                    ' make') % (vm_util.VM_TMP_DIR, vm_util.VM_TMP_DIR))
  vm.RemoteCommand('cd ~/sysbench && sudo make install')
  vm.RemoteCommand('sudo mkdir %s/share/doc/sysbench/tests/db -p' %
                   vm_util.VM_TMP_DIR)
  vm.RemoteCommand('sudo cp ~/sysbench/sysbench/tests/db/*'
                   ' %s/share/doc/sysbench/tests/db/' % vm_util.VM_TMP_DIR)
  vm.RemoteCommand('echo "export PATH=$PATH:%s/bin" >> ~/.bashrc && '
                   'source ~/.bashrc' % vm_util.VM_TMP_DIR)

  # Cleanup the source code enlisthment from bzr, we don't need it anymore.
  vm.RemoteCommand('cd ~ && rm -fr ./sysbench')


def AptInstall(vm):
  """ Installs the sysbench 0.5 or later versions via APT Install """

  # Setup the proper sources list so apt get will get the latest version
  # of sysbench. By default, it only gets version earlier than 0.5.
  vm.RemoteCommand('sudo bash -c \'echo "deb http://repo.percona.com/apt'
                   ' trusty main">>/etc/apt/sources.list.d/percona.list\'')
  vm.RemoteCommand('sudo bash -c \'echo "deb-src http://repo.percona.com/apt'
                   ' trusty main">>/etc/apt/sources.list.d/percona.list\'')
  vm.RemoteCommand('sudo bash -c \'echo "deb http://security.ubuntu.com/ubuntu'
                   ' trusty-security main">>/etc/apt/sources.list\'')
  vm.RemoteCommand('sudo apt-key adv --keyserver keys.gnupg.net --recv-keys'
                   ' 1C4CBDCDCD2EFD2A')
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('libc6')
  vm.InstallPackages('mysql-client')
  vm.InstallPackages('sysbench')
