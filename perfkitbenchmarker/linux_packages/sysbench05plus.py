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
from perfkitbenchmarker.linux_packages import INSTALL_DIR

SYSBENCH05PLUS_PATH = '%s/bin/sysbench' % INSTALL_DIR
PREPARE_SCRIPT_PATH = ('%s/share/doc/sysbench/tests/db/parallel_prepare.lua'
                       % INSTALL_DIR)
OLTP_SCRIPT_PATH = '%s/share/doc/sysbench/tests/db/oltp.lua' % INSTALL_DIR


def _Install(vm):
  """Installs the SysBench 0.5 on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages('bzr')
  vm.RemoteCommand('cd ~ && bzr branch lp:sysbench')
  vm.RemoteCommand(('cd ~/sysbench && ./autogen.sh &&'
                    ' ./configure --prefix=%s --mandir=%s/share/man &&'
                    ' make') % (INSTALL_DIR, INSTALL_DIR))
  vm.RemoteCommand('cd ~/sysbench && sudo make install')
  vm.RemoteCommand('sudo mkdir %s/share/doc/sysbench/tests/db -p' %
                   INSTALL_DIR)
  vm.RemoteCommand('sudo cp ~/sysbench/sysbench/tests/db/*'
                   ' %s/share/doc/sysbench/tests/db/' % INSTALL_DIR)


def YumInstall(vm):
  """ Installs SysBench 0.5 for Rhel/CentOS. We have to build from source!"""
  vm.InstallPackages('mysql mysql-server mysql-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the sysbench 0.5 on the VM."""
  vm.RemoteCommand('sudo ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so '
                   '/usr/lib/x86_64-linux-gnu/libmysqlclient_r.so')
  vm.InstallPackages('mysql-client mysql-server libmysqlclient-dev')
  _Install(vm)


def Uninstall(vm):
  # Cleanup the source code enlisthment from bzr, we don't need it anymore.
  vm.RemoteCommand('cd ~ && rm -fr ./sysbench')
