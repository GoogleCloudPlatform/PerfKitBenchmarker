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


"""Module containing mysql installation and cleanup functions."""

import posixpath
from perfkitbenchmarker.linux_packages import INSTALL_DIR

MYSQL_RPM = 'mysql56-community-release-el6-5.noarch.rpm'
MYSQL_PSWD = 'perfkitbenchmarker'
MYSQL_URL = 'https://dev.mysql.com/get/' + MYSQL_RPM
PACKAGE_NAME = 'mysql'
PREPROVISIONED_DATA = {
    MYSQL_RPM:
        '81b2256f778bb3972054257edda2c2a82fcec455cae3d45ba9c8778a46aa8eb3'
}
PACKAGE_DATA_URL = {
    MYSQL_RPM: MYSQL_URL
}


def YumInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand('sudo setenforce 0')
  vm.InstallPreprovisionedPackageData(
      PACKAGE_NAME, PREPROVISIONED_DATA.keys(), INSTALL_DIR)
  vm.RemoteCommand(
      'sudo rpm -ivh --force %s' % posixpath.join(INSTALL_DIR, MYSQL_RPM))
  vm.InstallPackages('mysql-server')
  vm.RemoteCommand('sudo service mysqld start')
  vm.RemoteCommand('/usr/bin/mysqladmin -u root password "%s"' % MYSQL_PSWD)


def AptInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand('echo "mysql-server-5.6 mysql-server/root_password password '
                   '%s" | sudo debconf-set-selections' % MYSQL_PSWD)
  vm.RemoteCommand('echo "mysql-server-5.6 mysql-server/root_password_again '
                   'password %s" | sudo debconf-set-selections' % MYSQL_PSWD)
  vm.InstallPackages('mysql-server')


def YumGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  del vm
  return '/etc/my.cnf'


def AptGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  del vm
  return '/etc/mysql/my.cnf'


def YumGetServiceName(vm):
  """Returns the name of the mysql service."""
  del vm
  return 'mysqld'


def AptGetServiceName(vm):
  """Returns the name of the mysql service."""
  del vm
  return 'mysql'
