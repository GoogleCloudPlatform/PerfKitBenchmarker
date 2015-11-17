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

MYSQL_RPM = 'http://dev.mysql.com/get/mysql-community-release-el6-5.noarch.rpm'
MYSQL_PSWD = 'perfkitbenchmarker'


def YumInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand('sudo setenforce 0')
  vm.RemoteCommand('sudo rpm -ivh --force %s' % MYSQL_RPM)
  vm.InstallPackages('mysql-server')
  vm.RemoteCommand('sudo service mysqld start')
  vm.RemoteCommand('/usr/bin/mysqladmin -u root password "%s"' % MYSQL_PSWD)


def AptInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand('echo "mysql-server-5.5 mysql-server/root_password password '
                   '%s" | sudo debconf-set-selections' % MYSQL_PSWD)
  vm.RemoteCommand('echo "mysql-server-5.5 mysql-server/root_password_again '
                   'password %s" | sudo debconf-set-selections' % MYSQL_PSWD)
  vm.InstallPackages('mysql-server')


def YumGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  return '/etc/my.cnf'


def AptGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  return '/etc/mysql/my.cnf'


def YumGetServiceName(vm):
  """Returns the name of the mysql service."""
  return 'mysqld'


def AptGetServiceName(vm):
  """Returns the name of the mysql service."""
  return 'mysql'
