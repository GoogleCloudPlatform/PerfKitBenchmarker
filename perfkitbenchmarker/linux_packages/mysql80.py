# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

MYSQL_PSWD = 'perfkitbenchmarker'
PACKAGE_NAME = 'mysql'


def YumInstall(vm):
  """Installs the mysql package on the VM."""
  raise NotImplementedError


def AptInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand('echo "mysql-server-8.0 mysql-server/root_password password '
                   f'{MYSQL_PSWD}" | sudo debconf-set-selections')
  vm.RemoteCommand('echo "mysql-server-8.0 mysql-server/root_password_again '
                   f'password {MYSQL_PSWD}" | sudo debconf-set-selections')
  vm.InstallPackages('mysql-server')


def YumGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  raise NotImplementedError


def AptGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  del vm
  return '/etc/mysql/mysql.conf.d/mysqld.cnf'


def YumGetServiceName(vm):
  """Returns the name of the mysql service."""
  raise NotImplementedError


def AptGetServiceName(vm):
  """Returns the name of the mysql service."""
  del vm
  return 'mysql'
