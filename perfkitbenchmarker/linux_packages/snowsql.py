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
"""Module containing snowsql installation and cleanup functions."""

SNOWSQL_INSTALL_LOCATION = '~/bin'
SNOWSQL_VERSION = '1.2.5'
SNOWSQL_DOWNLOAD_URL = 'https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-%s-linux_x86_64.bash' % SNOWSQL_VERSION


def AptInstall(vm):
  """Installs snowsql on the Debian VM."""
  vm.Install('curl')
  vm.Install('unzip')
  vm.RemoteCommand('curl -O %s' % SNOWSQL_DOWNLOAD_URL)
  vm.RemoteCommand(
      'SNOWSQL_DEST=%s SNOWSQL_LOGIN_SHELL=~/.profile '
      'bash snowsql-%s-linux_x86_64.bash'
      % (SNOWSQL_INSTALL_LOCATION, SNOWSQL_VERSION))
  vm.RemoteCommand('chmod +x %s/snowsql' % SNOWSQL_INSTALL_LOCATION)


def YumInstall(vm):
  """Raises exception when trying to install on yum-based VMs."""
  del vm
  raise NotImplementedError(
      'PKB currently only supports the installation of snowsql on '
      'Debian-based VMs')


def AptUninstall(vm):
  """Removes snowsql from the Debian VM."""
  vm.RemoteCommand('rm -rf ~/bin/snowsql')
  vm.RemoteCommand('rm -rf ~/.snowsql')
  vm.RemoteCommand('rm -rf snowsql-%s-linux_x86_64.bash' % SNOWSQL_VERSION)
